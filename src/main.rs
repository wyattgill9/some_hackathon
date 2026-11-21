//! Shopping Sessions: reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
//! rules.md is the spec. Phases: links (union-find) -> parse events and clicks into hash partitions
//! (by event_id, by person) -> dedup per event partition -> sessions and attribution per person partition.
mod key;
mod links;
mod parquet;
mod parse;
mod person;
mod pool;
mod reader;
mod ts;
mod u64map;

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use key::{Key, mix};
use parquet::{Column, Kind};
use person::Cell;
use pool::Pool;
use reader::{Range, Reader};
use ts::TsCache;
use u64map::U64Map;

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const GAP: i64 = 1_800_000;

/// A parsed event copy. Times are relative to `cws` (window_start - 7 d): `ts` in ms, `ingest` in seconds
/// biased by 2^31. `pay` = price | category << 24 for a purchase, else 0. `flags` bit 0: valid and not late.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Ev {
    eid: Key,
    person: Key,
    ingest: u32,
    ts: u32,
    pay: u32,
    flags: u32,
}

/// A surviving event.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Ev2 {
    pub person: Key,
    pub ts: u32,
    pub pay: u32,
}

/// A valid click.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Ck {
    pub person: Key,
    pub cid: Key,
    pub ts: u32,
    pub camp: u32,
}

pub struct Ctx {
    pub ws: i64,
    pub we: i64,
    pub cws: i64,
    pub day0: i64,
    pub ndays: usize,
    pub bits: u32,
    pub ncamp: usize,
    pub cats: Vec<Vec<u8>>,
    pub unknown: u32,
}

fn main() {
    let t0 = Instant::now();
    let lap = |what: &str| eprintln!("{:>8.3}s {what}", t0.elapsed().as_secs_f64());
    let env = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let threads = env("NCPU").map(|n| n as usize).unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get())).max(1);

    let manifest = fs::read(data.join("manifest.json")).unwrap();
    let (ws, we) = (manifest_ts(&manifest, "window_start"), manifest_ts(&manifest, "window_end"));
    let cws = ws - WEEK;
    if we <= ws || we - cws >= 1 << 32 {
        eprintln!("window of {} ms not supported (times are u32 ms offsets)", we - ws);
        std::process::exit(2);
    }
    let (day0, ndays) = (ws.div_euclid(DAY), ((we - 1).div_euclid(DAY) - ws.div_euclid(DAY) + 1) as usize);

    // Products: category index, with categories (plus `unknown`) sorted so index order is byte order.
    let products_buf = fs::read(data.join("products.csv")).unwrap();
    let products: Vec<[&[u8]; 2]> = lines(&products_buf).filter_map(parse::fields::<2>).collect();
    let mut cats: Vec<&[u8]> = products.iter().map(|[_, c]| *c).chain([&b"unknown"[..]]).collect();
    cats.sort_unstable();
    cats.dedup();
    if cats.len() > 256 {
        eprintln!("{} categories: more than the 8-bit category index holds", cats.len());
        std::process::exit(3);
    }
    let unknown = cats.binary_search(&&b"unknown"[..]).unwrap() as u32;
    let mut cat_of = U64Map::new(products.len());
    for [p, c] in &products {
        if !p.is_empty() {
            cat_of.insert(key::pack(p), cats.binary_search(c).unwrap() as u32);
        }
    }
    // Campaigns: index, id, channel. `organic` is index ncamp.
    let campaigns_buf = fs::read(data.join("campaigns.csv")).unwrap();
    let campaigns: Vec<[&[u8]; 2]> = lines(&campaigns_buf).filter_map(parse::fields::<2>).collect();
    let mut camp_of = U64Map::new(campaigns.len());
    for (i, [c, _]) in campaigns.iter().enumerate() {
        if !c.is_empty() {
            camp_of.insert(key::pack(c), i as u32);
        }
    }
    let ncamp = campaigns.len();

    let event_files = reader::open_dir(&data.join("events"));
    let click_files = reader::open_dir(&data.join("clicks"));
    let link_files = reader::open_dir(&data.join("links"));
    let event_bytes: u64 = event_files.iter().map(|f| f.1).sum();
    // Partitions of about 2 MiB of CSV each: every later phase works on one in cache.
    let bits = env("SS_PBITS").map_or(((event_bytes >> 21).next_power_of_two().trailing_zeros()).clamp(4, 11), |b| b as u32);
    let parts = 1usize << bits;
    let range = env("SS_RANGE").unwrap_or(256 << 10);
    let ctx = Ctx { ws, we, cws, day0, ndays, bits, ncamp, cats: cats.iter().map(|c| c.to_vec()).collect(), unknown };
    fs::create_dir_all(out.join("user_daily")).unwrap();
    fs::create_dir_all(out.join("campaign_daily")).unwrap();
    lap("setup");

    // Links: parsed on every thread, joined on one.
    let link_ranges = reader::ranges(&link_files, range);
    let next = AtomicUsize::new(0);
    let edges: Vec<Vec<(Key, Key)>> = parallel(threads, |_| {
        let (mut rd, mut out) = (Reader::default(), Vec::new());
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(r) = link_ranges.get(i) else { break out };
            rd.lines(&link_files[r.file].0, r, |l| {
                if let Some([a, b]) = parse::fields::<2>(l) {
                    if !a.is_empty() && !b.is_empty() {
                        let (ka, kb) = (key::pack(a), key::pack(b));
                        if ka != kb {
                            out.push((ka, kb));
                        }
                    }
                }
            });
        }
    });
    let links = links::Links::build(&edges);
    drop(edges);
    lap("links");

    // Parse: event ranges then click ranges from one queue; records go to hash partitions.
    let mut tasks: Vec<(bool, Range)> = reader::ranges(&event_files, range).into_iter().map(|r| (true, r)).collect();
    tasks.extend(reader::ranges(&click_files, range).into_iter().map(|r| (false, r)));
    let next = AtomicUsize::new(0);
    let parsed: Vec<(Pool<Ev>, Pool<Ck>)> = parallel(threads, |_| {
        let (mut rd, mut tc) = (Reader::default(), TsCache::default());
        let (mut evs, mut cks) = (Pool::<Ev>::new(parts), Pool::<Ck>::new(parts));
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some((is_event, r)) = tasks.get(i) else { break };
            if *is_event {
                rd.lines(&event_files[r.file].0, r, |l| {
                    if let Some(e) = parse_event(l, &ctx, &cat_of, &links, &mut tc) {
                        evs.push((mix(e.eid) >> (64 - bits)) as usize, e);
                    }
                });
            } else {
                rd.lines(&click_files[r.file].0, r, |l| {
                    if let Some(c) = parse_click(l, &ctx, &camp_of, &links, &mut tc) {
                        cks.push((mix(c.person) >> (64 - bits)) as usize, c);
                    }
                });
            }
        }
        evs.finish();
        cks.finish();
        (evs, cks)
    });
    let (evs, cks): (Vec<Pool<Ev>>, Vec<Pool<Ck>>) = parsed.into_iter().unzip();
    lap("parse");

    // Dedup per event partition: keep the copy with the smallest ingest_ts, emit it if valid, by person.
    let next = AtomicUsize::new(0);
    let ev2: Vec<Pool<Ev2>> = parallel(threads, |_| {
        let (mut recs, mut tab) = (Vec::new(), Vec::new());
        let mut out = Pool::<Ev2>::new(parts);
        loop {
            let p = next.fetch_add(1, Ordering::Relaxed);
            if p >= parts {
                break;
            }
            recs.clear();
            for t in &evs {
                t.blocks(p).for_each(|b| recs.extend_from_slice(b));
            }
            dedup(&recs, &mut tab, bits, &mut out);
        }
        out.finish();
        out
    });
    drop(evs);
    lap("dedup");

    // People per person partition, largest first; each worker writes its own user_daily file.
    let mut order: Vec<usize> = (0..parts).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(ev2.iter().map(|t| t.len(q)).sum::<usize>()));
    let next = AtomicUsize::new(0);
    let results: Vec<(Vec<Cell>, usize)> = parallel(threads, |t| {
        let mut w = person::Worker::new(&ctx);
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(&q) = order.get(i) else { break };
            w.run(ev2.iter().flat_map(|t| t.blocks(q)), cks.iter().flat_map(|t| t.blocks(q)));
        }
        w.finish(&out.join("user_daily").join(format!("part-{t}.parquet")))
    });
    lap("people");

    // campaign_daily: the threads' cells summed; a row wherever a campaign credited or assisted a session.
    let mut cells = vec![Cell::default(); (ncamp + 1) * ndays];
    for (c, _) in &results {
        for (a, b) in cells.iter_mut().zip(c) {
            a.sessions += b.sessions;
            a.purchasing += b.purchasing;
            a.assisted += b.assisted;
            a.revenue += b.revenue;
        }
    }
    let (mut cid, mut chan, mut days) = (Vec::new(), Vec::new(), Vec::new());
    let (mut c0, mut c1, mut c2, mut c3) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (i, cell) in cells.iter().enumerate() {
        if cell.sessions == 0 && cell.assisted == 0 {
            continue;
        }
        let (c, d) = (i / ndays, i % ndays);
        let [id, ch] = if c == ncamp { [&b"organic"[..]; 2] } else { campaigns[c] };
        parquet::put_str(&mut cid, id);
        parquet::put_str(&mut chan, ch);
        days.push((day0 + d as i64) as i32);
        c0.push(cell.sessions as i64);
        c1.push(cell.purchasing as i64);
        c2.push(cell.revenue);
        c3.push(cell.assisted as i64);
    }
    parquet::write(
        &out.join("campaign_daily").join("part-0.parquet"),
        days.len(),
        &[
            Column { name: "campaign_id", kind: Kind::Str, optional: false, body: &cid },
            Column { name: "channel", kind: Kind::Str, optional: false, body: &chan },
            Column { name: "day", kind: Kind::Date, optional: false, body: parquet::plain(&days) },
            Column { name: "sessions", kind: Kind::Int64, optional: false, body: parquet::plain(&c0) },
            Column { name: "purchasing_sessions", kind: Kind::Int64, optional: false, body: parquet::plain(&c1) },
            Column { name: "revenue_cents", kind: Kind::Int64, optional: false, body: parquet::plain(&c2) },
            Column { name: "assisted_sessions", kind: Kind::Int64, optional: false, body: parquet::plain(&c3) },
        ],
    )
    .unwrap();
    let rows: usize = results.iter().map(|r| r.1).sum();
    let (cs, cr) = (c0.iter().sum::<i64>(), c2.iter().sum::<i64>());
    lap(&format!("write: {rows} user rows, {} campaign rows; sessions {cs}, revenue {cr}", days.len()));
    // Nothing is freed: the kernel reclaims the gigabytes at exit faster than Rust drops them.
    std::process::exit(0);
}

/// `n` threads running `f(thread index)`; their results in order.
fn parallel<T: Send>(n: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    std::thread::scope(|s| {
        let f = &f;
        let handles: Vec<_> = (0..n).map(|t| s.spawn(move || f(t))).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

#[inline]
fn parse_event(l: &[u8], ctx: &Ctx, cat_of: &U64Map, links: &links::Links, tc: &mut TsCache) -> Option<Ev> {
    let [id, user, ts, ingest, kind, product, price] = parse::fields::<7>(l)?;
    if id.is_empty() {
        return None;
    }
    let t = tc.parse(ts).filter(|t| (ctx.ws..ctx.we).contains(t));
    // ponytail: rules.md guarantees ingest_ts is valid F1; a bad one sorts last and fails the late check.
    let ing = tc.parse(ingest).unwrap_or(i64::MAX);
    let mut valid = !user.is_empty()
        && !product.is_empty()
        && matches!(kind, b"view" | b"cart" | b"purchase")
        && t.is_some_and(|t| ing - t <= DAY);
    let mut pay = 0;
    if valid && kind == b"purchase" {
        match parse::price(price) {
            Some(v) => pay = v | cat_of.get(key::pack(product)).unwrap_or(ctx.unknown) << 24,
            None => valid = false,
        }
    }
    let person = if valid { links.lookup(key::pack(user)) } else { 0 };
    Some(Ev {
        eid: key::pack(id),
        person,
        ingest: (ing.div_euclid(1000) - ctx.cws.div_euclid(1000) + (1 << 31)).clamp(0, u32::MAX as i64) as u32,
        ts: t.map_or(0, |t| (t - ctx.cws) as u32),
        pay,
        flags: valid as u32,
    })
}

#[inline]
fn parse_click(l: &[u8], ctx: &Ctx, camp_of: &U64Map, links: &links::Links, tc: &mut TsCache) -> Option<Ck> {
    let [id, user, camp, ts] = parse::fields::<4>(l)?;
    if id.is_empty() || user.is_empty() {
        return None;
    }
    let t = tc.parse(ts).filter(|t| (ctx.cws..ctx.we).contains(t))?;
    let camp = camp_of.get(key::pack(camp))?;
    Some(Ck { person: links.lookup(key::pack(user)), cid: key::pack(id), ts: (t - ctx.cws) as u32, camp })
}

/// One event partition: an open-addressing table on the bits below the partition bits keeps, per event_id,
/// the copy with the smallest ingest (the first seen on a tie: such copies are identical).
fn dedup(recs: &[Ev], tab: &mut Vec<u32>, bits: u32, out: &mut Pool<Ev2>) {
    if recs.is_empty() {
        return;
    }
    let cap = (recs.len() * 2).next_power_of_two().max(16);
    let (mask, shift) = (cap - 1, 64 - cap.trailing_zeros());
    tab.clear();
    tab.resize(cap, 0);
    for (i, r) in recs.iter().enumerate() {
        let mut h = ((mix(r.eid) << bits) >> shift) as usize;
        loop {
            let s = tab[h];
            if s == 0 {
                tab[h] = i as u32 + 1;
                break;
            }
            let j = (s - 1) as usize;
            if recs[j].eid == r.eid {
                if r.ingest < recs[j].ingest {
                    tab[h] = i as u32 + 1;
                }
                break;
            }
            h = (h + 1) & mask;
        }
    }
    for &s in tab.iter() {
        if s != 0 {
            let r = &recs[(s - 1) as usize];
            if r.flags & 1 == 1 {
                out.push((mix(r.person) >> (64 - bits)) as usize, Ev2 { person: r.person, ts: r.ts, pay: r.pay });
            }
        }
    }
}

fn manifest_ts(m: &[u8], key: &str) -> i64 {
    let key = format!("\"{key}\"");
    let at = m.windows(key.len()).position(|w| w == key.as_bytes()).unwrap() + key.len();
    let v = &m[at..];
    let v = &v[v.iter().position(|&b| b == b'"').unwrap() + 1..];
    parse::ts(&v[..v.iter().position(|&b| b == b'"').unwrap()]).unwrap()
}

/// A small file's data lines: everything after the header, split on `\n` only.
fn lines(f: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = f.iter().position(|&b| b == b'\n').map_or(&[][..], |i| &f[i + 1..]);
    body.strip_suffix(b"\n").unwrap_or(body).split(|&b| b == b'\n').filter(move |_| !body.is_empty())
}
