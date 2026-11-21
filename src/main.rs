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
mod scan;
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
use reader::{Line, Range, Reader};
use ts::TsCache;
use u64map::U64Map;

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const GAP: i64 = 1_800_000;

/// A parsed event copy, 28 bytes. Times are relative to `cws` (window_start - 7 d): `ts` in ms, `ingest` in
/// seconds biased by 2^31. `pay` = price | category << 24 for a purchase, else 0, with bit 31 = valid and not
/// late (categories therefore use 7 bits).
#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct Ev {
    eid: Key,
    person: Key,
    ingest: u32,
    ts: u32,
    pay: u32,
}

const VALID: u32 = 1 << 31;

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
    pub cws_s: i64,
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
    if cats.len() > 128 {
        eprintln!("{} categories: more than the 7-bit category index holds", cats.len());
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
    let ctx = Ctx { ws, we, cws, cws_s: cws.div_euclid(1000), day0, ndays, bits, ncamp, cats: cats.iter().map(|c| c.to_vec()).collect(), unknown };
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
            rd.fields::<2>(&link_files[r.file].0, r, |l| {
                let (a, b) = (l.get(0), l.get(1));
                if !a.is_empty() && !b.is_empty() {
                    let (ka, kb) = (key::pack(a), key::pack(b));
                    if ka != kb {
                        out.push((ka, kb));
                    }
                }
            });
        }
    });
    let links = links::Links::build(&edges, threads);
    drop(edges);
    lap("links");

    // Parse: event ranges then click ranges from one queue; records go to hash partitions.
    let mut tasks: Vec<(bool, Range)> = reader::ranges(&event_files, range).into_iter().map(|r| (true, r)).collect();
    tasks.extend(reader::ranges(&click_files, range).into_iter().map(|r| (false, r)));
    let next = AtomicUsize::new(0);
    let parsed: Vec<(Pool<Ev>, Pool<Ck>)> = parallel(threads, |_| {
        let (mut rd, mut tc) = (Reader::default(), TsCache::default());
        let (mut evs, mut cks) = (Pool::<Ev>::new(parts), Pool::<Ck>::new(parts));
        // Records wait in a ring while the link-table line of their user ID is prefetched; the lookup happens
        // PIPE lines later, by which time the line has arrived.
        let (mut epipe, mut cpipe) = (Pipe::<Ev>::default(), Pipe::<Ck>::default());
        let emit_ev = |mut e: Ev, evs: &mut Pool<Ev>| {
            e.person = links.lookup(e.person);
            let eid = e.eid;
            evs.push((mix(eid) >> (64 - bits)) as usize, e);
        };
        let emit_ck = |mut c: Ck, cks: &mut Pool<Ck>| {
            c.person = links.lookup(c.person);
            cks.push((mix(c.person) >> (64 - bits)) as usize, c);
        };
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some((is_event, r)) = tasks.get(i) else { break };
            if *is_event {
                rd.fields::<7>(&event_files[r.file].0, r, |l| {
                    let (slot, old) = epipe.slot();
                    if parse_event(l, &ctx, &cat_of, &mut tc, slot) {
                        let user = slot.person;
                        links.prefetch(user);
                        if let Some(old) = epipe.advance(old) {
                            emit_ev(old, &mut evs);
                        }
                    }
                });
            } else {
                rd.fields::<4>(&click_files[r.file].0, r, |l| {
                    if let Some(c) = parse_click(l, &ctx, &camp_of, &mut tc) {
                        links.prefetch(c.person);
                        if let Some(old) = cpipe.push(c) {
                            emit_ck(old, &mut cks);
                        }
                    }
                });
            }
        }
        for e in epipe.drain() {
            emit_ev(e, &mut evs);
        }
        for c in cpipe.drain() {
            emit_ck(c, &mut cks);
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
        let (mut recs, mut filter, mut suspects) = (Vec::new(), Vec::new(), Vec::new());
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
            dedup(&recs, &mut filter, &mut suspects, bits, &mut out);
        }
        out.finish();
        out
    });
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
    // Nothing is dropped; the big mappings are released on all threads so exit does not tear them down on one.
    parallel(threads, |t| {
        evs[t].release();
        cks[t].release();
        ev2[t].release();
    });
    std::process::exit(0);
}

/// `n` threads running `f(thread index)`; their results in order.
pub fn parallel<T: Send>(n: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    std::thread::scope(|s| {
        let f = &f;
        let handles: Vec<_> = (0..n).map(|t| s.spawn(move || f(t))).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// A fixed-size delay line: `push` returns the record pushed PIPE calls earlier.
struct Pipe<T: Copy> {
    ring: [T; PIPE],
    at: usize,
    filled: usize,
}

const PIPE: usize = 16;

impl<T: Copy> Default for Pipe<T> {
    fn default() -> Self {
        // The records are plain data; the zeroed slots are never read before being written.
        Pipe { ring: [unsafe { std::mem::zeroed() }; PIPE], at: 0, filled: 0 }
    }
}

impl<T: Copy> Pipe<T> {
    #[inline(always)]
    fn push(&mut self, v: T) -> Option<T> {
        let (slot, old) = self.slot();
        *slot = v;
        self.advance(old)
    }
    /// The slot the next record is built in, and what it held.
    #[inline(always)]
    fn slot(&mut self) -> (&mut T, T) {
        let old = self.ring[self.at];
        (&mut self.ring[self.at], old)
    }
    /// After the slot was filled: the record pushed PIPE calls earlier, once the ring is full.
    #[inline(always)]
    fn advance(&mut self, old: T) -> Option<T> {
        self.at = (self.at + 1) % PIPE;
        if self.filled < PIPE {
            self.filled += 1;
            None
        } else {
            Some(old)
        }
    }
    fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        let (at, filled) = (self.at, self.filled);
        self.filled = 0;
        (0..filled).map(move |i| self.ring[(at + PIPE - filled + i) % PIPE])
    }
}

/// Fills `e` from a line; false if the line yields no copy. Its `person` is still the user ID: the caller
/// resolves it through the link table.
#[inline(always)]
fn parse_event(l: &Line, ctx: &Ctx, cat_of: &U64Map, tc: &mut TsCache, e: &mut Ev) -> bool {
    let id = l.get(0);
    if id.is_empty() {
        return false;
    }
    let (user, ts, ingest, kind) = (l.get(1), l.get(2), l.get(3), l.get(4));
    // Fields point into the reader's buffer, which keeps 64 readable bytes past the data.
    let t = unsafe { tc.parse_raw(ts.as_ptr(), ts.len()) }.filter(|&t| t >= ctx.ws && t < ctx.we);
    // ponytail: rules.md guarantees ingest_ts is valid F1; a bad one sorts last and fails the late check.
    let ing = unsafe { tc.parse_raw(ingest.as_ptr(), ingest.len()) }.unwrap_or(i64::MAX);
    let purchase = kind.len() == 8 && u64::from_le_bytes(kind.try_into().unwrap()) == u64::from_le_bytes(*b"purchase");
    let kind_ok = purchase
        || (kind.len() == 4 && {
            let k = u32::from_le_bytes(kind.try_into().unwrap());
            k == u32::from_le_bytes(*b"view") || k == u32::from_le_bytes(*b"cart")
        });
    let product = l.get(5);
    let mut valid = !user.is_empty() && !product.is_empty() && kind_ok && t.is_some_and(|t| ing - t <= DAY);
    let mut pay = 0;
    if valid && purchase {
        match parse::price(l.get(6)) {
            Some(v) => pay = v | cat_of.get(unsafe { key::pack16(product.as_ptr(), product.len()) }).unwrap_or(ctx.unknown) << 24,
            None => valid = false,
        }
    }
    e.eid = unsafe { key::pack16(id.as_ptr(), id.len()) };
    e.person = if valid { unsafe { key::pack16(user.as_ptr(), user.len()) } } else { 0 };
    // F1 is whole seconds, so / is exact; biased by 2^31 so copies compare right for ±68 years.
    e.ingest = (ing / 1000 - ctx.cws_s + (1 << 31)).clamp(0, u32::MAX as i64) as u32;
    e.ts = t.map_or(0, |t| (t - ctx.cws) as u32);
    e.pay = pay | if valid { VALID } else { 0 };
    true
}

/// The click's `person` is still its user ID: the caller resolves it through the link table.
#[inline]
fn parse_click(l: &Line, ctx: &Ctx, camp_of: &U64Map, tc: &mut TsCache) -> Option<Ck> {
    let (id, user, camp, ts) = (l.get(0), l.get(1), l.get(2), l.get(3));
    if id.is_empty() || user.is_empty() {
        return None;
    }
    let t = unsafe { tc.parse_raw(ts.as_ptr(), ts.len()) }.filter(|&t| t >= ctx.cws && t < ctx.we)?;
    let camp = camp_of.get(unsafe { key::pack16(camp.as_ptr(), camp.len()) })?;
    Some(Ck {
        person: unsafe { key::pack16(user.as_ptr(), user.len()) },
        cid: unsafe { key::pack16(id.as_ptr(), id.len()) },
        ts: (t - ctx.cws) as u32,
        camp,
    })
}

/// One event partition. A two-bit counter per hash slot (16 slots per copy) tells which event_ids were
/// seen more than once: the others, nearly all of them, are emitted straight from one sequential pass;
/// the suspects are sorted by (event_id, ingest) so the earliest copy of each wins. Valid winners go to
/// their person's partition. Copies with equal ingest are identical, so which one wins is immaterial.
fn dedup(recs: &[Ev], filter: &mut Vec<u64>, suspects: &mut Vec<u32>, bits: u32, out: &mut Pool<Ev2>) {
    if recs.is_empty() {
        return;
    }
    let slots = (recs.len() * 16).next_power_of_two().max(64);
    let shift = 64 - slots.trailing_zeros();
    filter.clear();
    filter.resize(slots / 32, 0);
    for r in recs {
        let h = ((mix(r.eid) << bits) >> shift) as usize;
        let (w, sh) = (&mut filter[h >> 5], (h & 31) * 2);
        let seen = (*w >> sh) & 1;
        *w |= (1 | seen << 1) << sh;
    }
    let emit = |r: Ev, out: &mut Pool<Ev2>| {
        if r.pay & VALID != 0 {
            out.push((mix(r.person) >> (64 - bits)) as usize, Ev2 { person: r.person, ts: r.ts, pay: r.pay & !VALID });
        }
    };
    suspects.clear();
    for (i, r) in recs.iter().enumerate() {
        let h = ((mix(r.eid) << bits) >> shift) as usize;
        if (filter[h >> 5] >> ((h & 31) * 2)) & 2 != 0 {
            suspects.push(i as u32);
        } else {
            emit(*r, out);
        }
    }
    suspects.sort_unstable_by_key(|&i| (recs[i as usize].eid, recs[i as usize].ingest));
    let mut k = 0;
    while k < suspects.len() {
        let r = recs[suspects[k] as usize];
        emit(r, out);
        k += 1;
        while k < suspects.len() && recs[suspects[k] as usize].eid == r.eid {
            k += 1;
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
