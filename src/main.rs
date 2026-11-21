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
use reader::{Fields, Handler, Range, Reader};
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
    // Campaign IDs of the files' own shape, `c_` + a number without a leading zero, index an array; every
    // other spelling (` c_1`, `C_1`, `c_01`) is a different ID and goes through the exact map.
    let mut camp_num: Vec<u32> = Vec::new();
    for (i, [c, _]) in campaigns.iter().enumerate() {
        if !c.is_empty() {
            camp_of.insert(key::pack(c), i as u32);
        }
        if let Some(n) = numbered(c, b"c_") {
            if camp_num.len() <= n {
                camp_num.resize(n + 1, 0);
            }
            camp_num[n] = i as u32 + 1;
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
        let (mut rd, mut out) = (Reader::default(), LinkLines(Vec::new()));
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(r) = link_ranges.get(i) else { break out.0 };
            rd.fields::<2>(&link_files[r.file].0, r, &mut out);
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
        let mut rd = Reader::default();
        let mut w = Parser {
            ctx: &ctx,
            cat_of: &cat_of,
            camp_of: &camp_of,
            camp_num: &camp_num,
            links: &links,
            tc: TsCache::default(),
            evs: Pool::<Ev>::new(parts),
            cks: Pool::<Ck>::new(parts),
            epipe: Pipe::default(),
            cpipe: Pipe::default(),
        };
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some((is_event, r)) = tasks.get(i) else { break };
            if *is_event {
                rd.fields::<7>(&event_files[r.file].0, r, &mut w);
            } else {
                rd.fields::<4>(&click_files[r.file].0, r, &mut w);
            }
        }
        w.finish()
    });
    let (evs, cks): (Vec<Pool<Ev>>, Vec<Pool<Ck>>) = parsed.into_iter().unzip();
    lap("parse");
    // Dedup per event partition: keep the copy with the smallest ingest_ts, emit it if valid, by person.
    let next = AtomicUsize::new(0);
    let ev2: Vec<Pool<Ev2>> = parallel(threads, |_| {
        let (mut filter, mut suspects) = (Vec::new(), Vec::new());
        let mut out = Pool::<Ev2>::new(parts);
        loop {
            let p = next.fetch_add(1, Ordering::Relaxed);
            if p >= parts {
                break;
            }
            let blocks: Vec<&[Ev]> = evs.iter().flat_map(|t| t.blocks(p)).collect();
            dedup(&blocks, &mut filter, &mut suspects, bits, &mut out);
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

/// The number in an ID of the shape `prefix` + 1..=7 ASCII digits without a leading zero.
#[inline(always)]
fn numbered(id: &[u8], prefix: &[u8]) -> Option<usize> {
    let digits = id.strip_prefix(prefix)?;
    if digits.is_empty() || digits.len() > 7 || (digits[0] == b'0' && digits.len() > 1) {
        return None;
    }
    let mut n = 0usize;
    for &d in digits {
        if !d.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (d - b'0') as usize;
    }
    Some(n)
}

/// `n` threads running `f(thread index)`; their results in order.
pub fn parallel<T: Send>(n: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    std::thread::scope(|s| {
        let f = &f;
        let handles: Vec<_> = (0..n).map(|t| s.spawn(move || f(t))).collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// The link lines of one thread as key pairs.
struct LinkLines(Vec<(Key, Key)>);

impl Handler<2> for LinkLines {
    #[inline(always)]
    fn line<'a>(&mut self, mut f: impl Fields<'a>) {
        let (a, b) = (f.next(), f.next());
        if !a.is_empty() && !b.is_empty() {
            let (ka, kb) = (key::pack(a), key::pack(b));
            if ka != kb {
                self.0.push((ka, kb));
            }
        }
    }
}

/// One thread's parse state. Records wait in a ring while the link-table line of their user ID is prefetched;
/// the lookup happens PIPE lines later, by which time the line has arrived.
struct Parser<'a> {
    ctx: &'a Ctx,
    cat_of: &'a U64Map,
    camp_of: &'a U64Map,
    camp_num: &'a [u32],
    links: &'a links::Links,
    tc: TsCache,
    evs: Pool<Ev>,
    cks: Pool<Ck>,
    epipe: Pipe<Ev>,
    cpipe: Pipe<Ck>,
}

impl Parser<'_> {
    #[inline(always)]
    fn emit_ev(&mut self, mut e: Ev) {
        e.person = self.links.lookup(e.person);
        let eid = e.eid;
        self.evs.push((mix(eid) >> (64 - self.ctx.bits)) as usize, e);
    }

    #[inline(always)]
    fn emit_ck(&mut self, mut c: Ck) {
        c.person = self.links.lookup(c.person);
        self.cks.push((mix(c.person) >> (64 - self.ctx.bits)) as usize, c);
    }

    fn finish(mut self) -> (Pool<Ev>, Pool<Ck>) {
        for e in self.epipe.drain().collect::<Vec<_>>() {
            self.emit_ev(e);
        }
        for c in self.cpipe.drain().collect::<Vec<_>>() {
            self.emit_ck(c);
        }
        self.evs.finish();
        self.cks.finish();
        (self.evs, self.cks)
    }
}

impl Handler<7> for Parser<'_> {
    #[inline(always)]
    fn line<'a>(&mut self, f: impl Fields<'a>) {
        if let Some(e) = parse_event(f, self.ctx, self.cat_of, &mut self.tc) {
            self.links.prefetch(e.person);
            if let Some(old) = self.epipe.push(e) {
                self.emit_ev(old);
            }
        }
    }
}

impl Handler<4> for Parser<'_> {
    #[inline(always)]
    fn line<'a>(&mut self, f: impl Fields<'a>) {
        if let Some(c) = parse_click(f, self.ctx, self.camp_of, self.camp_num, &mut self.tc) {
            self.links.prefetch(c.person);
            if let Some(old) = self.cpipe.push(c) {
                self.emit_ck(old);
            }
        }
    }
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
        let old = std::mem::replace(&mut self.ring[self.at], v);
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

/// The copy a line yields, if any. Its `person` is still the user ID: the caller resolves it through the link
/// table.
#[inline(always)]
fn parse_event<'a>(mut f: impl Fields<'a>, ctx: &Ctx, cat_of: &U64Map, tc: &mut TsCache) -> Option<Ev> {
    let id = f.next();
    if id.is_empty() {
        return None;
    }
    let (user, ts, ingest, kind) = (f.next(), f.next(), f.next(), f.next());
    // Fields point into the reader's buffer, which keeps readable bytes past the data.
    let t = unsafe { tc.parse_raw(ts.as_ptr(), ts.len()) }.filter(|&t| t >= ctx.ws && t < ctx.we);
    // ponytail: rules.md guarantees ingest_ts is valid F1; a bad one sorts last and fails the late check.
    let ing = unsafe { tc.parse_raw(ingest.as_ptr(), ingest.len()) }.unwrap_or(i64::MAX);
    let purchase = kind.len() == 8 && u64::from_le_bytes(kind.try_into().unwrap()) == u64::from_le_bytes(*b"purchase");
    let kind_ok = purchase
        || (kind.len() == 4 && {
            let k = u32::from_le_bytes(kind.try_into().unwrap());
            (k == u32::from_le_bytes(*b"view")) | (k == u32::from_le_bytes(*b"cart"))
        });
    let (product, price) = (f.next(), f.next());
    let mut valid = !user.is_empty() && !product.is_empty() && kind_ok && t.is_some_and(|t| ing - t <= DAY);
    let mut pay = 0;
    if valid && purchase {
        match parse::price(price) {
            Some(v) => pay = v | cat_of.get(unsafe { key::pack16(product.as_ptr(), product.len()) }).unwrap_or(ctx.unknown) << 24,
            None => valid = false,
        }
    }
    Some(Ev {
        eid: unsafe { key::pack16(id.as_ptr(), id.len()) },
        person: if valid { unsafe { key::pack16(user.as_ptr(), user.len()) } } else { 0 },
        // F1 is whole seconds, so / is exact; biased by 2^31 so copies compare right for ±68 years.
        ingest: (ing / 1000 - ctx.cws_s + (1 << 31)).clamp(0, u32::MAX as i64) as u32,
        ts: t.map_or(0, |t| (t - ctx.cws) as u32),
        pay: pay | if valid { VALID } else { 0 },
    })
}

/// The click's `person` is still its user ID: the caller resolves it through the link table.
#[inline(always)]
fn parse_click<'a>(mut f: impl Fields<'a>, ctx: &Ctx, camp_of: &U64Map, camp_num: &[u32], tc: &mut TsCache) -> Option<Ck> {
    let (id, user, camp, ts) = (f.next(), f.next(), f.next(), f.next());
    if id.is_empty() || user.is_empty() {
        return None;
    }
    let t = unsafe { tc.parse_raw(ts.as_ptr(), ts.len()) }.filter(|&t| t >= ctx.cws && t < ctx.we)?;
    let camp = match numbered(camp, b"c_").and_then(|n| camp_num.get(n)).copied() {
        Some(i) if i != 0 => i - 1,
        _ => camp_of.get(unsafe { key::pack16(camp.as_ptr(), camp.len()) })?,
    };
    Some(Ck {
        person: unsafe { key::pack16(user.as_ptr(), user.len()) },
        cid: unsafe { key::pack16(id.as_ptr(), id.len()) },
        ts: (t - ctx.cws) as u32,
        camp,
    })
}

/// One event partition, read in place from its blocks. A two-bit counter per hash slot (32 slots per copy)
/// tells which event_ids were seen more than once: the others, nearly all of them, are emitted straight from
/// the second pass; the suspects are sorted by event_id and the copy with the smallest ingest of each wins.
/// Valid winners go to their person's partition. Copies with equal ingest are identical, so ties are immaterial.
fn dedup(blocks: &[&[Ev]], filter: &mut Vec<u64>, suspects: &mut Vec<Ev>, bits: u32, out: &mut Pool<Ev2>) {
    let n: usize = blocks.iter().map(|b| b.len()).sum();
    if n == 0 {
        return;
    }
    let slots = (n * 32).next_power_of_two().max(64);
    let shift = 64 - slots.trailing_zeros();
    filter.clear();
    filter.resize(slots / 32, 0);
    for r in blocks.iter().flat_map(|b| b.iter()) {
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
    for r in blocks.iter().flat_map(|b| b.iter()) {
        let h = ((mix(r.eid) << bits) >> shift) as usize;
        if (filter[h >> 5] >> ((h & 31) * 2)) & 2 != 0 {
            suspects.push(*r);
        } else {
            emit(*r, out);
        }
    }
    suspects.sort_unstable_by_key(|r| r.eid);
    let mut k = 0;
    while k < suspects.len() {
        let mut best = suspects[k];
        k += 1;
        while k < suspects.len() && suspects[k].eid == best.eid {
            if suspects[k].ingest < best.ingest {
                best = suspects[k];
            }
            k += 1;
        }
        emit(best, out);
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
