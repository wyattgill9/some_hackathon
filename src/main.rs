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
/// Staging bytes per partition of the event streams and of the click stream (see pool.rs).
const EV_STAGE: usize = 64;
const CK_STAGE: usize = 192;

/// A parsed event copy as the parser builds it: the dedup key (`eid`, `ingest` in ms since the epoch) and the
/// payload, which go to two parallel record streams of the event's partition.
#[derive(Clone, Copy)]
struct Ev {
    key: EvKey,
    pay: Ev2,
}

/// What dedup compares: copies of an event share `eid`; the one with the smallest `ingest` wins.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct EvKey {
    eid: Key,
    ingest: i64,
}

/// An event's payload, and a surviving event. `person` is 0 when the copy is invalid or late (a valid event's
/// user ID is never empty). `ts` is in ms relative to `cws` (window_start - 7 d). `pay` = price | category << 24
/// for a purchase, else 0.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Ev2 {
    pub person: Key,
    pub ts: u32,
    pub pay: u32,
}

/// A valid click, 24 bytes: eight per three staging lines.
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
    let parsed: Vec<(Pool<EvKey, EV_STAGE>, Pool<Ev2, EV_STAGE>, Pool<Ck, CK_STAGE>)> = parallel(threads, |_| {
        let (mut rd, mut tc) = (Reader::default(), TsCache::default());
        let mut evs = (Pool::<EvKey, EV_STAGE>::new(parts), Pool::<Ev2, EV_STAGE>::new(parts));
        let mut cks = Pool::<Ck, CK_STAGE>::new(parts);
        // Records wait in a ring while the link-table line of their user ID is prefetched; the lookup happens
        // PIPE lines later, by which time the line has arrived.
        let (mut epipe, mut cpipe) = (Pipe::<Ev>::default(), Pipe::<Ck>::default());
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some((is_event, r)) = tasks.get(i) else { break };
            if *is_event {
                rd.fields::<7>(&event_files[r.file].0, r, |l| {
                    let (slot, old) = epipe.slot();
                    if parse_event(l, &ctx, &cat_of, &mut tc, slot) {
                        let user = slot.pay.person;
                        links.prefetch(user);
                        if let Some(old) = epipe.advance(old) {
                            emit_ev(old, &links, bits, &mut evs);
                        }
                    }
                });
            } else {
                rd.fields::<4>(&click_files[r.file].0, r, |l| {
                    if let Some(c) = parse_click(l, &ctx, &camp_of, &camp_num, &mut tc) {
                        links.prefetch(c.person);
                        if let Some(old) = cpipe.push(c) {
                            emit_ck(old, &links, bits, &mut cks);
                        }
                    }
                });
            }
        }
        for e in epipe.drain() {
            emit_ev(e, &links, bits, &mut evs);
        }
        for c in cpipe.drain() {
            emit_ck(c, &links, bits, &mut cks);
        }
        evs.0.finish2(&mut evs.1);
        cks.finish();
        (evs.0, evs.1, cks)
    });
    let (mut evk, mut evp, mut cks) = (Vec::new(), Vec::new(), Vec::new());
    for (k, p, c) in parsed {
        evk.push(k);
        evp.push(p);
        cks.push(c);
    }
    lap("parse");

    // Dedup per event partition: keep the copy with the smallest ingest_ts, emit it if valid, by person. The
    // output blocks are carved from the partition's consumed input blocks.
    let next = AtomicUsize::new(0);
    let ev2: Vec<Pool<Ev2, EV_STAGE>> = parallel(threads, |_| {
        let (mut table, mut dropped) = (Vec::new(), Vec::new());
        let mut out = Pool::<Ev2, EV_STAGE>::new(parts);
        loop {
            let p = next.fetch_add(1, Ordering::Relaxed);
            if p >= parts {
                break;
            }
            let keys: Vec<&[EvKey]> = evk.iter().flat_map(|t| t.blocks(p)).collect();
            let pays: Vec<&[Ev2]> = evp.iter().flat_map(|t| t.blocks(p)).collect();
            dedup(&keys, &pays, &mut table, &mut dropped, bits, &mut out);
            for b in keys.iter().map(|b| b.as_ptr() as *mut u8).chain(pays.iter().map(|b| b.as_ptr() as *mut u8)) {
                out.recycle(b);
            }
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
        evk[t].release();
        evp[t].release();
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
            (k == u32::from_le_bytes(*b"view")) | (k == u32::from_le_bytes(*b"cart"))
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
    e.key.eid = unsafe { key::pack16(id.as_ptr(), id.len()) };
    e.key.ingest = ing;
    e.pay.person = if valid { unsafe { key::pack16(user.as_ptr(), user.len()) } } else { 0 };
    e.pay.ts = t.map_or(0, |t| (t - ctx.cws) as u32);
    e.pay.pay = pay;
    true
}

/// An event copy, its user ID resolved to its person, into its event partition's key and payload streams.
#[inline(always)]
fn emit_ev(e: Ev, links: &links::Links, bits: u32, (keys, pays): &mut (Pool<EvKey, EV_STAGE>, Pool<Ev2, EV_STAGE>)) {
    let p = (mix(e.key.eid) >> (64 - bits)) as usize;
    // The payload is built in registers: a wide load of a record patched in memory would wait for the patch.
    keys.push2(pays, p, e.key, Ev2 { person: links.lookup(e.pay.person), ts: e.pay.ts, pay: e.pay.pay });
}

/// A click, its user ID resolved to its person, into its person partition.
#[inline(always)]
fn emit_ck(c: Ck, links: &links::Links, bits: u32, cks: &mut Pool<Ck, CK_STAGE>) {
    let person = links.lookup(c.person);
    cks.push((mix(person) >> (64 - bits)) as usize, Ck { person, cid: c.cid, ts: c.ts, camp: c.camp });
}

/// The click's `person` is still its user ID: the caller resolves it through the link table.
#[inline]
fn parse_click(l: &Line, ctx: &Ctx, camp_of: &U64Map, camp_num: &[u32], tc: &mut TsCache) -> Option<Ck> {
    let (id, user, camp, ts) = (l.get(0), l.get(1), l.get(2), l.get(3));
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

/// A line of the dedup table: a count, then 15 slots of `tag << 20 | position`, filled in order.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
pub struct TLine([u32; 16]);

/// A record's position in its partition: block index << 10 | index in the block (blocks hold at most 1024).
const POS_BITS: u32 = 20;
const POS_MASK: u32 = (1 << POS_BITS) - 1;

/// One event partition: the key stream and the payload stream, parallel, read in place from their blocks.
/// The key pass walks every (event_id, ingest) through an open-addressing table whose lines of 15 slots are
/// probed with one compare each: a copy whose event_id is already in the table loses to the copy there or
/// replaces it, and the loser's position goes into a bitmap. The payload pass then copies every valid,
/// undropped payload to its person's partition. Copies with equal ingest are identical, so ties are immaterial.
fn dedup(keys: &[&[EvKey]], pays: &[&[Ev2]], table: &mut Vec<TLine>, dropped: &mut Vec<u64>, bits: u32, out: &mut Pool<Ev2, EV_STAGE>) {
    let n: usize = keys.iter().map(|b| b.len()).sum();
    if n == 0 {
        return;
    }
    debug_assert!(keys.len() == pays.len() && keys.iter().zip(pays).all(|(k, p)| k.len() == p.len()));
    dropped.clear();
    dropped.resize(keys.len() << (10 - 6), 0);
    if keys.len() > 1 << (POS_BITS - 10) {
        dedup_big(keys, dropped);
    } else {
        // Lines of 15 slots at a load of 33-67%: a line is rarely full, and then the next one is probed.
        let lines = (n / 10).next_power_of_two().max(2);
        // The partition took the top `bits` of the hash; the line takes the next ones, the tag 12 more.
        let lsh = 64 - bits - lines.trailing_zeros();
        let tsh = lsh.saturating_sub(12);
        table.clear();
        table.resize(lines, TLine([0; 16]));
        for (b, blk) in keys.iter().enumerate() {
            for (j, r) in blk.iter().enumerate() {
                let pos = (b << 10 | j) as u32;
                let h = mix(r.eid);
                let tag = (h >> tsh) as u32 & 0xfff | 1;
                let mut li = (h >> lsh) as usize & (lines - 1);
                // In bounds: li is masked, positions come from this walk, and the table is never full.
                unsafe {
                    loop {
                        let line = table.as_mut_ptr().add(li);
                        let mut found = probe(line, tag);
                        while found != 0 {
                            let s = found.trailing_zeros() as usize;
                            let q = (*line).0[s] & POS_MASK;
                            let other = keys.get_unchecked(q as usize >> 10).get_unchecked(q as usize & 1023);
                            if other.eid == r.eid {
                                let loser = if r.ingest < other.ingest {
                                    (*line).0[s] = tag << POS_BITS | pos;
                                    q
                                } else {
                                    pos
                                };
                                *dropped.get_unchecked_mut(loser as usize >> 6) |= 1 << (loser & 63);
                                break;
                            }
                            found &= found - 1;
                        }
                        if found != 0 {
                            break;
                        }
                        let count = (*line).0[0] as usize;
                        if count < 15 {
                            (*line).0[count + 1] = tag << POS_BITS | pos;
                            (*line).0[0] = count as u32 + 1;
                            break;
                        }
                        li = (li + 1) & (lines - 1);
                    }
                }
            }
        }
    }
    for (b, blk) in pays.iter().enumerate() {
        for (c, chunk) in blk.chunks(64).enumerate() {
            let mut word = dropped[b << 4 | c];
            for e in chunk {
                let keep = e.person != 0 && word & 1 == 0;
                word >>= 1;
                if keep {
                    out.push((mix(e.person) >> (64 - bits)) as usize, *e);
                }
            }
        }
    }
}

/// The slots of `line` holding `tag`, as a bit mask (bit 0, the count, never matches: tags are nonzero).
#[inline(always)]
unsafe fn probe(line: *const TLine, tag: u32) -> u32 {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let lo = _mm256_load_si256(line as *const __m256i);
        let hi = _mm256_load_si256((line as *const __m256i).add(1));
        let t = _mm256_set1_epi32((tag << POS_BITS) as i32);
        let m = _mm256_set1_epi32(!POS_MASK as i32);
        _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_and_si256(lo, m), t))) as u32
            | (_mm256_movemask_ps(_mm256_castsi256_ps(_mm256_cmpeq_epi32(_mm256_and_si256(hi, m), t))) as u32) << 8
    }
    #[cfg(not(target_feature = "avx2"))]
    unsafe {
        let mut f = 0;
        for (s, &v) in (*line).0.iter().enumerate() {
            f |= ((v >> POS_BITS == tag) as u32) << s;
        }
        f
    }
}

/// A partition too large for 20-bit positions (over a million copies): every key sorted, losers marked.
#[cold]
fn dedup_big(keys: &[&[EvKey]], dropped: &mut Vec<u64>) {
    let mut all: Vec<(Key, i64, usize)> = keys.iter().enumerate().flat_map(|(b, blk)| blk.iter().enumerate().map(move |(j, r)| (r.eid, r.ingest, b << 10 | j))).collect();
    all.sort_unstable();
    for w in all.windows(2) {
        if w[0].0 == w[1].0 {
            dropped[w[1].2 >> 6] |= 1 << (w[1].2 & 63);
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
