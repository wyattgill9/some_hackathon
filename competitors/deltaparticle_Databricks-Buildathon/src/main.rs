//! Shopping Sessions: a hand-written pipeline for rules.md.
//!
//! Phase 1 (parallel, newline-aligned chunks of mmapped CSVs): parse events and
//! clicks; events are routed by a hash of event_id. Links are joined on their
//! own thread meanwhile (union-find, smallest ID names the person).
//! Phase 2 (parallel over event_id partitions): dedup on event_id keeping the
//! smallest ingest_ts (a bitmap sends only possible duplicates through a hash
//! table), then route surviving events and clicks by person.
//! Phase 3 (parallel over person partitions): group by person with a counting
//! sort, sort each person's rows by time, sessionize, credit and assist,
//! aggregate. Each worker writes its own user_daily Parquet file.

mod csv;
mod index;
mod keys;
mod out;
mod sink;
mod ts;

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::thread;
use std::time::Instant;

use memmap2::Mmap;
use mimalloc::MiMalloc;
use sink::{Region, Sink};

use ts::DAY;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const GAP: i64 = 30 * 60 * 1000;
const REACH: i64 = 7 * DAY;
const P_BITS: u32 = 9;
const P: usize = 1 << P_BITS;
const Q_BITS: u32 = 7;
const Q: usize = 1 << Q_BITS;
const NONE: u32 = u32::MAX;
const CHUNK: usize = 2 << 20;
const EMPTY: u64 = u64::MAX;
/// `ip` = ingest seconds (biased, 40 bits) << 24 | price (24 bits).
const PRICE_MASK: u64 = (1 << 24) - 1;
const PRICE_INVALID: u64 = PRICE_MASK;
const INGEST_BIAS: i64 = 1 << 39;

#[inline(always)]
fn mix(k: u64) -> u64 {
    let mut x = k ^ (k >> 31);
    x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 29;
    x.wrapping_mul(0xBF58_476D_1CE4_E5B9)
}

#[inline(always)]
fn part(h: u64, bits: u32) -> usize {
    (h >> (64 - bits)) as usize
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch(p as *const i8, _MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// ID -> value, with a dense array for `<prefix><canonical number>` IDs.
struct Lookup {
    prefix: &'static [u8],
    by_num: Vec<u32>,
    map: HashMap<Vec<u8>, u32>,
}

impl Lookup {
    fn new(prefix: &'static [u8]) -> Self {
        Self { prefix, by_num: Vec::new(), map: HashMap::new() }
    }
    #[inline(always)]
    fn number(&self, s: &[u8]) -> Option<usize> {
        let p = self.prefix.len();
        if s.len() <= p || s.len() > p + 7 || &s[..p] != self.prefix || s[p] == b'0' {
            return None;
        }
        let mut v = 0usize;
        for &b in &s[p..] {
            let d = b.wrapping_sub(b'0');
            if d >= 10 {
                return None;
            }
            v = v * 10 + d as usize;
        }
        Some(v)
    }
    fn insert(&mut self, id: &[u8], value: u32) {
        match self.number(id) {
            Some(n) => {
                if self.by_num.len() <= n {
                    self.by_num.resize(n + 1, NONE);
                }
                self.by_num[n] = value;
            }
            None => {
                self.map.insert(id.to_vec(), value);
            }
        }
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> Option<u32> {
        match self.number(s) {
            Some(n) => self.by_num.get(n).copied().filter(|&v| v != NONE),
            None => self.map.get(s).copied(),
        }
    }
}

struct Ctx {
    ws: i64,
    we: i64,
    cws: i64,
    day0: i64,
    ndays: usize,
    products: Lookup,
    unknown_cat: u32,
    cat_names: Vec<Vec<u8>>,
    campaigns: Lookup,
    camp_ids: Vec<Vec<u8>>,
    channels: Vec<Vec<u8>>,
}

/// A parsed event copy, routed by event_id.
#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Ev {
    ekey: u64,
    ukey: u64,
    /// ingest << 24 | price; price PRICE_INVALID if the copy fails validation.
    ip: u64,
    /// ts - window_start.
    ts: u32,
    cat: u32,
}

/// A valid click.
#[derive(Clone, Copy, Default)]
struct Cl {
    ukey: u64,
    ckey: u64,
    /// ts - (window_start - 7 days).
    ts: u32,
    camp: u32,
}

/// A surviving event, routed by person.
#[derive(Clone, Copy, Default)]
struct PEv {
    person: u64,
    /// ts << 32 | price << 8 | category: sorts by time.
    w: u64,
}

/// A click, routed by person.
#[derive(Clone, Copy, Default)]
struct PCl {
    person: u64,
    ckey: u64,
    ts: u32,
    camp: u32,
}


/// One click of a grouped person.
#[derive(Clone, Copy, Default)]
struct C3 {
    ckey: u64,
    ts: u32,
    camp: u32,
}

fn pool<S: Send>(
    threads: usize,
    tasks: usize,
    init: impl Fn() -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    pool_finish(threads, tasks, init, work, |_, _| {})
}

/// Like `pool`, and each thread runs `finish(state, thread_index)` as soon as
/// no tasks are left for it (while other threads may still be working).
fn pool_finish<S: Send>(
    threads: usize,
    tasks: usize,
    init: impl Fn() -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
    finish: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|i| {
                let (next, init, work, finish) = (&next, &init, &work, &finish);
                scope.spawn(move || {
                    let mut state = init();
                    loop {
                        let t = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if t >= tasks {
                            break;
                        }
                        work(&mut state, t);
                    }
                    finish(&mut state, i);
                    state
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

/// Memory-map every non-empty file. Mapping lets the scanner read the OS
/// page cache directly: no copy into a buffer that would fall out of L2.
fn map_files(paths: &[PathBuf]) -> Vec<Mmap> {
    paths
        .iter()
        .filter_map(|p| {
            let f = std::fs::File::open(p).expect("open input");
            if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
                return None;
            }
            Some(unsafe { Mmap::map(&f).expect("mmap input") })
        })
        .collect()
}

/// Start the kernel reading every mapping now, in order (cold page cache).
#[cfg(unix)]
fn readahead_all(maps: &[&Mmap]) {
    for m in maps {
        let _ = m.advise(memmap2::Advice::Sequential);
        let _ = m.advise(memmap2::Advice::WillNeed);
    }
}

/// Newline-aligned byte ranges (file index, start, end) of every body.
fn chunks(maps: &[Mmap]) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    for (fi, m) in maps.iter().enumerate() {
        let len = m.len();
        let mut pos = len - body(m).len();
        while pos < len {
            let mut end = (pos + CHUNK).min(len);
            if end < len {
                end = match memchr::memchr(b'\n', &m[end..]) {
                    Some(i) => end + i + 1,
                    None => len,
                };
            }
            out.push((fi, pos, end));
            pos = end;
        }
    }
    out
}

/// The data lines of a file: everything after the header line.
fn body(m: &[u8]) -> &[u8] {
    match memchr::memchr(b'\n', m) {
        Some(i) => &m[i + 1..],
        None => &[],
    }
}

fn read_small(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = std::fs::read(path).unwrap_or_default();
    let mut rows = Vec::new();
    csv::for_lines(body(&data), |line| {
        let mut f = [&b""[..]; 2];
        if csv::split(line, &mut f) && !f[0].is_empty() {
            rows.push((f[0].to_vec(), f[1].to_vec()));
        }
    });
    rows
}

fn manifest_value(text: &str, key: &str) -> i64 {
    let at = text.find(&format!("\"{key}\"")).expect("manifest key");
    let rest = &text[at + key.len() + 2..];
    let rest = &rest[rest.find(':').expect("manifest colon") + 1..];
    let rest = &rest[rest.find('"').expect("manifest quote") + 1..];
    let value = &rest[..rest.find('"').expect("manifest quote")];
    ts::parse(value.as_bytes()).expect("manifest timestamp")
}

fn load_ctx(data: &Path) -> Ctx {
    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    assert!(
        we - ws + REACH < u32::MAX as i64,
        "window longer than ~42 days is not supported by the compact records"
    );

    let product_rows = read_small(&data.join("products.csv"));
    let mut names: BTreeSet<Vec<u8>> = product_rows.iter().map(|r| r.1.clone()).collect();
    names.insert(b"unknown".to_vec());
    let cat_names: Vec<Vec<u8>> = names.into_iter().collect();
    assert!(cat_names.len() <= 256, "more than 255 product categories is not supported by the compact records");
    let cat_index: HashMap<&[u8], u32> =
        cat_names.iter().enumerate().map(|(i, n)| (n.as_slice(), i as u32)).collect();
    let mut products = Lookup::new(b"p");
    for (id, cat) in &product_rows {
        products.insert(id, cat_index[cat.as_slice()]);
    }
    let unknown_cat = cat_index[&b"unknown"[..]];

    let campaign_rows = read_small(&data.join("campaigns.csv"));
    let mut campaigns = Lookup::new(b"c_");
    let mut camp_ids = Vec::new();
    let mut channels = Vec::new();
    for (id, channel) in campaign_rows {
        campaigns.insert(&id, camp_ids.len() as u32);
        camp_ids.push(id);
        channels.push(channel);
    }
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    Ctx {
        ws,
        we,
        cws: ws - REACH,
        day0,
        ndays,
        products,
        unknown_cat,
        cat_names,
        campaigns,
        camp_ids,
        channels,
    }
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

#[inline(always)]
fn parse_event(f: &[&[u8]; 7], ctx: &Ctx, buckets: &mut Sink<Ev>, tsc: &mut ts::DateCache) {
    if f[0].is_empty() {
        return;
    }
    let ekey = keys::key(f[0], b'e');
    let ingest = ts::parse_c(f[3], tsc);
    let ingest_key = match ingest {
        Some(t) => (t.div_euclid(1000) + INGEST_BIAS).clamp(0, (1 << 40) - 1) as u64,
        None => (1 << 40) - 1,
    };
    let mut ev = Ev { ekey, ukey: 0, ip: ingest_key << 24 | PRICE_INVALID, ts: 0, cat: 0 };
    'valid: {
        if f[1].is_empty() || f[5].is_empty() {
            break 'valid;
        }
        let Some(t) = ts::parse_c(f[2], tsc) else { break 'valid };
        if t < ctx.ws || t >= ctx.we {
            break 'valid;
        }
        let price = match f[4] {
            b"view" | b"cart" => 0,
            b"purchase" => match parse_price(f[6]) {
                Some(p) => p,
                None => break 'valid,
            },
            _ => break 'valid,
        };
        match ingest {
            Some(i) if i - t <= DAY => {}
            _ => break 'valid,
        }
        ev.ukey = keys::key(f[1], b'u');
        ev.ts = (t - ctx.ws) as u32;
        ev.ip = ingest_key << 24 | price as u64;
        if price > 0 {
            ev.cat = ctx.products.get(f[5]).unwrap_or(ctx.unknown_cat);
        }
    }
    buckets.push(part(mix(ekey), P_BITS), ev);
}

#[inline(always)]
fn parse_click(f: &[&[u8]; 4], ctx: &Ctx, aliases: &Aliases, out: &mut Sink<PCl>, tsc: &mut ts::DateCache) {
    if f[0].is_empty() || f[1].is_empty() {
        return;
    }
    let Some(t) = ts::parse_c(f[3], tsc) else { return };
    if t < ctx.cws || t >= ctx.we {
        return;
    }
    let Some(camp) = ctx.campaigns.get(f[2]) else { return };
    let ukey = keys::key(f[1], b'u');
    let h = mix(ukey);
    let person = aliases.person(ukey, h);
    let ph = if person == ukey { h } else { mix(person) };
    out.push(part(ph, Q_BITS), PCl { person, ckey: keys::key(f[0], b'k'), ts: (t - ctx.cws) as u32, camp });
}

/// Every linked ID that is not its person's name -> the name. A bitmap in
/// front of the table answers most lookups (IDs that are names) from cache.
struct Aliases {
    keys: Vec<u64>,
    vals: Vec<u64>,
    mask: usize,
    filter: Vec<u64>,
    fmask: u64,
}

impl Aliases {
    fn from_pairs(pairs: &[(u64, u64)]) -> Self {
        let cap = (pairs.len() * 2).next_power_of_two().max(16);
        let fbits = (pairs.len() * 16).next_power_of_two().max(1 << 12);
        let mut m = Aliases {
            keys: vec![EMPTY; cap],
            vals: vec![0; cap],
            mask: cap - 1,
            filter: vec![0; fbits / 64],
            fmask: fbits as u64 - 1,
        };
        for &(k, v) in pairs {
            let h = mix(k);
            let f = h & m.fmask;
            m.filter[(f >> 6) as usize] |= 1 << (f & 63);
            let mut s = h as usize & m.mask;
            while m.keys[s] != EMPTY && m.keys[s] != k {
                s = (s + 1) & m.mask;
            }
            m.keys[s] = k;
            m.vals[s] = v;
        }
        m
    }
    /// `h` is mix(k).
    #[inline(always)]
    fn person(&self, k: u64, h: u64) -> u64 {
        let f = h & self.fmask;
        if self.filter[(f >> 6) as usize] & (1 << (f & 63)) == 0 {
            return k;
        }
        let mut s = h as usize & self.mask;
        loop {
            let x = self.keys[s];
            if x == k {
                return self.vals[s];
            }
            if x == EMPTY {
                return k;
            }
            s = (s + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        let f = h & self.fmask;
        prefetch(self.filter.as_ptr().wrapping_add((f >> 6) as usize));
        prefetch(self.keys.as_ptr().wrapping_add(h as usize & self.mask));
    }
}

/// Union-find over the links: the byte-wise smallest ID names each person.
///
/// Link lines are scanned with the SIMD scanner into key pairs; each ID gets a
/// slot in one open-addressing table (indexed by multiply-high, so its size
/// need not be a power of two) and union-find runs directly over slot numbers.
/// Lookups are prefetched a batch ahead, since the table is far bigger than
/// cache.
fn build_aliases(paths: &[PathBuf]) -> Aliases {
    let t_start = Instant::now();
    let maps = map_files(paths);
    let bytes: usize = maps.iter().map(|m| m.len()).sum();
    let mut pairs: Vec<(u64, u64)> = Vec::with_capacity(bytes / 18 + 16);
    for m in &maps {
        csv::scan::<2>(body(m), |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                pairs.push((keys::key(f[0], b'u'), keys::key(f[1], b'u')));
            }
        });
    }
    let t_parse = t_start.elapsed().as_secs_f64();

    // At most two new IDs per pair; ~2.5 slots per expected ID keeps probes short.
    let cap = (pairs.len() * 3).max(64);
    let mut slot_key = vec![EMPTY; cap];
    let mut parent = vec![NONE; cap];
    #[inline(always)]
    fn home(k: u64, cap: usize) -> usize {
        ((mix(k) as u128 * cap as u128) >> 64) as usize
    }
    #[inline(always)]
    fn slot_of(slot_key: &mut [u64], parent: &mut [u32], k: u64, cap: usize) -> u32 {
        let mut s = home(k, cap);
        loop {
            let x = slot_key[s];
            if x == k {
                return s as u32;
            }
            if x == EMPTY {
                slot_key[s] = k;
                parent[s] = s as u32;
                return s as u32;
            }
            s += 1;
            if s == cap {
                s = 0;
            }
        }
    }
    #[inline(always)]
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let gp = parent[parent[x as usize] as usize];
            parent[x as usize] = gp;
            x = gp;
        }
        x
    }
    const AHEAD: usize = 16;
    for i in 0..pairs.len() {
        if i + AHEAD < pairs.len() {
            let (a, b) = pairs[i + AHEAD];
            prefetch(slot_key.as_ptr().wrapping_add(home(a, cap)));
            prefetch(slot_key.as_ptr().wrapping_add(home(b, cap)));
        }
        let (a, b) = pairs[i];
        let sa = slot_of(&mut slot_key, &mut parent, a, cap);
        let sb = slot_of(&mut slot_key, &mut parent, b, cap);
        let ra = find(&mut parent, sa);
        let rb = find(&mut parent, sb);
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }
    let t_union = t_start.elapsed().as_secs_f64();

    // Smallest ID (byte order) per root, then every non-name ID -> its name.
    // First compress every slot straight to its root; the next passes read
    // roots in slot order, so their random accesses can be prefetched.
    for s in 0..cap {
        if slot_key[s] != EMPTY {
            let r = find(&mut parent, s as u32);
            parent[s] = r;
        }
    }
    let mut best = vec![EMPTY; cap];
    for s in 0..cap {
        if s + AHEAD < cap && parent[s + AHEAD] != NONE {
            prefetch(best.as_ptr().wrapping_add(parent[s + AHEAD] as usize));
        }
        let k = slot_key[s];
        if k == EMPTY {
            continue;
        }
        let r = parent[s] as usize;
        if best[r] == EMPTY || keys::cmp(k, best[r], b'u').is_lt() {
            best[r] = k;
        }
    }
    let mut alias_pairs = Vec::with_capacity(pairs.len());
    for s in 0..cap {
        if s + AHEAD < cap && parent[s + AHEAD] != NONE {
            prefetch(best.as_ptr().wrapping_add(parent[s + AHEAD] as usize));
        }
        let k = slot_key[s];
        if k == EMPTY {
            continue;
        }
        let name = best[parent[s] as usize];
        if name != k {
            alias_pairs.push((k, name));
        }
    }
    let t_best = t_start.elapsed().as_secs_f64();
    let a = Aliases::from_pairs(&alias_pairs);
    if std::env::var_os("SHOP_PROFILE").is_some() {
        eprintln!(
            "links: parse {:.3}s, union {:.3}s, names {:.3}s, table {:.3}s",
            t_parse,
            t_union - t_parse,
            t_best - t_union,
            t_start.elapsed().as_secs_f64() - t_best
        );
    }
    a
}
static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

/// Growable open-addressing u64 -> u32 index (dense ids).
struct SmallIndex {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
    len: usize,
}

impl SmallIndex {
    fn new(cap: usize) -> Self {
        let cap = cap.next_power_of_two().max(16);
        Self { keys: vec![EMPTY; cap], vals: vec![0; cap], mask: cap - 1, len: 0 }
    }
    fn reset(&mut self, expect: usize) {
        let cap = (expect * 2).next_power_of_two().max(16);
        if cap > self.keys.len() || cap * 4 < self.keys.len() {
            self.keys = vec![EMPTY; cap];
            self.vals = vec![0; cap];
            self.mask = cap - 1;
        } else {
            self.keys.fill(EMPTY);
        }
        self.len = 0;
    }
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64, next: u32) -> (u32, bool) {
        let mut s = mix(k) as usize & self.mask;
        loop {
            let x = self.keys[s];
            if x == k {
                return (self.vals[s], false);
            }
            if x == EMPTY {
                self.keys[s] = k;
                self.vals[s] = next;
                self.len += 1;
                if self.len * 2 > self.keys.len() {
                    self.grow();
                }
                return (next, true);
            }
            s = (s + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut s = mix(k) as usize & self.mask;
        loop {
            let x = self.keys[s];
            if x == k {
                return Some(self.vals[s]);
            }
            if x == EMPTY {
                return None;
            }
            s = (s + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let cap = self.keys.len() * 2;
        let old_k = std::mem::replace(&mut self.keys, vec![EMPTY; cap]);
        let old_v = std::mem::replace(&mut self.vals, vec![0; cap]);
        self.mask = cap - 1;
        for (k, v) in old_k.into_iter().zip(old_v) {
            if k != EMPTY {
                let mut s = mix(k) as usize & self.mask;
                while self.keys[s] != EMPTY {
                    s = (s + 1) & self.mask;
                }
                self.keys[s] = k;
                self.vals[s] = v;
            }
        }
    }
}

struct P1<'r> {
    idx: index::Index,
    tsc: ts::DateCache,
    prof: [f64; 2],
    ev: Sink<'r, Ev>,
    /// Clicks, already resolved to their person and partitioned by person.
    pc: Sink<'r, PCl>,
}

struct P2<'r> {
    pe: Sink<'r, PEv>,
    seen: Vec<u64>,
    coll: Vec<u64>,
    dups: SmallIndex,
    best: Vec<u64>,
    prof: [f64; 4],
}

#[derive(Default)]
struct UserRows {
    names: Vec<u8>,
    name_ends: Vec<u32>,
    days: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenue: Vec<i64>,
    top: Vec<u8>,
    top_ends: Vec<u32>,
    top_defs: Vec<i16>,
}

struct P3 {
    index: SmallIndex,
    persons: Vec<u64>,
    eidx: Vec<u32>,
    eoff: Vec<u32>,
    coff: Vec<u32>,
    evs: Vec<u64>,
    cls: Vec<C3>,
    catsum: Vec<u64>,
    touched: Vec<u32>,
    ccount: Vec<u32>,
    cpos: Vec<u32>,
    active: Vec<u32>,
    /// (campaigns + organic) x days x [sessions, purchasing, revenue, assisted]
    acc: Vec<[i64; 4]>,
    rows: UserRows,
    prof: [f64; 4],
}

impl P3 {
    #[inline(always)]
    fn add(&mut self, camp: u32) {
        let c = camp as usize;
        if self.ccount[c] == 0 {
            self.cpos[c] = self.active.len() as u32;
            self.active.push(camp);
        }
        self.ccount[c] += 1;
    }
    #[inline(always)]
    fn remove(&mut self, camp: u32) {
        let c = camp as usize;
        self.ccount[c] -= 1;
        if self.ccount[c] == 0 {
            let at = self.cpos[c] as usize;
            let last = self.active.pop().unwrap();
            if at < self.active.len() {
                self.active[at] = last;
                self.cpos[last as usize] = at as u32;
            }
        }
    }

    fn flush_day(&mut self, ctx: &Ctx, person: u64, day: i64, s: i64, e: i64, rev: i64) {
        let r = &mut self.rows;
        keys::bytes(person, b'u', &mut r.names);
        r.name_ends.push(r.names.len() as u32);
        r.days.push(day as i32);
        r.sessions.push(s as i32);
        r.events.push(e as i32);
        r.revenue.push(rev);
        if self.touched.is_empty() {
            r.top_defs.push(0);
        } else {
            let mut best = self.touched[0];
            for &c in &self.touched[1..] {
                let (sc, sb) = (self.catsum[c as usize], self.catsum[best as usize]);
                if sc > sb || (sc == sb && c < best) {
                    best = c;
                }
            }
            for &c in &self.touched {
                self.catsum[c as usize] = 0;
            }
            self.touched.clear();
            r.top.extend_from_slice(&ctx.cat_names[best as usize]);
            r.top_ends.push(r.top.len() as u32);
            r.top_defs.push(1);
        }
    }

    /// One person: events evs[e_lo..e_hi] sorted by ts, clicks cls[c_lo..c_hi]
    /// sorted by (ts, click_id). Event ts are relative to window_start, click
    /// ts to window_start - 7 days.
    fn person(&mut self, ctx: &Ctx, person: u64, e_lo: usize, e_hi: usize, c_lo: usize, c_hi: usize) {
        let organic = ctx.camp_ids.len();
        let nd = ctx.ndays;
        let (mut lo, mut hi) = (c_lo, c_lo);
        let mut cur_day = i64::MIN;
        let (mut ds, mut de, mut dr) = (0i64, 0i64, 0i64);
        let mut k = e_lo;
        while k < e_hi {
            let start = (self.evs[k] >> 32) as i64;
            let day = (ctx.ws + start).div_euclid(DAY);
            if day != cur_day {
                if cur_day != i64::MIN {
                    self.flush_day(ctx, person, cur_day, ds, de, dr);
                }
                cur_day = day;
                (ds, de, dr) = (0, 0, 0);
            }
            let mut last = start;
            let mut n = 0i64;
            let mut rev = 0i64;
            while k < e_hi {
                let w = self.evs[k];
                let t = (w >> 32) as i64;
                let price = (w >> 8) as u32 & 0xFF_FFFF;
                let cat = w as u32 & 0xFF;
                if t - last > GAP {
                    break;
                }
                last = t;
                n += 1;
                if price > 0 {
                    rev += price as i64;
                    let c = cat as usize;
                    if self.catsum[c] == 0 {
                        self.touched.push(cat);
                    }
                    self.catsum[c] += price as u64;
                }
                k += 1;
            }
            ds += 1;
            de += n;
            dr += rev;

            // Click times in the event frame: click.ts - REACH.
            let start_c = start + REACH;
            while hi < c_hi && self.cls[hi].ts as i64 <= start_c {
                let camp = self.cls[hi].camp;
                self.add(camp);
                hi += 1;
            }
            while lo < hi && (self.cls[lo].ts as i64) < start {
                let camp = self.cls[lo].camp;
                self.remove(camp);
                lo += 1;
            }
            let di = (day - ctx.day0) as usize;
            let credited = if hi > lo {
                let t = self.cls[hi - 1].ts;
                let mut m = hi - 1;
                while m > lo && self.cls[m - 1].ts == t {
                    m -= 1;
                }
                self.cls[m].camp as usize
            } else {
                organic
            };
            let a = &mut self.acc[credited * nd + di];
            a[0] += 1;
            a[1] += (rev > 0) as i64;
            a[2] += rev;
            for &camp in &self.active {
                self.acc[camp as usize * nd + di][3] += 1;
            }
        }
        if cur_day != i64::MIN {
            self.flush_day(ctx, person, cur_day, ds, de, dr);
        }
        for &camp in &self.active {
            self.ccount[camp as usize] = 0;
        }
        self.active.clear();
    }
}

/// Single-threaded, in-memory parse benchmark (no I/O): ns per line.
fn bench_parse(ctx: &Ctx, path: &str) {
    let mut data = std::fs::read(path).expect("bench file");
    data.truncate(200 << 20);
    let cut = data.iter().rposition(|&b| b == b'\n').unwrap() + 1;
    data.truncate(cut);
    let body = body(&data);
    let lines = memchr::memchr_iter(b'\n', body).count();
    for round in 0..6 {
        let t = Instant::now();
        let mut n = 0usize;
        csv::scan::<7>(body, |f| n += f[6].len());
        let scan = t.elapsed().as_secs_f64();
        let region = Region::new(body.len() + Sink::<Ev>::slack(P));
        let mut b = Sink::<Ev>::new(&region, P);
        let mut tsc = ts::DateCache::new();
        let t = Instant::now();
        csv::scan::<7>(body, |f| parse_event(f, ctx, &mut b, &mut tsc));
        let full = t.elapsed().as_secs_f64();
        let region2 = Region::new(body.len() + Sink::<Ev>::slack(P));
        let mut b2 = Sink::<Ev>::new(&region2, P);
        let mut idx = index::Index::new();
        let t = Instant::now();
        index::scan_indexed::<7>(body, &mut idx, |f| parse_event(f, ctx, &mut b2, &mut tsc));
        let full2 = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let mut nn = 0usize;
        index::scan_indexed::<7>(body, &mut idx, |f| nn += f[6].len());
        let scan2 = t.elapsed().as_secs_f64();
        eprintln!("   indexed: scan {:.1} ns/line  full {:.1} ns/line [{}]", scan2 * 1e9 / lines as f64, full2 * 1e9 / lines as f64, nn & 1);
        let t = Instant::now();
        let mut x = 0i64;
        let mut tsc2 = ts::DateCache::new();
        csv::scan::<7>(body, |f| x ^= ts::parse_c(f[2], &mut tsc2).unwrap_or(0) ^ ts::parse_c(f[3], &mut tsc2).unwrap_or(0));
        let tsp = t.elapsed().as_secs_f64() - scan;
        let t = Instant::now();
        let mut y = 0u64;
        csv::scan::<7>(body, |f| y ^= keys::key(f[0], b'e') ^ keys::key(f[1], b'u'));
        let kp = t.elapsed().as_secs_f64() - scan;
        let ns = |s: f64| s * 1e9 / lines as f64;
        eprintln!(
            "round {round}: {lines} lines  scan {:.1} ns/line  full {:.1}  (2 ts {:.1}, 2 keys {:.1})  [{} {} {}]",
            ns(scan), ns(full), ns(tsp), ns(kp), n & 1, x & 1, y & 1
        );
    }
}

/// Alias-lookup benchmark on real user IDs (single thread, in memory).
fn bench_alias(ctx: &Ctx, data: &Path) {
    let a = build_aliases(&list_csv(&data.join("links")));
    let maps = map_files(&list_csv(&data.join("events")));
    let mut ukeys: Vec<u64> = Vec::new();
    for m in maps.iter().take(8) {
        csv::scan::<7>(body(m), |f| {
            if !f[1].is_empty() {
                ukeys.push(keys::key(f[1], b'u'));
            }
        });
    }
    let _ = ctx;
    let n = ukeys.len();
    for round in 0..3 {
        let t = Instant::now();
        let mut x = 0u64;
        for &u in &ukeys {
            x ^= a.person(u, mix(u));
        }
        let plain = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let mut y = 0u64;
        for i in 0..n {
            if i + 16 < n {
                a.prefetch(mix(ukeys[i + 16]));
            }
            let u = ukeys[i];
            y ^= a.person(u, mix(u));
        }
        let pf = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let mut z = 0u64;
        for &u in &ukeys {
            z ^= mix(u);
        }
        let base = t.elapsed().as_secs_f64();
        eprintln!(
            "round {round}: {n} lookups  plain {:.1} ns  prefetch16 {:.1} ns  mix-only {:.1} ns [{}]",
            plain * 1e9 / n as f64, pf * 1e9 / n as f64, base * 1e9 / n as f64, (x ^ y ^ z) & 1
        );
    }
}

fn write_user(path: &Path, r: UserRows) {
    out::write(
        path,
        out::USER_SCHEMA,
        vec![
            out::Col::Str(r.names, r.name_ends),
            out::Col::I32(r.days),
            out::Col::I32(r.sessions),
            out::Col::I32(r.events),
            out::Col::I64(r.revenue),
            out::Col::OptStr(r.top, r.top_ends, r.top_defs),
        ],
    )
    .expect("write user_daily");
}

fn main() {
    let t0 = Instant::now();
    let _ = T0.set(t0);
    let profile = std::env::var_os("SHOP_PROFILE").is_some();
    let lap = |what: &str| {
        if profile {
            eprintln!("{:>8.3}s {what}", t0.elapsed().as_secs_f64());
        }
    };
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let outdir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| thread::available_parallelism().map_or(8, usize::from));

    let ctx = load_ctx(&data);
    if std::env::var_os("SHOP_BENCH_ALIAS").is_some() {
        bench_alias(&ctx, &data);
        return;
    }
    if let Ok(path) = std::env::var("SHOP_BENCH_PARSE") {
        bench_parse(&ctx, &path);
        return;
    }
    let event_files = map_files(&list_csv(&data.join("events")));
    let click_files = map_files(&list_csv(&data.join("clicks")));
    let link_paths = list_csv(&data.join("links"));
    let event_chunks = chunks(&event_files);
    let click_chunks = chunks(&click_files);
    let n_event_chunks = event_chunks.len();
    let event_bytes: usize = event_files.iter().map(|m| m.len()).sum();
    let click_bytes: usize = click_files.iter().map(|m| m.len()).sum();
    let ev_region = Region::new(event_bytes / 32 * std::mem::size_of::<Ev>() + Sink::<Ev>::slack(P) * threads);
    let pe_region = Region::new(event_bytes / 32 * std::mem::size_of::<PEv>() + Sink::<PEv>::slack(Q) * threads);
    let pc_region = Region::new(click_bytes / 24 * std::mem::size_of::<PCl>() + Sink::<PCl>::slack(Q) * threads);
    lap("setup");

    // Phase 1: parse.
    let aliases_cell: std::sync::OnceLock<Aliases> = std::sync::OnceLock::new();
    let p1 = thread::scope(|scope| {
        #[cfg(unix)]
        {
            let all: Vec<&Mmap> = event_files.iter().chain(click_files.iter()).collect();
            scope.spawn(move || readahead_all(&all));
        }
        scope.spawn(|| {
            let t = Instant::now();
            let a = build_aliases(&link_paths);
            if profile {
                eprintln!("links built in {:.3}s", t.elapsed().as_secs_f64());
            }
            let _ = aliases_cell.set(a);
        });
        let p1 = pool_finish(
            threads,
            n_event_chunks + click_chunks.len(),
            || P1 {
                idx: index::Index::new(),
                tsc: ts::DateCache::new(),
                prof: [0.0; 2],
                ev: Sink::new(&ev_region, P),
                pc: Sink::new(&pc_region, Q),
            },
            |st, t| {
                let tt = Instant::now();
                if t < n_event_chunks {
                    let (fi, a, b) = event_chunks[t];
                    let slice = &event_files[fi][a..b];
                    let (idx, ev, tsc) = (&mut st.idx, &mut st.ev, &mut st.tsc);
                    index::scan_indexed::<7>(slice, idx, |f| parse_event(f, &ctx, ev, tsc));
                } else {
                    let (fi, a, b) = click_chunks[t - n_event_chunks];
                    let slice = &click_files[fi][a..b];
                    let aliases = aliases_cell.wait();
                    let (idx, pc, tsc) = (&mut st.idx, &mut st.pc, &mut st.tsc);
                    index::scan_indexed::<4>(slice, idx, |f| parse_click(f, &ctx, aliases, pc, tsc));
                }
                st.prof[(t >= n_event_chunks) as usize] += tt.elapsed().as_secs_f64();
            },
            |st, _| {
                st.ev.finish();
                st.pc.finish();
            },
        );
        p1
    });
    let aliases = aliases_cell.into_inner().expect("links");
    if profile {
        let ev: f64 = p1.iter().map(|w| w.prof[0]).sum();
        let cl: f64 = p1.iter().map(|w| w.prof[1]).sum();
        eprintln!("parse cpu: events {ev:.3} clicks {cl:.3}");
    }
    lap("parse");

    // Phase 2: dedup events, route events and clicks by person.
    let p2 = pool_finish(
        threads,
        P,
        || P2 {
            pe: Sink::new(&pe_region, Q),
            seen: Vec::new(),
            coll: Vec::new(),
            dups: SmallIndex::new(1024),
            best: Vec::new(),
            prof: [0.0; 4],
        },
        |st, t| {
            let tt = Instant::now();
            let n: usize = p1.iter().map(|w| w.ev.len(t)).sum();
            if n == 0 {
                return;
            }
            let bits = (n * 16).next_power_of_two().max(64);
            let fmask = bits as u64 - 1;
            st.seen.clear();
            st.seen.resize(bits / 64, 0);
            st.coll.clear();
            st.coll.resize(bits / 64, 0);
            for w in &p1 {
                for e in w.ev.slices(t).flatten() {
                    let f = mix(e.ekey) & fmask;
                    let (word, bit) = ((f >> 6) as usize, 1u64 << (f & 63));
                    st.coll[word] |= st.seen[word] & bit;
                    st.seen[word] |= bit;
                }
            }
            st.prof[0] += tt.elapsed().as_secs_f64();
            let tt = Instant::now();
            st.dups.reset(n / 8);
            st.best.clear();
            let emit = |st_pe: &mut Sink<PEv>, e: &Ev| {
                let price = e.ip & PRICE_MASK;
                if price == PRICE_INVALID {
                    return;
                }
                let h = mix(e.ukey);
                let person = aliases.person(e.ukey, h);
                st_pe.push(part(if person == e.ukey { h } else { mix(person) }, Q_BITS), PEv {
                    person,
                    w: (e.ts as u64) << 32 | price << 8 | e.cat as u64,
                });
            };
            let mut dup_refs: Vec<(u32, u32)> = Vec::new();
            for (wi, w) in p1.iter().enumerate() {
                for (j, e) in w.ev.slices(t).flatten().enumerate() {
                    let f = mix(e.ekey) & fmask;
                    if st.coll[(f >> 6) as usize] & (1u64 << (f & 63)) == 0 {
                        emit(&mut st.pe, e);
                    } else {
                        let (slot, new) = st.dups.get_or_insert(e.ekey, dup_refs.len() as u32);
                        if new {
                            dup_refs.push((wi as u32, j as u32));
                        } else {
                            let (bw, bj) = dup_refs[slot as usize];
                            if e.ip < p1[bw as usize].ev.get(t, bj as usize).ip {
                                dup_refs[slot as usize] = (wi as u32, j as u32);
                            }
                        }
                    }
                }
            }
            for &(wi, j) in &dup_refs {
                emit(&mut st.pe, p1[wi as usize].ev.get(t, j as usize));
            }
            st.prof[1] += tt.elapsed().as_secs_f64();
        },
        |st, _| st.pe.finish(),
    );
    if profile {
        let mut tot = [0.0; 4];
        for w in &p2 {
            for i in 0..4 {
                tot[i] += w.prof[i];
            }
        }
        eprintln!("dedup cpu: bitmap {:.3} emit {:.3}", tot[0], tot[1]);
    }
    // p1 stays alive: its click sinks feed phase 3. Nothing is freed until exit.
    lap("dedup");

    // Phase 3: group by person, sessions, attribution, aggregation. Each
    // worker writes its own user_daily file as soon as it runs out of work.
    let user_dir = outdir.join("user_daily");
    let camp_dir = outdir.join("campaign_daily");
    std::fs::create_dir_all(&user_dir).expect("create user_daily");
    std::fs::create_dir_all(&camp_dir).expect("create campaign_daily");
    let wrote_user = std::sync::atomic::AtomicBool::new(false);
    let ncamp = ctx.camp_ids.len();
    // Largest person partitions first, so no thread is left with a big tail.
    let mut order: Vec<usize> = (0..Q).collect();
    order.sort_by_key(|&q| {
        std::cmp::Reverse(p2.iter().map(|w| w.pe.len(q)).sum::<usize>() + p1.iter().map(|w| w.pc.len(q)).sum::<usize>())
    });
    let p3 = pool_finish(
        threads,
        Q,
        || P3 {
            index: SmallIndex::new(1024),
            persons: Vec::new(),
            eidx: Vec::new(),
            eoff: Vec::new(),
            coff: Vec::new(),
            evs: Vec::new(),
            cls: Vec::new(),
            catsum: vec![0; ctx.cat_names.len()],
            touched: Vec::new(),
            ccount: vec![0; ncamp],
            cpos: vec![0; ncamp],
            active: Vec::new(),
            acc: vec![[0; 4]; (ncamp + 1) * ctx.ndays],
            rows: UserRows::default(),
            prof: [0.0; 4],
        },
        |st, t| {
            let q = order[t];
            let tt = Instant::now();
            let ne: usize = p2.iter().map(|w| w.pe.len(q)).sum();
            if ne == 0 {
                return;
            }
            // Dense person ids and counts.
            st.index.reset(ne / 20);
            st.persons.clear();
            st.eidx.clear();
            st.eoff.clear();
            for w in &p2 {
                for e in w.pe.slices(q).flatten() {
                    let (i, new) = st.index.get_or_insert(e.person, st.persons.len() as u32);
                    if new {
                        st.persons.push(e.person);
                        st.eoff.push(0);
                    }
                    st.eoff[i as usize] += 1;
                    st.eidx.push(i);
                }
            }
            let np = st.persons.len();
            // Exclusive prefix sums, then scatter (eoff ends as each start).
            let mut run = 0u32;
            for c in st.eoff.iter_mut() {
                let x = *c;
                *c = run;
                run += x;
            }
            st.eoff.push(run);
            st.evs.clear();
            st.evs.resize(ne, 0);
            {
                let mut cursor: Vec<u32> = st.eoff[..np].to_vec();
                let mut k = 0;
                for w in &p2 {
                    for e in w.pe.slices(q).flatten() {
                        let i = st.eidx[k] as usize;
                        k += 1;
                        st.evs[cursor[i] as usize] = e.w;
                        cursor[i] += 1;
                    }
                }
            }
            // Clicks of persons that have events.
            st.coff.clear();
            st.coff.resize(np + 1, 0);
            st.eidx.clear();
            let mut nc = 0usize;
            for w in &p1 {
                for c in w.pc.slices(q).flatten() {
                    match st.index.get(c.person) {
                        Some(i) => {
                            st.coff[i as usize] += 1;
                            st.eidx.push(i);
                            nc += 1;
                        }
                        None => st.eidx.push(NONE),
                    }
                }
            }
            let mut run = 0u32;
            for c in st.coff[..np].iter_mut() {
                let x = *c;
                *c = run;
                run += x;
            }
            st.coff[np] = run;
            st.cls.clear();
            st.cls.resize(nc, C3::default());
            {
                let mut cursor: Vec<u32> = st.coff[..np].to_vec();
                let mut k = 0;
                for w in &p1 {
                    for c in w.pc.slices(q).flatten() {
                        let i = st.eidx[k];
                        k += 1;
                        if i != NONE {
                            let i = i as usize;
                            st.cls[cursor[i] as usize] = C3 { ckey: c.ckey, ts: c.ts, camp: c.camp };
                            cursor[i] += 1;
                        }
                    }
                }
            }
            st.prof[0] += tt.elapsed().as_secs_f64();
            let tt = Instant::now();
            for p in 0..np {
                let (e_lo, e_hi) = (st.eoff[p] as usize, st.eoff[p + 1] as usize);
                let (c_lo, c_hi) = (st.coff[p] as usize, st.coff[p + 1] as usize);
                st.evs[e_lo..e_hi].sort_unstable();
                if c_hi - c_lo > 1 {
                    st.cls[c_lo..c_hi].sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| keys::cmp(a.ckey, b.ckey, b'k')));
                }
                let person = st.persons[p];
                st.person(&ctx, person, e_lo, e_hi, c_lo, c_hi);
            }
            st.prof[1] += tt.elapsed().as_secs_f64();
        },
        |st, i| {
            if !st.rows.days.is_empty() {
                let r = std::mem::take(&mut st.rows);
                write_user(&user_dir.join(format!("part-{i:03}.parquet")), r);
                wrote_user.store(true, AtomicOrdering::Relaxed);
            }
        },
    );
    if profile {
        let mut tot = [0.0; 4];
        for w in &p3 {
            for i in 0..4 {
                tot[i] += w.prof[i];
            }
        }
        eprintln!("session cpu: group {:.3} sweep {:.3}", tot[0], tot[1]);
    }
    std::mem::forget(p2); // freed at exit
    std::mem::forget(p1);
    lap("sessions");

    // Output: campaign_daily (user_daily files are already written).
    let nd = ctx.ndays;
    let mut acc = vec![[0i64; 4]; (ncamp + 1) * nd];
    for st in &p3 {
        for (a, b) in acc.iter_mut().zip(&st.acc) {
            for x in 0..4 {
                a[x] += b[x];
            }
        }
    }
    if !wrote_user.load(AtomicOrdering::Relaxed) {
        write_user(&user_dir.join("part-000.parquet"), UserRows::default());
    }
    {
        let (mut ids, mut id_ends, mut chans, mut chan_ends) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut days, mut s, mut ps, mut rev, mut asst) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for c in 0..=ncamp {
            for d in 0..nd {
                let a = acc[c * nd + d];
                if a[0] == 0 && a[3] == 0 {
                    continue;
                }
                let (id, ch): (&[u8], &[u8]) = if c == ncamp {
                    (b"organic", b"organic")
                } else {
                    (&ctx.camp_ids[c], &ctx.channels[c])
                };
                ids.extend_from_slice(id);
                id_ends.push(ids.len() as u32);
                chans.extend_from_slice(ch);
                chan_ends.push(chans.len() as u32);
                days.push((ctx.day0 + d as i64) as i32);
                s.push(a[0]);
                ps.push(a[1]);
                rev.push(a[2]);
                asst.push(a[3]);
            }
        }
        out::write(
            &camp_dir.join("part-000.parquet"),
            out::CAMPAIGN_SCHEMA,
            vec![
                out::Col::Str(ids, id_ends),
                out::Col::Str(chans, chan_ends),
                out::Col::I32(days),
                out::Col::I64(s),
                out::Col::I64(ps),
                out::Col::I64(rev),
                out::Col::I64(asst),
            ],
        )
        .expect("write campaign_daily");
    }
    lap("write");
    // Skip freeing everything: the OS reclaims it at exit.
    std::process::exit(0);
}
