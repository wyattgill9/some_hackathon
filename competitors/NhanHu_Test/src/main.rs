//! Shopping Sessions pipeline. Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.

mod alloc;
mod csv;
mod keys;
mod parquet;
mod table;
mod ts;

use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use keys::{fib, fib2, key_cmp, key_lookup_padded, key_of, key_of_padded};
use table::{IdAlloc, UserTable};
use ts::{parse_ts, TsParser, DAY};

#[global_allocator]
static GLOBAL: alloc::HugeAlloc = alloc::HugeAlloc;

const GAP: u32 = 1_800_000;
const WEEK: u32 = 604_800_000;
const LATE: i64 = DAY;
const INVALID: u32 = u32::MAX;
const CHUNK: usize = 4 << 20;
const BATCH: usize = 16;

macro_rules! log {
    ($($t:tt)*) => { if std::env::var_os("QUIET").is_none() { eprintln!($($t)*); } }
}

// ---------------------------------------------------------------- small fixed maps

struct SmallMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
    bits: u32,
}

impl SmallMap {
    fn new(items: &[(u64, u32)]) -> Self {
        let cap = (items.len() * 2).next_power_of_two().max(16);
        let mut m = SmallMap { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1, bits: cap.trailing_zeros() };
        for &(k, v) in items {
            let mut h = fib(k, m.bits);
            while m.keys[h] != 0 && m.keys[h] != k {
                h = (h + 1) & m.mask;
            }
            m.keys[h] = k;
            m.vals[h] = v;
        }
        m
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut h = fib(k, self.bits);
        loop {
            let kk = self.keys[h];
            if kk == k {
                return Some(self.vals[h]);
            }
            if kk == 0 {
                return None;
            }
            h = (h + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------- context

struct Ctx {
    ws: i64,
    we: i64,
    /// time base for u32 offsets: window_start - 7 days
    base: i64,
    base_s: i64,
    first_day: i64,
    n_days: usize,
    cat_names: Vec<Vec<u8>>,
    unknown_cat: u32,
    products: SmallMap,
    camps: SmallMap,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
}

impl Ctx {
    /// `s` must be followed by 16 readable bytes.
    #[inline(always)]
    fn product_cat(&self, s: &[u8]) -> u32 {
        match unsafe { key_lookup_padded(s) } {
            Some(k) => self.products.get(k).unwrap_or(self.unknown_cat),
            None => self.unknown_cat,
        }
    }
    /// `s` must be followed by 16 readable bytes.
    #[inline(always)]
    fn campaign(&self, s: &[u8]) -> Option<u32> {
        unsafe { key_lookup_padded(s) }.and_then(|k| self.camps.get(k))
    }
}

/// (first, second) field spans of every well-formed two-field data line.
fn read_small_csv(data: &[u8]) -> Vec<((usize, usize), (usize, usize))> {
    let mut out = Vec::new();
    let base = data.as_ptr() as usize;
    csv::for_each_line(data, |line| {
        if let Some(f) = csv::split_scalar::<2>(line) {
            let o = line.as_ptr() as usize - base;
            if f[0].1 > f[0].0 {
                out.push(((o + f[0].0 as usize, o + f[0].1 as usize), (o + f[1].0 as usize, o + f[1].1 as usize)));
            }
        }
    });
    out
}

fn manifest_value(text: &str, name: &str) -> i64 {
    let at = text.find(&format!("\"{name}\"")).unwrap_or_else(|| panic!("manifest has no {name}"));
    let rest = &text[at + name.len() + 2..];
    let colon = rest.find(':').unwrap();
    let rest = &rest[colon + 1..];
    let q1 = rest.find('"').unwrap();
    let q2 = rest[q1 + 1..].find('"').unwrap();
    let v = &rest[q1 + 1..q1 + 1 + q2];
    parse_ts(v.as_bytes()).unwrap_or_else(|| panic!("bad manifest time {v}"))
}

fn build_ctx(data: &Path) -> Ctx {
    let man = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_value(&man, "window_start");
    let we = manifest_value(&man, "window_end");
    let base = ws - 7 * DAY;
    assert!(we > ws && we - base < u32::MAX as i64, "window too long");
    let first_day = ws.div_euclid(DAY);
    let n_days = ((we - 1).div_euclid(DAY) - first_day + 1) as usize;

    let pdata = std::fs::read(data.join("products.csv")).unwrap_or_default();
    let prows = read_small_csv(&pdata);
    // categories, numbered in byte order (with "unknown") so ties go to the smaller id
    let mut cat_of: std::collections::HashMap<&[u8], u32> = std::collections::HashMap::new();
    for &(_, (a, b)) in &prows {
        cat_of.entry(&pdata[a..b]).or_insert(0);
    }
    cat_of.entry(b"unknown").or_insert(0);
    let mut cats: Vec<Vec<u8>> = cat_of.keys().map(|c| c.to_vec()).collect();
    cats.sort();
    for (i, c) in cats.iter().enumerate() {
        *cat_of.get_mut(c.as_slice()).unwrap() = i as u32;
    }
    let unknown_cat = cat_of[&b"unknown"[..]];
    let pitems: Vec<(u64, u32)> = prows.iter().map(|&((a, b), (c, d))| (key_of(&pdata[a..b]), cat_of[&pdata[c..d]])).collect();
    let cdata = std::fs::read(data.join("campaigns.csv")).unwrap_or_default();
    let crows = read_small_csv(&cdata);
    let mut camp_ids = Vec::new();
    let mut camp_channels = Vec::new();
    let mut citems = Vec::new();
    for &((a, b), (c, d)) in &crows {
        citems.push((key_of(&cdata[a..b]), camp_ids.len() as u32));
        camp_ids.push(cdata[a..b].to_vec());
        camp_channels.push(cdata[c..d].to_vec());
    }
    Ctx {
        ws,
        we,
        base,
        base_s: ws.div_euclid(1000) - (1i64 << 31),
        first_day,
        n_days,
        cat_names: cats,
        unknown_cat,
        products: SmallMap::new(&pitems),
        camps: SmallMap::new(&citems),
        camp_ids,
        camp_channels,
    }
}

// ---------------------------------------------------------------- records

pub trait ERec: Copy + Send + Sync + 'static {
    fn make(key: u64, ingest: u32, user: u32, ts: u32, price: u32, cat: u32) -> Self;
    fn key(&self) -> u64;
    fn ingest(&self) -> u32;
    fn user(&self) -> u32;
    fn ts(&self) -> u32;
    fn price(&self) -> u32;
    fn cat(&self) -> u32;
}

#[derive(Clone, Copy)]
#[repr(C)]
struct ENarrow {
    key: u64,
    ingest: u32,
    user: u32,
    ts: u32,
    pay: u32,
}

impl ERec for ENarrow {
    #[inline(always)]
    fn make(key: u64, ingest: u32, user: u32, ts: u32, price: u32, cat: u32) -> Self {
        ENarrow { key, ingest, user, ts, pay: price | (cat << 24) }
    }
    #[inline(always)]
    fn key(&self) -> u64 {
        self.key
    }
    #[inline(always)]
    fn ingest(&self) -> u32 {
        self.ingest
    }
    #[inline(always)]
    fn user(&self) -> u32 {
        self.user
    }
    #[inline(always)]
    fn ts(&self) -> u32 {
        self.ts
    }
    #[inline(always)]
    fn price(&self) -> u32 {
        self.pay & 0xFF_FFFF
    }
    #[inline(always)]
    fn cat(&self) -> u32 {
        self.pay >> 24
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EWide {
    key: u64,
    ingest: u32,
    user: u32,
    ts: u32,
    price: u32,
    cat: u32,
    _p: u32,
}

impl ERec for EWide {
    #[inline(always)]
    fn make(key: u64, ingest: u32, user: u32, ts: u32, price: u32, cat: u32) -> Self {
        EWide { key, ingest, user, ts, price, cat, _p: 0 }
    }
    #[inline(always)]
    fn key(&self) -> u64 {
        self.key
    }
    #[inline(always)]
    fn ingest(&self) -> u32 {
        self.ingest
    }
    #[inline(always)]
    fn user(&self) -> u32 {
        self.user
    }
    #[inline(always)]
    fn ts(&self) -> u32 {
        self.ts
    }
    #[inline(always)]
    fn price(&self) -> u32 {
        self.price
    }
    #[inline(always)]
    fn cat(&self) -> u32 {
        self.cat
    }
}

/// A valid click after parsing.
#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    ckey: u64,
    user: u32,
    ts: u32,
    camp: u32,
    _p: u32,
}

/// A surviving event in a person partition: k = local person << 32 | ts.
#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    k: u64,
    price: u32,
    cat: u32,
}

/// A click in a person partition.
#[derive(Clone, Copy)]
#[repr(C)]
struct PClick {
    k: u64,
    ckey: u64,
    camp: u32,
    _p: u32,
}

#[derive(Clone, Copy)]
struct Ev {
    ts: u32,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy)]
struct Cl {
    ts: u32,
    camp: u32,
    ckey: u64,
}

// ---------------------------------------------------------------- memory

mod sys {
    extern "C" {
        pub fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
        pub fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    }
    pub const PROT_READ: i32 = 1;
    pub const MAP_PRIVATE: i32 = 2;
    pub const MAP_FIXED: i32 = 0x10;
    pub const MAP_ANONYMOUS: i32 = 0x20;
    pub const MAP_NORESERVE: i32 = 0x4000;
    pub const MADV_SEQUENTIAL: i32 = 2;
}

fn huge_alloc(bytes: usize) -> (*mut u8, usize) {
    (table::zeroed_bytes(bytes), bytes)
}

/// Bump allocator for fixed-size blocks, shared by threads; memory is touched only as used.
struct Pool {
    base: *mut u8,
    size: usize,
    next: AtomicUsize,
}

unsafe impl Sync for Pool {}
unsafe impl Send for Pool {}

impl Pool {
    fn new(bytes: usize) -> Pool {
        let (base, size) = huge_alloc(bytes);
        Pool { base, size, next: AtomicUsize::new(0) }
    }
    fn alloc(&self, bytes: usize) -> *mut u8 {
        let off = self.next.fetch_add(bytes, Ordering::Relaxed);
        if off + bytes <= self.size {
            unsafe { self.base.add(off) }
        } else {
            huge_alloc(bytes).0
        }
    }
}

/// Not a power of two, so the current blocks of different partitions do not alias in the caches.
const BLOCK_BYTES: usize = (16 << 10) + 3 * 64;

#[derive(Clone, Copy)]
#[repr(C)]
struct PartHead<T> {
    cur: *mut T,
    fill: u32,
    _pad: u32,
}

/// Per-partition append buffers made of blocks from a shared pool.
struct PartBuf<'p, T: Copy> {
    pool: &'p Pool,
    per_block: u32,
    heads: Vec<PartHead<T>>,
    blocks: Vec<Vec<*mut T>>,
    /// recycled blocks to use before asking the pool
    spare: Vec<*mut u8>,
}

unsafe impl<T: Copy + Send> Send for PartBuf<'_, T> {}
unsafe impl<T: Copy + Sync> Sync for PartBuf<'_, T> {}

impl<'p, T: Copy> PartBuf<'p, T> {
    fn new(pool: &'p Pool, n_parts: usize) -> Self {
        let per_block = (BLOCK_BYTES / std::mem::size_of::<T>()) as u32;
        PartBuf {
            pool,
            per_block,
            heads: vec![PartHead { cur: std::ptr::null_mut(), fill: per_block, _pad: 0 }; n_parts],
            blocks: (0..n_parts).map(|_| Vec::new()).collect(),
            spare: Vec::new(),
        }
    }
    #[inline(always)]
    fn push(&mut self, p: usize, x: T) {
        unsafe {
            let h = self.heads.get_unchecked_mut(p);
            if h.fill == self.per_block {
                self.new_block(p);
            }
            let h = self.heads.get_unchecked_mut(p);
            h.cur.add(h.fill as usize).write(x);
            h.fill += 1;
        }
    }
    #[cold]
    #[inline(never)]
    fn new_block(&mut self, p: usize) {
        let b = match self.spare.pop() {
            Some(b) => b as *mut T,
            None => self.pool.alloc(BLOCK_BYTES) as *mut T,
        };
        self.blocks[p].push(b);
        let h = unsafe { self.heads.get_unchecked_mut(p) };
        h.cur = b;
        h.fill = 0;
    }
    /// The partition's records, block by block.
    fn chunks(&self, p: usize) -> impl Iterator<Item = &[T]> + '_ {
        let nb = self.blocks[p].len();
        let per = self.per_block as usize;
        let last = self.heads[p].fill as usize;
        self.blocks[p]
            .iter()
            .enumerate()
            .map(move |(i, &b)| unsafe { std::slice::from_raw_parts(b as *const T, if i + 1 == nb { last } else { per }) })
    }
    /// The raw blocks of partition p (for recycling once their records are consumed).
    fn block_ptrs(&self, p: usize) -> impl Iterator<Item = *mut u8> + '_ {
        self.blocks[p].iter().map(|&b| b as *mut u8)
    }
    fn len(&self, p: usize) -> usize {
        let nb = self.blocks[p].len();
        if nb == 0 {
            0
        } else {
            (nb - 1) * self.per_block as usize + self.heads[p].fill as usize
        }
    }
}

// ---------------------------------------------------------------- inputs

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Links,
    Events,
    Clicks,
}

struct InFile {
    ptr: *const u8,
    size: usize,
    kind: Kind,
}

unsafe impl Sync for InFile {}
unsafe impl Send for InFile {}

impl InFile {
    /// Maps the file read-only, followed by at least one page of zeros.
    fn open(path: &Path, kind: Kind) -> InFile {
        let file = File::open(path).expect("open");
        let size = file.metadata().map(|m| m.len()).unwrap_or(0) as usize;
        if size == 0 {
            return InFile { ptr: std::ptr::null(), size: 0, kind };
        }
        let page = 4096usize;
        let len = size.div_ceil(page) * page + page;
        unsafe {
            let reserve = sys::mmap(
                std::ptr::null_mut(),
                len,
                sys::PROT_READ,
                sys::MAP_PRIVATE | sys::MAP_ANONYMOUS | sys::MAP_NORESERVE,
                -1,
                0,
            );
            assert!(reserve as isize != -1, "mmap reserve failed");
            let p = sys::mmap(reserve, size, sys::PROT_READ, sys::MAP_PRIVATE | sys::MAP_FIXED, file.as_raw_fd(), 0);
            assert!(p == reserve, "mmap file failed");
            sys::madvise(p, size, sys::MADV_SEQUENTIAL);
            InFile { ptr: p, size, kind }
        }
    }
}

struct Chunk {
    file: usize,
    s: usize,
    e: usize,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map_or(false, |x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

struct T1Out<'p, R: Copy> {
    ev: PartBuf<'p, R>,
    clicks: Vec<CRec>,
    links: Vec<(u32, u32)>,
    lines: [u64; 3],
}

#[derive(Clone, Copy)]
struct UserCache {
    keys: [u64; 512],
    ids: [u32; 512],
}

impl UserCache {
    fn new() -> Self {
        UserCache { keys: [0; 512], ids: [0; 512] }
    }
}

#[derive(Clone, Copy, Default)]
struct PendEv {
    key: u64,
    ukey: u64,
    uh: usize,
    user: u32,
    ing: u32,
    ts: u32,
    price: u32,
    cat: u32,
    valid: bool,
}

#[derive(Clone, Copy, Default)]
struct PendCl {
    ckey: u64,
    ukey: u64,
    uh: usize,
    user: u32,
    ts: u32,
    camp: u32,
}

struct Parser<'a> {
    ctx: &'a Ctx,
    table: &'a UserTable,
    alloc: IdAlloc,
    cache: UserCache,
    pshift: u32,
    tsp: TsParser,
    evb: [PendEv; BATCH],
    nev: usize,
    clb: [PendCl; BATCH],
    ncl: usize,
    lkb: [(u64, u64, usize, usize); BATCH],
    nlk: usize,
}

impl<'a> Parser<'a> {
    #[inline(always)]
    fn event<R: ERec>(&mut self, line: &[u8], f: &csv::Spans<7>, out: &mut T1Out<R>) {
        let fld = |i: usize| unsafe { line.get_unchecked(f[i].0 as usize..f[i].1 as usize) };
        let eid = fld(0);
        if eid.is_empty() {
            return;
        }
        let key = unsafe { key_of_padded(eid) };
        let ctx = self.ctx;
        let ing_opt = self.tsp.parse_f1_seconds(fld(3));
        let (ing_ms, ing_s) = ing_opt.unwrap_or((i64::MAX, 0));
        let mut valid = ing_opt.is_some();
        let ing = if valid {
            (ing_s - ctx.base_s).clamp(0, u32::MAX as i64 - 1) as u32
        } else {
            u32::MAX
        };
        let user_s = fld(1);
        let prod = fld(5);
        valid &= !user_s.is_empty() && !prod.is_empty();
        let mut ts = 0u32;
        let mut price = 0u32;
        let mut cat = 0u32;
        if valid {
            match unsafe { self.tsp.parse_padded(fld(2)) } {
                Some(t) if t >= ctx.ws && t < ctx.we && ing_ms - t <= LATE => {
                    ts = (t - ctx.base) as u32;
                }
                _ => valid = false,
            }
        }
        if valid {
            let et = fld(4);
            if et.len() == 8 && et == b"purchase" {
                let p = fld(6);
                let mut v: u64 = 0;
                let mut ok = !p.is_empty();
                for &c in p {
                    let d = c.wrapping_sub(b'0');
                    if d > 9 {
                        ok = false;
                        break;
                    }
                    v = (v * 10 + d as u64).min(100_000_000);
                }
                if ok && (1..=10_000_000).contains(&v) {
                    price = v as u32;
                    cat = ctx.product_cat(prod);
                } else {
                    valid = false;
                }
            } else if !(et.len() == 4 && (et == b"view" || et == b"cart")) {
                valid = false;
            }
        }
        let mut pe = PendEv { key, ukey: 0, uh: 0, user: INVALID, ing, ts, price, cat, valid };
        if valid {
            let ukey = unsafe { key_of_padded(user_s) };
            pe.ukey = ukey;
            let idx = (ukey ^ (ukey >> 9)) as usize & 511;
            if self.cache.keys[idx] == ukey {
                pe.user = self.cache.ids[idx];
            } else {
                let uh = self.table.hash(ukey);
                pe.uh = uh;
                self.table.prefetch(uh);
            }
        }
        self.evb[self.nev] = pe;
        self.nev += 1;
        if self.nev == BATCH {
            self.flush_events(out);
        }
    }

    #[inline(always)]
    fn flush_events<R: ERec>(&mut self, out: &mut T1Out<R>) {
        for i in 0..self.nev {
            let pe = self.evb[i];
            let user = if pe.valid {
                if pe.user != INVALID {
                    pe.user
                } else {
                    let id = self.table.get_or_insert(pe.ukey, pe.uh, &mut self.alloc);
                    let idx = (pe.ukey ^ (pe.ukey >> 9)) as usize & 511;
                    self.cache.keys[idx] = pe.ukey;
                    self.cache.ids[idx] = id;
                    id
                }
            } else {
                INVALID
            };
            let p = fib(pe.key, 64 - self.pshift);
            out.ev.push(p, R::make(pe.key, pe.ing, user, pe.ts, pe.price, pe.cat));
        }
        self.nev = 0;
    }

    #[inline(always)]
    fn click<R: Copy>(&mut self, line: &[u8], f: &csv::Spans<4>, out: &mut T1Out<R>) {
        let fld = |i: usize| unsafe { line.get_unchecked(f[i].0 as usize..f[i].1 as usize) };
        let cid = fld(0);
        let user_s = fld(1);
        if cid.is_empty() || user_s.is_empty() {
            return;
        }
        let ctx = self.ctx;
        let t = match unsafe { self.tsp.parse_padded(fld(3)) } {
            Some(t) if t >= ctx.base && t < ctx.we => t,
            _ => return,
        };
        let camp = match ctx.campaign(fld(2)) {
            Some(c) => c,
            None => return,
        };
        let ukey = unsafe { key_of_padded(user_s) };
        let idx = (ukey ^ (ukey >> 9)) as usize & 511;
        let (user, uh) = if self.cache.keys[idx] == ukey {
            (self.cache.ids[idx], 0)
        } else {
            let uh = self.table.hash(ukey);
            self.table.prefetch(uh);
            (INVALID, uh)
        };
        self.clb[self.ncl] = PendCl { ckey: unsafe { key_of_padded(cid) }, ukey, uh, user, ts: (t - ctx.base) as u32, camp };
        self.ncl += 1;
        if self.ncl == BATCH {
            self.flush_clicks(out);
        }
    }

    #[inline(always)]
    fn flush_clicks<R: Copy>(&mut self, out: &mut T1Out<R>) {
        for i in 0..self.ncl {
            let pc = self.clb[i];
            let user = if pc.user != INVALID {
                pc.user
            } else {
                let id = self.table.get_or_insert(pc.ukey, pc.uh, &mut self.alloc);
                let idx = (pc.ukey ^ (pc.ukey >> 9)) as usize & 511;
                self.cache.keys[idx] = pc.ukey;
                self.cache.ids[idx] = id;
                id
            };
            out.clicks.push(CRec { ckey: pc.ckey, user, ts: pc.ts, camp: pc.camp, _p: 0 });
        }
        self.ncl = 0;
    }

    #[inline(always)]
    fn link<R: Copy>(&mut self, line: &[u8], f: &csv::Spans<2>, out: &mut T1Out<R>) {
        let a = &line[f[0].0 as usize..f[0].1 as usize];
        let b = &line[f[1].0 as usize..f[1].1 as usize];
        if a.is_empty() || b.is_empty() || a == b {
            return;
        }
        let ka = unsafe { key_of_padded(a) };
        let kb = unsafe { key_of_padded(b) };
        let (ha, hb) = (self.table.hash(ka), self.table.hash(kb));
        self.table.prefetch(ha);
        self.table.prefetch(hb);
        self.lkb[self.nlk] = (ka, kb, ha, hb);
        self.nlk += 1;
        if self.nlk == BATCH {
            self.flush_links(out);
        }
    }

    #[inline(always)]
    fn flush_links<R: Copy>(&mut self, out: &mut T1Out<R>) {
        for i in 0..self.nlk {
            let (ka, kb, ha, hb) = self.lkb[i];
            let ia = self.table.get_or_insert(ka, ha, &mut self.alloc);
            let ib = self.table.get_or_insert(kb, hb, &mut self.alloc);
            out.links.push((ia, ib));
        }
        self.nlk = 0;
    }
}

fn process_chunk<R: ERec>(p: &mut Parser, f: &InFile, ch: &Chunk, out: &mut T1Out<'_, R>) {
    let data = unsafe { std::slice::from_raw_parts(f.ptr, f.size) };
    let n = f.size;
    // first line starting in [s, e); the line at 0 is the header
    let mut i = if ch.s == 0 {
        data.iter().position(|&c| c == b'\n').map_or(n, |q| q + 1)
    } else if data[ch.s - 1] == b'\n' {
        ch.s
    } else {
        data[ch.s..].iter().position(|&c| c == b'\n').map_or(n, |q| ch.s + q + 1)
    };
    let ptr = f.ptr;
    let mut count = 0u64;
    while i < ch.e {
        let avail = n - i;
        macro_rules! handle {
            ($nf:literal, $method:ident) => {{
                let (len, fields) = unsafe { csv::split_line::<$nf>(ptr.add(i), avail) };
                if let Some(fl) = fields {
                    let line = unsafe { std::slice::from_raw_parts(ptr.add(i), len) };
                    p.$method(line, &fl, out);
                }
                count += 1;
                i += len + 1;
            }};
        }
        match f.kind {
            Kind::Events => handle!(7, event),
            Kind::Clicks => handle!(4, click),
            Kind::Links => handle!(2, link),
        }
    }
    p.flush_events(out);
    p.flush_clicks(out);
    p.flush_links(out);
    out.lines[f.kind as usize] += count;
}

// ---------------------------------------------------------------- union-find (lock-free)

use std::sync::atomic::{AtomicU32, AtomicU64};

#[inline]
fn uf_find(parent: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let p = parent[x as usize].load(Ordering::Acquire);
        if p == x {
            return x;
        }
        let gp = parent[p as usize].load(Ordering::Acquire);
        if gp != p {
            let _ = parent[x as usize].compare_exchange(p, gp, Ordering::AcqRel, Ordering::Relaxed);
        }
        x = gp;
    }
}

/// Joins the sets of a and b, always hanging the larger root under the smaller.
fn uf_unite(parent: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = uf_find(parent, a);
        b = uf_find(parent, b);
        if a == b {
            return;
        }
        if a < b {
            std::mem::swap(&mut a, &mut b);
        }
        if parent[a as usize].compare_exchange(a, b, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

fn atomic_min_key(cell: &AtomicU64, k: u64) {
    let mut cur = cell.load(Ordering::Relaxed);
    loop {
        if cur != 0 && key_cmp(cur, k) != std::cmp::Ordering::Greater {
            return;
        }
        match cell.compare_exchange_weak(cur, k, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return,
            Err(v) => cur = v,
        }
    }
}

/// Runs `f(thread, range)` over 0..n split into `nt` contiguous ranges, in parallel.
fn par_ranges(n: usize, nt: usize, f: impl Fn(usize, std::ops::Range<usize>) + Sync) {
    let per = n.div_ceil(nt).max(1);
    thread::scope(|sc| {
        for t in 0..nt {
            let f = &f;
            let r = (t * per).min(n)..((t + 1) * per).min(n);
            sc.spawn(move || f(t, r));
        }
    });
}

// ---------------------------------------------------------------- per-person processing

#[derive(Clone, Copy, Default)]
struct CampCnt {
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

#[derive(Clone, Copy)]
struct UserRow {
    person: u32,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u32,
}

struct T3State {
    rows: Vec<UserRow>,
    cc: Vec<CampCnt>,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    daycats: Vec<(u32, u64)>,
    evs: Vec<Ev>,
    cls: Vec<Cl>,
    counts: Vec<u32>,
    ccounts: Vec<u32>,
    fill: Vec<u32>,
    cfill: Vec<u32>,
}

struct DayAcc {
    day: i64,
    sessions: u32,
    events: u32,
    revenue: u64,
}

fn flush_day(st: &mut T3State, person: u32, acc: &DayAcc) {
    let mut top = INVALID;
    let mut best = 0u64;
    for &(c, s) in &st.daycats {
        if s > best || (s == best && c < top) {
            best = s;
            top = c;
        }
    }
    st.daycats.clear();
    st.rows.push(UserRow {
        person,
        day: acc.day as i32,
        sessions: acc.sessions,
        events: acc.events,
        revenue: acc.revenue,
        top,
    });
}

#[inline(always)]
fn add_camp(st: &mut T3State, c: u32) {
    let n = &mut st.cnt[c as usize];
    *n += 1;
    if *n == 1 {
        st.pos[c as usize] = st.active.len() as u32;
        st.active.push(c);
    }
}

#[inline(always)]
fn remove_camp(st: &mut T3State, c: u32) {
    let n = &mut st.cnt[c as usize];
    *n -= 1;
    if *n == 0 {
        let p = st.pos[c as usize] as usize;
        let last = st.active.pop().unwrap();
        if p < st.active.len() {
            st.active[p] = last;
            st.pos[last as usize] = p as u32;
        }
    }
}

fn process_person(ctx: &Ctx, st: &mut T3State, person: u32, evs: &[Ev], cls: &[Cl], organic: u32) {
    let n_days = ctx.n_days;
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut acc = DayAcc { day: i64::MIN, sessions: 0, events: 0, revenue: 0 };
    let mut i = 0usize;
    while i < evs.len() {
        let s = evs[i].ts;
        let mut prev = s;
        let mut j = i;
        let mut rev = 0u64;
        while j < evs.len() {
            let e = evs[j];
            if e.ts - prev > GAP {
                break;
            }
            prev = e.ts;
            rev += e.price as u64;
            j += 1;
        }
        let day = (ctx.base + s as i64).div_euclid(DAY);
        if day != acc.day {
            if acc.sessions > 0 {
                flush_day(st, person, &acc);
            }
            acc = DayAcc { day, sessions: 0, events: 0, revenue: 0 };
        }
        acc.sessions += 1;
        acc.events += (j - i) as u32;
        acc.revenue += rev;
        if rev > 0 {
            for e in &evs[i..j] {
                if e.price > 0 {
                    match st.daycats.iter_mut().find(|x| x.0 == e.cat) {
                        Some(x) => x.1 += e.price as u64,
                        None => st.daycats.push((e.cat, e.price as u64)),
                    }
                }
            }
        }

        while hi < cls.len() && cls[hi].ts <= s {
            add_camp(st, cls[hi].camp);
            hi += 1;
        }
        while lo < hi && (cls[lo].ts as u64) + (WEEK as u64) < s as u64 {
            remove_camp(st, cls[lo].camp);
            lo += 1;
        }
        let camp = if hi > lo {
            // Clicks are sorted (ts ASC, ckey DESC), so cls[hi-1] is the click
            // with the latest ts and, among ties, the smallest click_id.
            cls[hi - 1].camp
        } else {
            organic
        };
        let di = (day - ctx.first_day) as usize;
        let c = &mut st.cc[camp as usize * n_days + di];
        c.sessions += 1;
        c.revenue += rev;
        if rev > 0 {
            c.purchasing += 1;
        }
        for a in 0..st.active.len() {
            let cid = st.active[a] as usize;
            st.cc[cid * n_days + di].assisted += 1;
        }
        i = j;
    }
    if acc.sessions > 0 {
        flush_day(st, person, &acc);
    }
    for a in 0..st.active.len() {
        let cid = st.active[a] as usize;
        st.cnt[cid] = 0;
    }
    st.active.clear();
}

// ---------------------------------------------------------------- main

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(env_or("DATA_DIR", "/data"));
    let out = PathBuf::from(env_or("OUTPUT_DIR", "/output"));
    let args: Vec<String> = std::env::args().collect();
    let (data, out) = if args.len() >= 3 { (PathBuf::from(&args[1]), PathBuf::from(&args[2])) } else { (data, out) };
    let nt: usize = std::env::var("THREADS")
        .ok()
        .or_else(|| std::env::var("NCPU").ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| thread::available_parallelism().map_or(8, |n| n.get()))
        .max(1);

    let ctx = build_ctx(&data);
    log!("ctx: {:.3}s", t0.elapsed().as_secs_f64());
    if ctx.cat_names.len() <= 256 {
        run::<ENarrow>(&ctx, &data, &out, nt, t0);
    } else {
        run::<EWide>(&ctx, &data, &out, nt, t0);
    }
}

fn run<R: ERec>(ctx: &Ctx, data: &Path, out: &Path, nt: usize, t0: Instant) {
    let mut files: Vec<InFile> = Vec::new();
    for (dir, kind) in [("links", Kind::Links), ("events", Kind::Events), ("clicks", Kind::Clicks)] {
        for p in list_csv(&data.join(dir)) {
            files.push(InFile::open(&p, kind));
        }
    }
    let event_bytes: u64 = files.iter().filter(|f| f.kind == Kind::Events).map(|f| f.size as u64).sum();
    let total_bytes: u64 = files.iter().map(|f| f.size as u64).sum();
    let click_bytes: u64 = files.iter().filter(|f| f.kind == Kind::Clicks).map(|f| f.size as u64).sum();
    // chunks: links first (small), then the rest biggest-file-first
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut order: Vec<usize> = (0..files.len()).collect();
    order.sort_by_key(|&i| (files[i].kind != Kind::Links, std::cmp::Reverse(files[i].size)));
    for &fi in &order {
        let size = files[fi].size;
        let mut s = 0;
        while s < size {
            let e = (s + CHUNK).min(size);
            chunks.push(Chunk { file: fi, s, e });
            s = e;
        }
    }
    let est_events = (event_bytes / 75).max(1000);
    let pbits = std::env::var("PBITS").ok().and_then(|v| v.parse().ok()).unwrap_or(((est_events / 48_000).max(16).next_power_of_two().trailing_zeros()).min(10));
    let np_parts = 1usize << pbits;
    let pshift = 64 - pbits;

    let mut slots = (total_bytes / 600).max(1 << 16) as usize * 3 / 2;
    let n_link_chunks = chunks.iter().filter(|c| files[c.file].kind == Kind::Links).count();
    let rec_bytes = std::mem::size_of::<R>();
    let pool1 = Pool::new(est_events as usize * rec_bytes * 5 / 4 + nt * np_parts * BLOCK_BYTES);
    let (t1, table) = 'attempt: loop {
        let table = UserTable::new(slots, nt);
        log!("table: {:.3}s", t0.elapsed().as_secs_f64());
        let new_out = || {
            T1Out::<R> {
                ev: PartBuf::new(&pool1, np_parts),
                clicks: Vec::with_capacity((click_bytes / 45 / nt as u64) as usize * 5 / 4 + 1024),
                links: Vec::new(),
                lines: [0; 3],
            }
        };
        let parse = |range: std::ops::Range<usize>, with_buffers: bool| -> Vec<T1Out<R>> {
            let next = AtomicUsize::new(range.start);
            thread::scope(|sc| {
                let hs: Vec<_> = (0..nt)
                    .map(|_| {
                        sc.spawn(|| {
                            let mut o = if with_buffers {
                                new_out()
                            } else {
                                T1Out::<R> { ev: PartBuf::new(&pool1, 1), clicks: Vec::new(), links: Vec::new(), lines: [0; 3] }
                            };
                            let mut p = Parser {
                                ctx,
                                table: &table,
                                alloc: IdAlloc::new(),
                                cache: UserCache::new(),
                                pshift,
                                tsp: TsParser::new(),
                                evb: [PendEv::default(); BATCH],
                                nev: 0,
                                clb: [PendCl::default(); BATCH],
                                ncl: 0,
                                lkb: [(0, 0, 0, 0); BATCH],
                                nlk: 0,
                            };
                            loop {
                                let c = next.fetch_add(1, Ordering::Relaxed);
                                if c >= range.end || table.overflow.load(Ordering::Relaxed) {
                                    break;
                                }
                                let ch = &chunks[c];
                                process_chunk(&mut p, &files[ch.file], ch, &mut o);
                            }
                            o
                        })
                    })
                    .collect();
                hs.into_iter().map(|h| h.join().unwrap()).collect()
            })
        };
        // links first: every person's ids are joined before any event is read
        let tl = parse(0..n_link_chunks, false);
        if table.overflow.load(Ordering::Relaxed) {
            slots *= 4;
            continue 'attempt;
        }
        log!("links parsed: {:.3}s", t0.elapsed().as_secs_f64());
        let n_ids = table.id_limit();
        let parent: Vec<AtomicU32> = (0..n_ids as u32).map(AtomicU32::new).collect();
        let edges: Vec<&[(u32, u32)]> = tl.iter().map(|o| o.links.as_slice()).collect();
        let n_edges: usize = edges.iter().map(|e| e.len()).sum();
        par_ranges(edges.len(), nt.min(edges.len()).max(1), |_, r| {
            for e in &edges[r] {
                for &(a, b) in e.iter() {
                    uf_unite(&parent, a, b);
                }
            }
        });
        log!("links united: {:.3}s", t0.elapsed().as_secs_f64());
        let names = table.names();
        let minname: Vec<AtomicU64> = (0..n_ids).map(|_| AtomicU64::new(0)).collect();
        par_ranges(n_ids, nt, |_, r| {
            for x in r {
                if names[x] != 0 && parent[x].load(Ordering::Relaxed) != x as u32 {
                    let root = uf_find(&parent, x as u32);
                    atomic_min_key(&minname[root as usize], names[x]);
                    table.redirect(names[x], root);
                }
            }
        });
        par_ranges(n_ids, nt, |_, r| {
            for x in r {
                let m = minname[x].load(Ordering::Relaxed);
                if m != 0 && key_cmp(m, names[x]) == std::cmp::Ordering::Less {
                    unsafe { table.set_name(x as u32, m) };
                }
            }
        });
        log!("links: {:.3}s  edges {}  ids {}", t0.elapsed().as_secs_f64(), n_edges, n_ids);
        drop(tl);
        let t1 = parse(n_link_chunks..chunks.len(), true);
        if table.overflow.load(Ordering::Relaxed) {
            log!("user table overflow at {slots} slots; retrying larger");
            slots *= 4;
            continue 'attempt;
        }
        break (t1, table);
    };
    let lines: [u64; 3] = t1.iter().fold([0; 3], |a, o| [a[0] + o.lines[0], a[1] + o.lines[1], a[2] + o.lines[2]]);
    log!(
        "stage1 parse: {:.3}s  lines links/events/clicks {:?}  users {}  parts {}",
        t0.elapsed().as_secs_f64(),
        lines,
        table.id_limit(),
        np_parts
    );
    let names = table.names();
    let qbits = pbits;
    let nq = 1usize << qbits;
    let np = table.id_limit();

    // ---- stage 2: dedup events per event-id partition, scatter survivors and clicks by person
    struct T2Out<'p> {
        ev: PartBuf<'p, PRec>,
        cl: PartBuf<'p, PClick>,
    }
    const CLICK_ITEM: usize = 1 << 20;
    let n_clicks: usize = t1.iter().map(|o| o.clicks.len()).sum();
    let n_events: usize = t1.iter().map(|o| (0..np_parts).map(|p| o.ev.len(p)).sum::<usize>()).sum();
    let _ = n_events;
    let pool2 = Pool::new(n_clicks * std::mem::size_of::<PClick>() + 2 * nt * nq * BLOCK_BYTES);
    let mut items2: Vec<(usize, usize)> = (0..np_parts).map(|p| (usize::MAX, p)).collect();
    for (t, o) in t1.iter().enumerate() {
        for s in (0..o.clicks.len()).step_by(CLICK_ITEM) {
            items2.push((t, s));
        }
    }
    let next = AtomicUsize::new(0);
    let t2: Vec<T2Out> = thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                sc.spawn(|| {
                    let mut o = T2Out { ev: PartBuf::new(&pool2, nq), cl: PartBuf::new(&pool2, nq) };
                    let mut recs: Vec<R> = Vec::new();
                    let mut tab: Vec<u32> = Vec::new();
                    let qmask = (nq - 1) as u32;
                    loop {
                        let it = next.fetch_add(1, Ordering::Relaxed);
                        if it >= items2.len() {
                            break;
                        }
                        let (t, x) = items2[it];
                        if t == usize::MAX {
                            recs.clear();
                            for o1 in &t1 {
                                for c in o1.ev.chunks(x) {
                                    recs.extend_from_slice(c);
                                }
                            }
                            // partition x is consumed: its blocks back this thread's output
                            for o1 in &t1 {
                                o.ev.spare.extend(o1.ev.block_ptrs(x));
                            }
                            let n = recs.len();
                            let cap = (n * 2).next_power_of_two().max(16);
                            let mask = cap - 1;
                            let tbits = cap.trailing_zeros();
                            tab.clear();
                            tab.resize(cap, 0);
                            for i in 0..n {
                                let k = recs[i].key();
                                let mut h = fib2(k, tbits);
                                loop {
                                    let sl = unsafe { *tab.get_unchecked(h) };
                                    if sl == 0 {
                                        unsafe { *tab.get_unchecked_mut(h) = i as u32 + 1; }
                                        break;
                                    }
                                    let j = (sl - 1) as usize;
                                    if unsafe { recs.get_unchecked(j) }.key() == k {
                                        if recs[i].ingest() < unsafe { recs.get_unchecked(j) }.ingest() {
                                            unsafe { *tab.get_unchecked_mut(h) = i as u32 + 1; }
                                        }
                                        break;
                                    }
                                    h = (h + 1) & mask;
                                }
                            }
                            for &sl in &tab {
                                if sl == 0 {
                                    continue;
                                }
                                let r = unsafe { recs.get_unchecked((sl - 1) as usize) };
                                if r.user() == INVALID {
                                    continue;
                                }
                                let d = r.user();
                                let q = (d & qmask) as usize;
                                let local = (d >> qbits) as u64;
                                o.ev.push(q, PRec { k: (local << 32) | r.ts() as u64, price: r.price(), cat: r.cat() });
                            }
                        } else {
                            let need = (CLICK_ITEM * std::mem::size_of::<PClick>()).div_ceil(BLOCK_BYTES) + nq;
                            while o.cl.spare.len() < need {
                                match o.ev.spare.pop() {
                                    Some(b) => o.cl.spare.push(b),
                                    None => break,
                                }
                            }
                            let cl = &t1[t].clicks;
                            for c in &cl[x..(x + CLICK_ITEM).min(cl.len())] {
                                let d = c.user;
                                let q = (d & qmask) as usize;
                                let local = (d >> qbits) as u64;
                                o.cl.push(q, PClick { k: (local << 32) | c.ts as u64, ckey: c.ckey, camp: c.camp, _p: 0 });
                            }
                        }
                    }
                    o
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(t1);
    log!("stage2 dedup+scatter: {:.3}s", t0.elapsed().as_secs_f64());

    // ---- stage 3: sessions, attribution, aggregation per person partition
    let n_camp = ctx.camp_ids.len();
    let organic = n_camp as u32;
    let mut items3: Vec<(usize, usize)> =
        (0..nq).map(|q| (t2.iter().map(|o| o.ev.len(q)).sum::<usize>(), q)).collect();
    items3.sort_unstable_by(|a, b| b.cmp(a));
    let next = AtomicUsize::new(0);
    let t3: Vec<T3State> = thread::scope(|sc| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                sc.spawn(|| {
                    let mut st = T3State {
                        rows: Vec::new(),
                        cc: vec![CampCnt::default(); (n_camp + 1) * ctx.n_days],
                        cnt: vec![0; n_camp + 1],
                        pos: vec![0; n_camp + 1],
                        active: Vec::new(),
                        daycats: Vec::new(),
                        evs: Vec::new(),
                        cls: Vec::new(),
                        counts: Vec::new(),
                        ccounts: Vec::new(),
                        fill: Vec::new(),
                        cfill: Vec::new(),
                    };
                    loop {
                        let it = next.fetch_add(1, Ordering::Relaxed);
                        if it >= items3.len() {
                            break;
                        }
                        let q = items3[it].1;
                        let n_local = (np >> qbits) + 1;
                        // counting sort events by local person
                        let mut counts = std::mem::take(&mut st.counts);
                        counts.clear();
                        counts.resize(n_local + 1, 0);
                        let mut n = 0usize;
                        for o in &t2 {
                            for c in o.ev.chunks(q) {
                                for r in c {
                                    counts[(r.k >> 32) as usize + 1] += 1;
                                }
                                n += c.len();
                            }
                        }
                        for l in 0..n_local {
                            counts[l + 1] += counts[l];
                        }
                        let mut evs = std::mem::take(&mut st.evs);
                        evs.clear();
                        evs.reserve(n);
                        unsafe { evs.set_len(n); }
                        {
                            st.fill.clear();
                            st.fill.extend_from_slice(&counts);
                            let fill = &mut st.fill;
                            for o in &t2 {
                                for c in o.ev.chunks(q) {
                                    for r in c {
                                        let l = (r.k >> 32) as usize;
                                        evs[fill[l] as usize] = Ev { ts: r.k as u32, price: r.price, cat: r.cat };
                                        fill[l] += 1;
                                    }
                                }
                            }
                        }
                        let mut cc = std::mem::take(&mut st.ccounts);
                        cc.clear();
                        cc.resize(n_local + 1, 0);
                        let mut ncl = 0usize;
                        for o in &t2 {
                            for c in o.cl.chunks(q) {
                                for r in c {
                                    cc[(r.k >> 32) as usize + 1] += 1;
                                }
                                ncl += c.len();
                            }
                        }
                        for l in 0..n_local {
                            cc[l + 1] += cc[l];
                        }
                        let mut cls = std::mem::take(&mut st.cls);
                        cls.clear();
                        cls.reserve(ncl);
                        unsafe { cls.set_len(ncl); }
                        {
                            st.cfill.clear();
                            st.cfill.extend_from_slice(&cc);
                            let fill = &mut st.cfill;
                            for o in &t2 {
                                for c in o.cl.chunks(q) {
                                    for r in c {
                                        let l = (r.k >> 32) as usize;
                                        cls[fill[l] as usize] = Cl { ts: r.k as u32, camp: r.camp, ckey: r.ckey };
                                        fill[l] += 1;
                                    }
                                }
                            }
                        }
                        for l in 0..n_local {
                            let (a, b) = (counts[l] as usize, counts[l + 1] as usize);
                            if a == b {
                                continue;
                            }
                            let es = &mut evs[a..b];
                            match es.len() {
                                0 | 1 => {}
                                2 => {
                                    if es[0].ts > es[1].ts {
                                        es.swap(0, 1);
                                    }
                                }
                                3 => {
                                    if es[0].ts > es[1].ts { es.swap(0, 1); }
                                    if es[1].ts > es[2].ts { es.swap(1, 2); }
                                    if es[0].ts > es[1].ts { es.swap(0, 1); }
                                }
                                _ => es.sort_unstable_by_key(|e| e.ts),
                            }
                            let (ca, cb) = (cc[l] as usize, cc[l + 1] as usize);
                            let cs = &mut cls[ca..cb];
                            match cs.len() {
                                0 | 1 => {}
                                2 => {
                                    let cmp = cs[0].ts.cmp(&cs[1].ts).then_with(|| key_cmp(cs[1].ckey, cs[0].ckey));
                                    if cmp == std::cmp::Ordering::Greater {
                                        cs.swap(0, 1);
                                    }
                                }
                                _ => cs.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key_cmp(b.ckey, a.ckey))),
                            }
                            let cs_slice = if cs.len() > 1 {
                                let mut w = 0;
                                for r in 1..cs.len() {
                                    if cs[r].ckey != cs[w].ckey {
                                        w += 1;
                                        cs[w] = cs[r];
                                    }
                                }
                                &cs[..=w]
                            } else {
                                &*cs
                            };
                            let person = (l * nq + q) as u32;
                            process_person(ctx, &mut st, person, es, cs_slice, organic);
                        }
                        st.counts = counts;
                        st.ccounts = cc;
                        st.evs = evs;
                        st.cls = cls;
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    drop(t2);
    log!(
        "stage3 sessions: {:.3}s  user rows {}",
        t0.elapsed().as_secs_f64(),
        t3.iter().map(|s| s.rows.len()).sum::<usize>()
    );

    // ---- outputs
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    thread::scope(|sc| {
        for (t, st) in t3.iter().enumerate() {
            let ud = &ud;
            sc.spawn(move || write_user_daily(ctx, &ud.join(format!("part-{t:04}.parquet")), &st.rows, names));
        }
        let mut cc = vec![CampCnt::default(); (n_camp + 1) * ctx.n_days];
        for st in &t3 {
            for (a, b) in cc.iter_mut().zip(&st.cc) {
                a.sessions += b.sessions;
                a.purchasing += b.purchasing;
                a.revenue += b.revenue;
                a.assisted += b.assisted;
            }
        }
        write_campaign_daily(ctx, &cd.join("part-0000.parquet"), &cc);
    });
    log!("done: {:.3}s", t0.elapsed().as_secs_f64());
    // Skip freeing gigabytes of heap memory — all outputs are flushed.
    std::process::exit(0);
}

fn write_user_daily(ctx: &Ctx, path: &Path, rows: &[UserRow], names: &[u64]) {
    use parquet::{as_bytes, push_bytes, Column, Kind as K};
    let n = rows.len();
    let mut ubuf = Vec::with_capacity(n * 14);
    let mut days = Vec::with_capacity(n);
    let mut sess = Vec::with_capacity(n);
    let mut evs = Vec::with_capacity(n);
    let mut def = Vec::with_capacity(n);
    let mut cbuf = Vec::with_capacity(n * 8);
    let mut rev = Vec::with_capacity(n);
    for r in rows {
        keys::push_plain(names[r.person as usize], &mut ubuf);
        days.push(r.day);
        sess.push(r.sessions as i32);
        evs.push(r.events as i32);
        rev.push(r.revenue as i64);
        if r.top != INVALID {
            def.push(true);
            push_bytes(&mut cbuf, &ctx.cat_names[r.top as usize]);
        } else {
            def.push(false);
        }
    }
    // INT64 even when the values would fit INT32: a reader summing an INT32 column can overflow.
    let defb = parquet::encode_def(&def);
    parquet::write_file(
        path,
        n,
        &[
            Column { name: "user_id", kind: K::Utf8, optional: false, parts: vec![&ubuf] },
            Column { name: "day", kind: K::Date, optional: false, parts: vec![as_bytes(&days)] },
            Column { name: "sessions", kind: K::Int32, optional: false, parts: vec![as_bytes(&sess)] },
            Column { name: "events", kind: K::Int32, optional: false, parts: vec![as_bytes(&evs)] },
            Column { name: "revenue_cents", kind: K::Int64, optional: false, parts: vec![as_bytes(&rev)] },
            Column { name: "top_category", kind: K::Utf8, optional: true, parts: vec![&defb, &cbuf] },
        ],
    )
    .expect("write user_daily");
}

fn write_campaign_daily(ctx: &Ctx, path: &Path, cc: &[CampCnt]) {
    use parquet::{as_bytes, push_bytes, Column, Kind as K};
    let n_days = ctx.n_days;
    let n_camp = ctx.camp_ids.len();
    let mut ib = Vec::new();
    let mut chb = Vec::new();
    let mut days = Vec::new();
    let (mut s, mut p, mut r, mut a) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=n_camp {
        for d in 0..n_days {
            let x = cc[c * n_days + d];
            if x.sessions == 0 && x.assisted == 0 {
                continue;
            }
            if c == n_camp {
                push_bytes(&mut ib, b"organic");
                push_bytes(&mut chb, b"organic");
            } else {
                push_bytes(&mut ib, &ctx.camp_ids[c]);
                push_bytes(&mut chb, &ctx.camp_channels[c]);
            }
            days.push((ctx.first_day + d as i64) as i32);
            s.push(x.sessions as i64);
            p.push(x.purchasing as i64);
            r.push(x.revenue as i64);
            a.push(x.assisted as i64);
        }
    }
    parquet::write_file(
        path,
        days.len(),
        &[
            Column { name: "campaign_id", kind: K::Utf8, optional: false, parts: vec![&ib] },
            Column { name: "channel", kind: K::Utf8, optional: false, parts: vec![&chb] },
            Column { name: "day", kind: K::Date, optional: false, parts: vec![as_bytes(&days)] },
            Column { name: "sessions", kind: K::Int64, optional: false, parts: vec![as_bytes(&s)] },
            Column { name: "purchasing_sessions", kind: K::Int64, optional: false, parts: vec![as_bytes(&p)] },
            Column { name: "revenue_cents", kind: K::Int64, optional: false, parts: vec![as_bytes(&r)] },
            Column { name: "assisted_sessions", kind: K::Int64, optional: false, parts: vec![as_bytes(&a)] },
        ],
    )
    .expect("write campaign_daily");
}
