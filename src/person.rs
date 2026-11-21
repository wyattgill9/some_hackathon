//! One person partition at a time: a local person index, counting sorts of events and clicks by person, then per
//! person the sessions (gap > 30 min), the user_daily rows, the credited click and the assisting campaigns.
use std::path::Path;

use crate::key::{self, Key};
use crate::mem;
use crate::parquet::{self, Buf, Column, Enc, Kind};
use crate::{Ck, Ctx, DAY, Ev2, GAP, WEEK};

const NONE: u32 = u32::MAX;
/// Events and clicks are binned by `ts >> BIN`: 2^20 ms, under the session gap, so one bin never spans two
/// sessions and a session boundary is a gap over GAP between consecutive non-empty bins.
const BIN: u32 = 20;
const _: () = assert!((1u64 << BIN) <= GAP as u64 + 1);
/// Multiplier of the local index hash (the partition took the top bits of `mix`).
const HASH: u64 = 0xD6E8_FEB8_6659_FD93;
/// Sessions x clicks up to which a person takes the all-pairs attribution path.
const SMALL: usize = 256;

/// Per (campaign, day) totals; `organic` is campaign index `ncamp`.
#[derive(Clone, Copy, Default)]
pub struct Cell {
    pub sessions: u32,
    pub purchasing: u32,
    pub assisted: u32,
    pub revenue: i64,
}

/// The credited part of a cell, 16 bytes: what every session updates.
#[derive(Clone, Copy, Default)]
struct Credit {
    sessions: u32,
    purchasing: u32,
    revenue: i64,
}

/// user_daily rows as columns: strings as lengths and bytes, `present` as packed bits.
pub struct Rows {
    pub n: usize,
    idlen: Buf<i32>,
    idbytes: Buf<u8>,
    day: Buf<i32>,
    sessions: Buf<i32>,
    events: Buf<i32>,
    revenue: Buf<i64>,
    present: Buf<u8>,
    toplen: Buf<i32>,
    topbytes: Buf<u8>,
}

impl Rows {
    /// Columns reserved for about `rows` rows (virtual; they grow past it).
    fn new(rows: usize) -> Self {
        Rows {
            n: 0,
            idlen: Buf::new(rows),
            idbytes: Buf::new(rows * 10),
            day: Buf::new(rows),
            sessions: Buf::new(rows),
            events: Buf::new(rows),
            revenue: Buf::new(rows),
            present: Buf::new(rows / 8 + 1),
            toplen: Buf::new(rows),
            topbytes: Buf::new(rows * 10),
        }
    }
    /// Room for `n` more rows whose strings are at most 16 bytes (longer ones reserve as they go).
    fn reserve(&mut self, n: usize) {
        self.idlen.reserve(n);
        self.idbytes.reserve(n * 16);
        self.day.reserve(n);
        self.sessions.reserve(n);
        self.events.reserve(n);
        self.revenue.reserve(n);
        self.present.reserve(n / 8 + 1);
        self.toplen.reserve(n);
        self.topbytes.reserve(n * 16);
    }
}

/// A session: start in ms from `cws`, day index, purchase revenue.
struct Sess {
    start: u32,
    day: u32,
    rev: u64,
}

/// The per-person state and the outputs.
struct Out {
    sessions: Vec<Sess>,
    /// `day_first[d]` = index of the person's first session on day `d` or later; `ndays + 1` entries.
    day_first: Vec<u32>,
    cat_rev: Vec<u64>,
    touched: Vec<u32>,
    /// Per campaign, `person << 32 | sessions already counted as assisted` (stale when the person differs).
    covered: Vec<u64>,
    person: u64,
    buf: Vec<u8>,
    /// The ID of `buf` zero-padded, when it fits; the categories likewise.
    id: [u8; 16],
    cat16: Vec<[u8; 16]>,
    rows: Rows,
    /// `[campaign][day]` credits and assisted sessions; `organic` is campaign `ncamp`, which never assists.
    cells: Vec<Credit>,
    assisted: Vec<u32>,
}

/// A slot of the local person index: the key, its dense local id and its event count.
#[derive(Clone, Copy, Default)]
struct Slot {
    key: Key,
    li: u32,
    count: u32,
}

pub struct Worker<'a> {
    ctx: &'a Ctx,
    nbins: usize,
    /// Open addressing, sized from the persons-per-event ratio seen so far; only touched slots are cleared.
    table: Vec<Slot>,
    /// Slot of each local id.
    persons: Vec<u32>,
    ev_seen: usize,
    np_seen: usize,
    bcounts: Vec<u32>,
    boffs: Vec<u32>,
    cursor: Vec<u32>,
    lis: Vec<u32>,
    /// Events by bin as `ts << 32 | local id`, then by person as `ts`; purchases apart as (local id, ts, pay),
    /// then by person as (ts, pay).
    tmp: Vec<u64>,
    offs: Vec<u32>,
    sorted: Vec<u32>,
    purch: Vec<[u32; 3]>,
    pcounts: Vec<u32>,
    poffs: Vec<u32>,
    psorted: Vec<[u32; 2]>,
    ccounts: Vec<u32>,
    coffs: Vec<u32>,
    ctmp: Vec<[u64; 2]>,
    ctmp_li: Vec<u32>,
    csorted: Vec<[u64; 2]>,
    out: Out,
    /// The encoded user_daily columns (see `UserFile::write`), mapped and prefaulted up front so the encoding
    /// at the end never waits for fresh pages.
    enc: [Buf<u8>; 7],
}

impl<'a> Worker<'a> {
    /// `rows` is the worker's expected share of user_daily rows (an upper estimate; only virtual space).
    pub fn new(ctx: &'a Ctx, rows: usize) -> Self {
        Worker {
            ctx,
            nbins: ((ctx.we - ctx.cws) >> BIN) as usize + 2,
            table: Vec::new(),
            persons: Vec::new(),
            ev_seen: 0,
            np_seen: 0,
            bcounts: Vec::new(),
            boffs: Vec::new(),
            cursor: Vec::new(),
            lis: Vec::new(),
            tmp: Vec::new(),
            offs: Vec::new(),
            sorted: Vec::new(),
            purch: Vec::new(),
            pcounts: Vec::new(),
            poffs: Vec::new(),
            psorted: Vec::new(),
            ccounts: Vec::new(),
            coffs: Vec::new(),
            ctmp: Vec::new(),
            ctmp_li: Vec::new(),
            csorted: Vec::new(),
            enc: {
                let bound = parquet::delta_bound(rows);
                let mut enc = [Buf::new(bound), Buf::new(bound), Buf::new(bound), Buf::new(bound), Buf::new(bound), Buf::new(rows / 8 + 64), Buf::new(bound)];
                let r = rows / 2; // about 1.5x the rows expected, at the bytes a row usually encodes to
                for (b, bytes) in enc.iter_mut().zip([r, r, r, r, 3 * r, rows / 8 + 64, r]) {
                    b.prefault(bytes);
                }
                enc
            },
            out: Out {
                sessions: Vec::new(),
                day_first: Vec::new(),
                cat_rev: mem::zeroed(ctx.cats.len()),
                touched: Vec::new(),
                covered: mem::zeroed(ctx.ncamp),
                person: 0,
                buf: Vec::new(),
                id: [0; 16],
                cat16: ctx.cats.iter().map(|c| { let mut a = [0; 16]; for (d, &s) in a.iter_mut().zip(c) { *d = s; } a }).collect(),
                rows: Rows::new(rows),
                // Written pages, not calloc: these are read-modify-written, and a calloc page read first
                // maps the zero page, whose copy-on-write flushes every thread's TLB.
                cells: mem::zeroed::<Credit>((ctx.ncamp + 1) * ctx.ndays),
                assisted: mem::zeroed(ctx.ncamp * ctx.ndays),
            },
        }
    }

    /// Processes one partition, given its events and clicks from every parse thread.
    pub fn run<'e, 'c>(&mut self, evs: impl Iterator<Item = &'e [Ev2]>, cks: impl Iterator<Item = &'c [Ck]>) {
        // The blocks are read in place, twice each: once to count, once to scatter.
        let eblocks: Vec<&[Ev2]> = evs.collect();
        let n: usize = eblocks.iter().map(|b| b.len()).sum();
        if n == 0 {
            return;
        }
        let cblocks: Vec<&[Ck]> = cks.collect();
        self.out.rows.reserve(n); // at most one row per event
        // Events are never sorted: a counting sort by bin, then a stable one by person, leaves each person's
        // events in bin order, which is all the session walk needs. The bin scatter also builds the local
        // index (person key -> dense local id; clicks only look up) and tags each record with its id. A table
        // that fills past half is abandoned and the partition scattered again into one four times larger.
        self.bcounts.clear();
        self.bcounts.resize(self.nbins, 0);
        for e in eblocks.iter().flat_map(|b| b.iter()) {
            // In bounds: the bin comes from a ts inside the window.
            unsafe { *self.bcounts.get_unchecked_mut((e.ts >> BIN) as usize) += 1 };
        }
        prefix(&self.bcounts, &mut self.boffs);
        let est = if self.ev_seen == 0 { n / 4 } else { n * self.np_seen / self.ev_seen + n / 32 };
        let mut cap = (est + est / 2 + 2).next_power_of_two().max(1024);
        let np = loop {
            if self.table.len() != cap {
                self.table.clear();
                self.table.resize(cap, Slot::default());
            }
            match self.index(&eblocks, n) {
                Some(np) => break np,
                None => cap *= 4,
            }
        };
        self.ev_seen += n;
        self.np_seen += np;
        self.offs.clear();
        self.offs.push(0);
        let mut acc = 0;
        for &s in &self.persons {
            acc += self.table[s as usize].count;
            self.offs.push(acc);
        }
        self.cursor.clear();
        self.cursor.extend_from_slice(&self.offs[..np]);
        self.sorted.clear();
        self.sorted.reserve(n);
        unsafe {
            let (srt, cur) = (self.sorted.as_mut_ptr(), self.cursor.as_mut_ptr());
            for &rec in &self.tmp {
                let c = cur.add(rec as u32 as usize);
                *srt.add(*c as usize) = (rec >> 32) as u32;
                *c += 1;
            }
            self.sorted.set_len(n);
        }
        // Purchases by person, each person's in ts order.
        self.pcounts.clear();
        self.pcounts.resize(np, 0);
        for p in &self.purch {
            self.pcounts[p[0] as usize] += 1;
        }
        prefix(&self.pcounts, &mut self.poffs);
        self.cursor.clear();
        self.cursor.extend_from_slice(&self.poffs[..np]);
        self.psorted.clear();
        self.psorted.resize(self.purch.len(), [0; 2]);
        for p in &self.purch {
            let c = &mut self.cursor[p[0] as usize];
            self.psorted[*c as usize] = [p[1], p[2]];
            *c += 1;
        }
        // Clicks of people with no events are dropped: they can credit nothing. The rest are counting-sorted
        // by bin then person like the events; ties within a bin are ordered per person.
        let m: usize = cblocks.iter().map(|b| b.len()).sum();
        self.lis.clear();
        self.lis.reserve(m);
        self.bcounts.clear();
        self.bcounts.resize(self.nbins, 0);
        self.ccounts.clear();
        self.ccounts.resize(np, 0);
        let (mask, shift) = (self.table.len() - 1, 64 - self.table.len().trailing_zeros());
        unsafe {
            let (t, lis, bc, cc) = (self.table.as_ptr(), self.lis.as_mut_ptr(), self.bcounts.as_mut_ptr(), self.ccounts.as_mut_ptr());
            let mut k = 0;
            for c in cblocks.iter().flat_map(|b| b.iter()) {
                let mut i = (c.person.wrapping_mul(HASH) >> shift) as usize;
                let li = loop {
                    let s = &*t.add(i);
                    if s.key == c.person {
                        break s.li;
                    }
                    if s.key == 0 {
                        break NONE;
                    }
                    i = (i + 1) & mask;
                };
                *lis.add(k) = li;
                k += 1;
                if li != NONE {
                    *bc.add((c.ts >> BIN) as usize) += 1;
                    *cc.add(li as usize) += 1;
                }
            }
            self.lis.set_len(m);
        }
        prefix(&self.bcounts, &mut self.boffs);
        let mv = self.boffs[self.nbins] as usize;
        self.cursor.clear();
        self.cursor.extend_from_slice(&self.boffs[..self.nbins]);
        self.ctmp.clear();
        self.ctmp.reserve(mv);
        self.ctmp_li.clear();
        self.ctmp_li.reserve(mv);
        unsafe {
            let (ct, cl, cur) = (self.ctmp.as_mut_ptr(), self.ctmp_li.as_mut_ptr(), self.cursor.as_mut_ptr());
            for (c, &li) in cblocks.iter().flat_map(|b| b.iter()).zip(&self.lis) {
                if li != NONE {
                    let k = cur.add((c.ts >> BIN) as usize);
                    let p = *k as usize;
                    *k += 1;
                    *ct.add(p) = [(c.ts as u64) << 32 | c.camp as u64, c.cid];
                    *cl.add(p) = li;
                }
            }
            self.ctmp.set_len(mv);
            self.ctmp_li.set_len(mv);
        }
        prefix(&self.ccounts, &mut self.coffs);
        self.cursor.clear();
        self.cursor.extend_from_slice(&self.coffs[..np]);
        self.csorted.clear();
        self.csorted.reserve(mv);
        unsafe {
            let (cs, cur) = (self.csorted.as_mut_ptr(), self.cursor.as_mut_ptr());
            for (&rec, &li) in self.ctmp.iter().zip(&self.ctmp_li) {
                let c = cur.add(li as usize);
                *cs.add(*c as usize) = rec;
                *c += 1;
            }
            self.csorted.set_len(mv);
        }
        for li in 0..np {
            let evs = &self.sorted[self.offs[li] as usize..self.offs[li + 1] as usize];
            let purch = &mut self.psorted[self.poffs[li] as usize..self.poffs[li + 1] as usize];
            if purch.len() > 1 {
                purch.sort_unstable_by_key(|p| p[0]);
            }
            let cks = &mut self.csorted[self.coffs[li] as usize..self.coffs[li + 1] as usize];
            order_bins(cks);
            person(self.ctx, &mut self.out, self.table[self.persons[li] as usize].key, evs, purch, cks);
        }
        for &s in &self.persons {
            self.table[s as usize] = Slot::default();
        }
    }

    /// Scatters the events by bin into `tmp` (`ts << 32 | local id`), the purchases also into `purch`, building the index:
    /// the number of persons, or None (and a cleared table) when the table got half full.
    fn index(&mut self, eblocks: &[&[Ev2]], n: usize) -> Option<usize> {
        let cap = self.table.len();
        let (mask, shift) = (cap - 1, 64 - cap.trailing_zeros());
        self.persons.clear();
        self.persons.reserve(cap / 2);
        self.cursor.clear();
        self.cursor.extend_from_slice(&self.boffs[..self.nbins]);
        self.tmp.clear();
        self.tmp.reserve(n);
        self.purch.clear();
        self.purch.reserve(n);
        let mut np = 0usize;
        // Every event writes one of the n reserved slots (bins from the window, counted above), every new
        // person (at most cap / 2) one `persons` slot; probes wrap within the table.
        let full = unsafe {
            let (t, tmp, cur, ps, pu) = (self.table.as_mut_ptr(), self.tmp.as_mut_ptr(), self.cursor.as_mut_ptr(), self.persons.as_mut_ptr(), self.purch.as_mut_ptr());
            let mut npu = 0;
            let mut full = false;
            'all: for e in eblocks.iter().flat_map(|b| b.iter()) {
                let mut i = (e.person.wrapping_mul(HASH) >> shift) as usize;
                let li = loop {
                    let s = &mut *t.add(i);
                    if s.key == e.person {
                        s.count += 1;
                        break s.li;
                    }
                    if s.key == 0 {
                        if np * 10 >= cap * 7 {
                            full = true;
                            break 'all;
                        }
                        *s = Slot { key: e.person, li: np as u32, count: 1 };
                        *ps.add(np) = i as u32;
                        np += 1;
                        break np as u32 - 1;
                    }
                    i = (i + 1) & mask;
                };
                let c = cur.add((e.ts >> BIN) as usize);
                let p = *c as usize;
                *c += 1;
                *tmp.add(p) = (e.ts as u64) << 32 | li as u64;
                if e.pay != 0 {
                    *pu.add(npu) = [li, e.ts, e.pay];
                    npu += 1;
                }
            }
            if !full {
                self.tmp.set_len(n);
            }
            self.persons.set_len(np);
            self.purch.set_len(npu);
            full
        };
        if full {
            for &s in &self.persons {
                self.table[s as usize] = Slot::default();
            }
            return None;
        }
        Some(np)
    }

    /// The campaign cells, the user_daily row count and the encoded user_daily file.
    pub fn finish(self) -> (Vec<Cell>, usize, UserFile) {
        let assisted = self.out.assisted.iter().copied().chain(std::iter::repeat(0));
        let cells = self.out.cells.iter().zip(assisted).map(|(c, a)| Cell { sessions: c.sessions, purchasing: c.purchasing, assisted: a, revenue: c.revenue }).collect();
        let r = self.out.rows;
        let mut enc = self.enc;
        let [idl, day, sess, ev, rev, defb, topl] = &mut enc;
        parquet::delta(r.idlen.as_slice(), idl);
        parquet::delta(r.day.as_slice(), day);
        parquet::delta(r.sessions.as_slice(), sess);
        parquet::delta(r.events.as_slice(), ev);
        parquet::delta(r.revenue.as_slice(), rev);
        parquet::delta(r.toplen.as_slice(), topl);
        let def = parquet::def_levels(r.present.as_slice(), r.n);
        defb.clear();
        defb.reserve(def.len());
        defb.extend(&def);
        (cells, r.n, UserFile { rows: r, enc })
    }
}

/// A worker's user_daily table, encoded: the delta bodies of the integer and length columns, the string bytes.
pub struct UserFile {
    rows: Rows,
    enc: [Buf<u8>; 7],
}

impl UserFile {
    pub fn write(&self, path: &Path) {
        let (r, [idl, day, sess, ev, rev, def, topl]) = (&self.rows, &self.enc);
        let cols = [
            Column { name: "user_id", kind: Kind::Str, enc: Enc::Delta, optional: false, parts: &[idl.as_slice(), r.idbytes.as_slice()] },
            Column { name: "day", kind: Kind::Date, enc: Enc::Delta, optional: false, parts: &[day.as_slice()] },
            Column { name: "sessions", kind: Kind::Int32, enc: Enc::Delta, optional: false, parts: &[sess.as_slice()] },
            Column { name: "events", kind: Kind::Int32, enc: Enc::Delta, optional: false, parts: &[ev.as_slice()] },
            Column { name: "revenue_cents", kind: Kind::Int64, enc: Enc::Delta, optional: false, parts: &[rev.as_slice()] },
            Column { name: "top_category", kind: Kind::Str, enc: Enc::Delta, optional: true, parts: &[def.as_slice(), topl.as_slice(), r.topbytes.as_slice()] },
        ];
        parquet::write(path, r.n, &cols).unwrap();
    }
}

fn prefix(counts: &[u32], offs: &mut Vec<u32>) {
    offs.clear();
    offs.push(0);
    let mut acc = 0;
    for &c in counts {
        acc += c;
        offs.push(acc);
    }
}

/// One person's clicks `[ts << 32 | campaign, click_id]`, in bin order: each run of one bin is sorted by ts
/// ascending then click_id descending, so the last click at or before a session start is its credit.
fn order_bins(cks: &mut [[u64; 2]]) {

    let mut i = 0;
    while i < cks.len() {
        let bin = (cks[i][0] >> (32 + BIN)) as u32;
        let mut j = i + 1;
        while j < cks.len() && (cks[j][0] >> (32 + BIN)) as u32 == bin {
            j += 1;
        }
        if j - i > 1 {
            cks[i..j].sort_unstable_by(|a, b| (a[0] >> 32).cmp(&(b[0] >> 32)).then_with(|| key::cmp(b[1], a[1])));
        }
        i = j;
    }
}

/// One person: event timestamps grouped by bin in bin order (unordered within a bin), purchases `(ts, pay)` in ts order, and clicks
/// `[ts << 32 | campaign, click_id]` sorted as above. Events in one bin are under 30 minutes apart, so they
/// share a session; a session boundary falls between two consecutive non-empty bins iff the gap from the
/// earlier bin's last event to the later bin's first is over 30 minutes.
fn person(ctx: &Ctx, o: &mut Out, pk: Key, evs: &[u32], purch: &[[u32; 2]], cks: &[[u64; 2]]) {
    o.sessions.clear();
    o.person += 1;
    o.buf.clear();
    key::decode(pk, &mut o.buf);
    o.id = [0; 16];
    for (d, &s) in o.id.iter_mut().zip(&o.buf) {
        *d = s;
    }
    let (mut day, mut sessions, mut events, mut rev) = (i32::MIN, 0u32, 0u32, 0i64);
    let mut prev_hi = 0u32;

    let (mut i, mut pi) = (0, 0);
    while i < evs.len() {
        let bin = evs[i] >> BIN;
        let (mut lo, mut hi, mut count, mut brev) = (u32::MAX, 0u32, 0u32, 0i64);
        while i < evs.len() && evs[i] >> BIN == bin {
            let t = evs[i];
            lo = lo.min(t);
            hi = hi.max(t);
            count += 1;
            i += 1;
        }
        // The bin's purchases, applied once the session, so the day, is known.
        let p0 = pi;
        while pi < purch.len() && purch[pi][0] >> BIN == bin {
            brev += (purch[pi][1] & 0xff_ffff) as i64;
            pi += 1;
        }
        if o.sessions.is_empty() || lo - prev_hi > GAP as u32 {
            let d = (ctx.cws + lo as i64).div_euclid(DAY) as i32;
            if d != day {
                flush(ctx, o, day, sessions, events, rev);
                (day, sessions, events, rev) = (d, 0, 0, 0);
            }
            sessions += 1;
            let di = (d as i64 - ctx.day0) as u32;
            o.sessions.push(Sess { start: lo, day: di, rev: 0 });
        }
        events += count;
        rev += brev;
        o.sessions.last_mut().unwrap().rev += brev as u64;
        for &[_, pay] in &purch[p0..pi] {
            let (price, cat) = ((pay & 0xff_ffff) as u64, (pay >> 24) as usize);
            if o.cat_rev[cat] == 0 {
                o.touched.push(cat as u32);
            }
            o.cat_rev[cat] += price;
        }
        prev_hi = hi;
    }
    flush(ctx, o, day, sessions, events, rev);
    attribute(ctx, o, cks);
}

/// Emits a user_daily row. The top category is the highest revenue, ties to the smallest index (= byte order).
fn flush(ctx: &Ctx, o: &mut Out, day: i32, sessions: u32, events: u32, rev: i64) {
    if sessions == 0 {
        return;
    }
    let (mut top, mut best) = (NONE, 0u64);
    for &c in &o.touched {
        let r = o.cat_rev[c as usize];
        if r > best || (r == best && c < top) {
            (best, top) = (r, c);
        }
    }
    for &c in &o.touched {
        o.cat_rev[c as usize] = 0;
    }
    o.touched.clear();
    let r = &mut o.rows;
    r.idlen.push(o.buf.len() as i32);
    if o.buf.len() <= 16 {
        put16(&mut r.idbytes, o.id, o.buf.len());
    } else {
        r.idbytes.reserve(o.buf.len());
        r.idbytes.extend(&o.buf);
    }
    r.day.push(day);
    r.sessions.push(sessions as i32);
    r.events.push(events as i32);
    r.revenue.push(rev);
    if r.n % 8 == 0 {
        r.present.push(0);
    }
    *r.present.last_mut() |= ((top != NONE) as u8) << (r.n % 8);
    if top != NONE {
        let c = &ctx.cats[top as usize];
        r.toplen.push(c.len() as i32);
        if c.len() <= 16 {
            put16(&mut r.topbytes, o.cat16[top as usize], c.len());
        } else {
            r.topbytes.reserve(c.len());
            r.topbytes.extend(c);
        }
    }
    r.n += 1;
}

/// Appends `len <= 16` bytes from a padded copy with one store (room for 16 was reserved).
#[inline(always)]
fn put16(out: &mut Buf<u8>, v: [u8; 16], len: usize) {
    out.put16(v, len);
}

/// Credit each session to the latest click in [start - 7d, start] (ties: smallest click_id, which sorts last),
/// else organic. A click at `t` assists every session starting in [t, t + 7d]; with the clicks in `t` order
/// both ends of that session range only move up, so each click adds the sessions past what its campaign's
/// earlier clicks covered, counted per day through `day_first`.
fn attribute(ctx: &Ctx, o: &mut Out, cks: &[[u64; 2]]) {
    let ts = |c: &[u64; 2]| c[0] >> 32;
    let camp = |c: &[u64; 2]| c[0] as u32 as usize;
    let (ndays, ncamp, week) = (ctx.ndays, ctx.ncamp, WEEK as u64);
    let (n, m) = (o.sessions.len(), cks.len());
    let pid = o.person << 32;
    if n * m <= SMALL {
        // Nearly every person: every (session, click) pair tested without a branch. `covered` stamps the
        // campaigns already counted for the session.
        for (i, s) in o.sessions.iter().enumerate() {
            let (start, day, sid) = (s.start as u64, s.day as usize, pid | i as u64);
            let mut best = m;
            for (j, c) in cks.iter().enumerate() {
                let (t, cm) = (ts(c), camp(c));
                let q = t <= start && t + week >= start;
                best = if q { j } else { best };
                let fresh = o.covered[cm] != sid;
                o.covered[cm] = if q { sid } else { o.covered[cm] };
                o.assisted[cm * ndays + day] += (q & fresh) as u32;
            }
            let credit = if best < m { camp(&cks[best]) } else { ncamp };
            let cell = &mut o.cells[credit * ndays + day];
            cell.sessions += 1;
            cell.purchasing += (s.rev > 0) as u32;
            cell.revenue += s.rev as i64;
        }
        return;
    }
    o.day_first.clear();
    for (i, s) in o.sessions.iter().enumerate() {
        while o.day_first.len() <= s.day as usize {
            o.day_first.push(i as u32);
        }
    }
    while o.day_first.len() <= ndays {
        o.day_first.push(n as u32);
    }
    let (mut lo, mut hi) = (0, 0);
    for s in &o.sessions {
        let start = s.start as u64;
        while hi < cks.len() && ts(&cks[hi]) <= start {
            hi += 1;
        }
        while lo < hi && ts(&cks[lo]) + week < start {
            lo += 1;
        }
        let credit = if lo < hi { camp(&cks[hi - 1]) } else { ncamp };
        let cell = &mut o.cells[credit * ndays + s.day as usize];
        cell.sessions += 1;
        cell.purchasing += (s.rev > 0) as u32;
        cell.revenue += s.rev as i64;
    }
    let (mut p_lo, mut p_hi) = (0, 0);
    for c in cks {
        let (t, camp) = (ts(c), camp(c));
        while p_lo < n && (o.sessions[p_lo].start as u64) < t {
            p_lo += 1;
        }
        while p_hi < n && (o.sessions[p_hi].start as u64) <= t + week {
            p_hi += 1;
        }
        let v = o.covered[camp];
        let done = if v >> 32 == o.person { v as u32 as usize } else { 0 };
        let mut i = p_lo.max(done);
        if i < p_hi {
            o.covered[camp] = pid | p_hi as u64;
            let mut d = o.sessions[i].day as usize;
            while i < p_hi {
                let end = p_hi.min(o.day_first[d + 1] as usize);
                o.assisted[camp * ndays + d] += (end - i) as u32;
                (i, d) = (end, d + 1);
            }
        }
    }
}
