//! One person partition at a time: a local person index, counting sorts of events and clicks by person, then per
//! person the sessions (gap > 30 min), the user_daily rows, the credited click and the assisting campaigns.
use std::path::Path;

use crate::key::{self, Key, mix};
use crate::parquet::{self, Column, Kind};
use crate::{Ck, Ctx, DAY, Ev2, GAP, WEEK};

const NONE: u32 = u32::MAX;

/// Per (campaign, day) totals; `organic` is campaign index `ncamp`.
#[derive(Clone, Copy, Default)]
pub struct Cell {
    pub sessions: u32,
    pub purchasing: u32,
    pub assisted: u32,
    pub revenue: i64,
}

/// user_daily rows as PLAIN column bodies, ready for the writer.
#[derive(Default)]
pub struct Rows {
    pub n: usize,
    ids: Vec<u8>,
    day: Vec<i32>,
    sessions: Vec<i32>,
    events: Vec<i32>,
    revenue: Vec<i64>,
    present: Vec<bool>,
    tops: Vec<u8>,
}

struct Sess {
    start: i64,
    rev: u64,
}

/// Campaigns with at least one click in the sliding window, with O(1) add and remove.
struct Window {
    count: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
}

impl Window {
    fn add(&mut self, c: u32) {
        if self.count[c as usize] == 0 {
            self.pos[c as usize] = self.active.len() as u32;
            self.active.push(c);
        }
        self.count[c as usize] += 1;
    }
    fn remove(&mut self, c: u32) {
        self.count[c as usize] -= 1;
        if self.count[c as usize] == 0 {
            let i = self.pos[c as usize] as usize;
            self.active.swap_remove(i);
            if let Some(&moved) = self.active.get(i) {
                self.pos[moved as usize] = i as u32;
            }
        }
    }
}

/// The per-person state and the outputs.
struct Out {
    sessions: Vec<Sess>,
    cat_rev: Vec<u64>,
    touched: Vec<u32>,
    win: Window,
    buf: Vec<u8>,
    rows: Rows,
    cells: Vec<Cell>,
}

pub struct Worker<'a> {
    ctx: &'a Ctx,
    keys: Vec<u64>,
    vals: Vec<u32>,
    lis: Vec<u32>,
    persons: Vec<Key>,
    counts: Vec<u32>,
    offs: Vec<u32>,
    cursor: Vec<u32>,
    sorted: Vec<u64>,
    ccounts: Vec<u32>,
    coffs: Vec<u32>,
    csorted: Vec<[u64; 2]>,
    out: Out,
}

impl<'a> Worker<'a> {
    pub fn new(ctx: &'a Ctx) -> Self {
        Worker {
            ctx,
            keys: Vec::new(),
            vals: Vec::new(),
            lis: Vec::new(),
            persons: Vec::new(),
            counts: Vec::new(),
            offs: Vec::new(),
            cursor: Vec::new(),
            sorted: Vec::new(),
            ccounts: Vec::new(),
            coffs: Vec::new(),
            csorted: Vec::new(),
            out: Out {
                sessions: Vec::new(),
                cat_rev: vec![0; ctx.cats.len()],
                touched: Vec::new(),
                win: Window { count: vec![0; ctx.ncamp], pos: vec![0; ctx.ncamp], active: Vec::new() },
                buf: Vec::new(),
                rows: Rows::default(),
                cells: vec![Cell::default(); (ctx.ncamp + 1) * ctx.ndays],
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
        // Local index: person key -> dense local id, built from the events; clicks only look up.
        let cap = (n * 2).next_power_of_two().max(16);
        let (mask, shift) = (cap - 1, 64 - cap.trailing_zeros());
        let bits = self.ctx.bits;
        self.keys.clear();
        self.keys.resize(cap, 0);
        self.vals.resize(cap, 0);
        self.persons.clear();
        self.counts.clear();
        self.lis.clear();
        for e in eblocks.iter().flat_map(|b| b.iter()) {
            let mut i = ((mix(e.person) << bits) >> shift) as usize;
            while self.keys[i] != 0 && self.keys[i] != e.person {
                i = (i + 1) & mask;
            }
            if self.keys[i] == 0 {
                self.keys[i] = e.person;
                self.vals[i] = self.persons.len() as u32;
                self.persons.push(e.person);
                self.counts.push(0);
            }
            let li = self.vals[i];
            self.counts[li as usize] += 1;
            self.lis.push(li);
        }
        let np = self.persons.len();
        prefix(&self.counts, &mut self.offs);
        self.cursor.clear();
        self.cursor.extend_from_slice(&self.offs[..np]);
        self.sorted.clear();
        self.sorted.resize(n, 0);
        for (e, &li) in eblocks.iter().flat_map(|b| b.iter()).zip(&self.lis) {
            let c = &mut self.cursor[li as usize];
            self.sorted[*c as usize] = (e.ts as u64) << 32 | e.pay as u64;
            *c += 1;
        }
        // Clicks of people with no events are dropped: they can credit nothing.
        self.ccounts.clear();
        self.ccounts.resize(np, 0);
        self.lis.clear();
        for c in cblocks.iter().flat_map(|b| b.iter()) {
            let mut i = ((mix(c.person) << bits) >> shift) as usize;
            while self.keys[i] != 0 && self.keys[i] != c.person {
                i = (i + 1) & mask;
            }
            let li = if self.keys[i] == c.person { self.vals[i] } else { NONE };
            if li != NONE {
                self.ccounts[li as usize] += 1;
            }
            self.lis.push(li);
        }
        prefix(&self.ccounts, &mut self.coffs);
        self.cursor.clear();
        self.cursor.extend_from_slice(&self.coffs[..np]);
        self.csorted.clear();
        self.csorted.resize(self.coffs[np] as usize, [0; 2]);
        for (c, &li) in cblocks.iter().flat_map(|b| b.iter()).zip(&self.lis) {
            if li != NONE {
                let k = &mut self.cursor[li as usize];
                self.csorted[*k as usize] = [(c.ts as u64) << 32 | c.camp as u64, c.cid];
                *k += 1;
            }
        }
        for li in 0..np {
            let evs = &mut self.sorted[self.offs[li] as usize..self.offs[li + 1] as usize];
            evs.sort_unstable();
            // ts ascending, then click_id descending: the last click at or before a start is the credit.
            let cks = &mut self.csorted[self.coffs[li] as usize..self.coffs[li + 1] as usize];
            cks.sort_unstable_by(|a, b| (a[0] >> 32).cmp(&(b[0] >> 32)).then_with(|| key::cmp(b[1], a[1])));
            person(self.ctx, &mut self.out, self.persons[li], evs, cks);
        }
    }

    pub fn finish(self, user_file: &Path) -> (Vec<Cell>, usize) {
        let r = &self.out.rows;
        let mut top = parquet::def_levels(&r.present);
        top.extend_from_slice(&r.tops);
        let cols = [
            Column { name: "user_id", kind: Kind::Str, optional: false, body: &r.ids },
            Column { name: "day", kind: Kind::Date, optional: false, body: parquet::plain(&r.day) },
            Column { name: "sessions", kind: Kind::Int32, optional: false, body: parquet::plain(&r.sessions) },
            Column { name: "events", kind: Kind::Int32, optional: false, body: parquet::plain(&r.events) },
            Column { name: "revenue_cents", kind: Kind::Int64, optional: false, body: parquet::plain(&r.revenue) },
            Column { name: "top_category", kind: Kind::Str, optional: true, body: &top },
        ];
        parquet::write(user_file, r.n, &cols).unwrap();
        (self.out.cells, r.n)
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

/// One person: ts-sorted events `ts << 32 | pay`, clicks `[ts << 32 | campaign, click_id]` sorted as above.
fn person(ctx: &Ctx, o: &mut Out, pk: Key, evs: &[u64], cks: &[[u64; 2]]) {
    o.sessions.clear();
    let (mut day, mut sessions, mut events, mut rev) = (i32::MIN, 0u32, 0u32, 0i64);
    let mut last = i64::MIN;
    for &e in evs {
        let t = ctx.cws + (e >> 32) as i64;
        if o.sessions.is_empty() || t - last > GAP {
            let d = t.div_euclid(DAY) as i32;
            if d != day {
                flush(ctx, o, pk, day, sessions, events, rev);
                (day, sessions, events, rev) = (d, 0, 0, 0);
            }
            sessions += 1;
            o.sessions.push(Sess { start: t, rev: 0 });
        }
        last = t;
        events += 1;
        let pay = e as u32;
        if pay != 0 {
            let (price, cat) = ((pay & 0xff_ffff) as u64, (pay >> 24) as usize);
            rev += price as i64;
            if o.cat_rev[cat] == 0 {
                o.touched.push(cat as u32);
            }
            o.cat_rev[cat] += price;
            o.sessions.last_mut().unwrap().rev += price;
        }
    }
    flush(ctx, o, pk, day, sessions, events, rev);
    attribute(ctx, o, cks);
}

/// Emits a user_daily row. The top category is the highest revenue, ties to the smallest index (= byte order).
fn flush(ctx: &Ctx, o: &mut Out, pk: Key, day: i32, sessions: u32, events: u32, rev: i64) {
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
    o.buf.clear();
    key::decode(pk, &mut o.buf);
    let r = &mut o.rows;
    parquet::put_str(&mut r.ids, &o.buf);
    r.day.push(day);
    r.sessions.push(sessions as i32);
    r.events.push(events as i32);
    r.revenue.push(rev);
    r.present.push(top != NONE);
    if top != NONE {
        parquet::put_str(&mut r.tops, &ctx.cats[top as usize]);
    }
    r.n += 1;
}

/// Credit each session to the latest click in [start - 7d, start] (ties: smallest click_id, which sorts last),
/// else organic; every campaign with a click in that range assists once.
fn attribute(ctx: &Ctx, o: &mut Out, cks: &[[u64; 2]]) {
    let ts = |c: &[u64; 2]| ctx.cws + (c[0] >> 32) as i64;
    let camp = |c: &[u64; 2]| c[0] as u32;
    let (ndays, ncamp) = (ctx.ndays, ctx.ncamp);
    let (mut lo, mut hi) = (0, 0);
    for s in &o.sessions {
        while hi < cks.len() && ts(&cks[hi]) <= s.start {
            o.win.add(camp(&cks[hi]));
            hi += 1;
        }
        while lo < hi && ts(&cks[lo]) < s.start - WEEK {
            o.win.remove(camp(&cks[lo]));
            lo += 1;
        }
        let day = (s.start.div_euclid(DAY) - ctx.day0) as usize;
        let credit = if lo < hi { camp(&cks[hi - 1]) as usize } else { ncamp };
        let cell = &mut o.cells[credit * ndays + day];
        cell.sessions += 1;
        cell.purchasing += (s.rev > 0) as u32;
        cell.revenue += s.rev as i64;
        for &c in &o.win.active {
            o.cells[c as usize * ndays + day].assisted += 1;
        }
    }
    while lo < hi {
        o.win.remove(camp(&cks[lo]));
        lo += 1;
    }
}
