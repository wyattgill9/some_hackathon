//! Shopping Sessions: reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/part-0.parquet.
//! rules.md is the spec; the comments name the rule each step implements.
mod parse;

use std::fs;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU32, Ordering};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use foldhash::HashMap;
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use parquet::arrow::ArrowWriter;
use rayon::prelude::*;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const CHUNK: usize = 8 << 20;
const NULL_CAT: u32 = u32::MAX;

/// A parsed event line, before dedup. `keep` = the copy is valid and not late.
struct Ev<'a> {
    id: &'a [u8],
    user: &'a [u8],
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u32,
    keep: bool,
}

/// A surviving event: `key` = person << 32 | (ts - window_start). `price` > 0 means purchase.
#[derive(Clone, Copy)]
struct Sv {
    key: u64,
    price: u32,
    cat: u32,
}

/// A valid click: `key` = person << 32 | (ts - (window_start - 7d)).
struct Ck<'a> {
    key: u64,
    camp: u32,
    id: &'a [u8],
}

struct Sess {
    start: i64,
    rev: u64,
}

struct UserRow {
    person: u32,
    day: i32,
    sessions: i64,
    events: i64,
    rev: i64,
    top: u32,
}

/// Per-thread results: user_daily rows, and [sessions, purchasing, revenue, assisted] per (campaign, day).
struct Acc {
    users: Vec<UserRow>,
    camp: Vec<[i64; 4]>,
}

fn main() {
    let t0 = Instant::now();
    let lap = |what: &str| eprintln!("{:>8.3}s {what}", t0.elapsed().as_secs_f64());
    if let Some(n) = std::env::var("NCPU").ok().and_then(|n| n.parse().ok()) {
        rayon::ThreadPoolBuilder::new().num_threads(n).build_global().unwrap();
    }
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));

    let manifest = fs::read(data.join("manifest.json")).unwrap();
    let (ws, we) = (manifest_ts(&manifest, "window_start"), manifest_ts(&manifest, "window_end"));
    // Relative timestamps are packed into 32 bits below.
    assert!(we - ws + WEEK <= u32::MAX as i64, "window too long");
    let (day0, ndays) = (ws.div_euclid(DAY), ((we - 1).div_euclid(DAY) - ws.div_euclid(DAY) + 1) as usize);

    // Products: category index, with categories (plus `unknown`) sorted so index order is byte order.
    let products_buf = fs::read(data.join("products.csv")).unwrap();
    let products: Vec<[&[u8]; 2]> = lines(body(&products_buf)).filter_map(parse::fields::<2>).collect();
    let mut cats: Vec<&[u8]> = products.iter().map(|[_, c]| *c).chain([&b"unknown"[..]]).collect();
    cats.sort_unstable();
    cats.dedup();
    let unknown = cats.binary_search(&&b"unknown"[..]).unwrap() as u32;
    let cat_of: HashMap<&[u8], u32> =
        products.iter().map(|[p, c]| (*p, cats.binary_search(c).unwrap() as u32)).collect();

    // Campaigns: index, id, channel. `organic` is index ncamp.
    let campaigns_buf = fs::read(data.join("campaigns.csv")).unwrap();
    let campaigns: Vec<[&[u8]; 2]> = lines(body(&campaigns_buf)).filter_map(parse::fields::<2>).collect();
    let camp_of: HashMap<&[u8], u32> = campaigns.iter().enumerate().map(|(i, [c, _])| (*c, i as u32)).collect();
    let ncamp = campaigns.len();

    let links_buf = read_csvs(&data.join("links"));
    let events_buf = read_csvs(&data.join("events"));
    let clicks_buf = read_csvs(&data.join("clicks"));
    lap("read");

    // Links: union-find over interned IDs; each person is named by its byte-wise smallest ID.
    let mut person_of: HashMap<&[u8], u32> = HashMap::default();
    let mut names: Vec<&[u8]> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    for c in chunks(&links_buf) {
        for [a, b] in lines(c).filter_map(parse::fields::<2>) {
            if a.is_empty() || b.is_empty() {
                continue;
            }
            let [a, b] = [a, b].map(|id| {
                *person_of.entry(id).or_insert_with(|| {
                    names.push(id);
                    parent.push(parent.len() as u32);
                    parent.len() as u32 - 1
                })
            });
            let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    for i in 0..parent.len() {
        let r = find(&mut parent, i as u32) as usize;
        if names[i] < names[r] {
            names[r] = names[i];
        }
    }
    for p in person_of.values_mut() {
        *p = parent[*p as usize];
    }
    lap("links");

    // Events 1: parse, and flag each copy valid-and-not-late up front (it is only acted on if kept).
    let evs: Vec<Ev> = chunks(&events_buf)
        .par_iter()
        .flat_map_iter(|c| lines(c).filter_map(|l| parse_event(l, ws, we, &cat_of, unknown)))
        .collect();
    assert!(evs.len() < u32::MAX as usize);
    lap("events parse");

    // Events 2: dedup on exact event_id, keeping the smallest ingest_ts, then drop kept copies that fail.
    let mut by_hash: Vec<(u64, u32)> = evs.par_iter().enumerate().map(|(i, e)| (hash(e.id), i as u32)).collect();
    by_hash.par_sort_unstable();
    let kept: Vec<u32> = by_hash
        .par_chunk_by(|a, b| a.0 == b.0)
        .flat_map_iter(|run| {
            let mut best: Vec<u32> = Vec::new(); // one per distinct id in this hash run (almost always 1)
            for &(_, i) in run {
                match best.iter_mut().find(|b| evs[**b as usize].id == evs[i as usize].id) {
                    Some(b) if evs[i as usize].ingest < evs[*b as usize].ingest => *b = i,
                    Some(_) => {}
                    None => best.push(i),
                }
            }
            best.into_iter().filter(|&i| evs[i as usize].keep)
        })
        .collect();
    drop(by_hash);
    lap("events dedup");

    // People: group surviving events by user (hash, then exact bytes) so each distinct user is resolved once.
    // A user_id not in any link is a person of its own, with a fresh id.
    let mut by_user: Vec<(u64, u32)> = kept.par_iter().map(|&i| (hash(evs[i as usize].user), i)).collect();
    drop(kept);
    by_user.par_sort_unstable();
    let next = AtomicU32::new(names.len() as u32);
    let (mut sv, new_ids): (Vec<Sv>, Vec<(u32, &[u8])>) = by_user
        .par_chunk_by(|a, b| a.0 == b.0)
        .fold(
            || (Vec::new(), Vec::new()),
            |(mut sv, mut new_ids): (Vec<Sv>, Vec<(u32, &[u8])>), run| {
                let mut seen: Vec<(&[u8], u32)> = Vec::new(); // distinct users in this hash run (almost always 1)
                for &(_, i) in run {
                    let e = &evs[i as usize];
                    let p = match seen.iter().find(|s| s.0 == e.user) {
                        Some(s) => s.1,
                        None => {
                            let p = person_of.get(e.user).copied().unwrap_or_else(|| {
                                let p = next.fetch_add(1, Ordering::Relaxed);
                                new_ids.push((p, e.user));
                                p
                            });
                            seen.push((e.user, p));
                            p
                        }
                    };
                    sv.push(Sv { key: (p as u64) << 32 | (e.ts - ws) as u64, price: e.price, cat: e.cat });
                }
                (sv, new_ids)
            },
        )
        .reduce(
            || (Vec::new(), Vec::new()),
            |(mut a, mut an), (b, bn)| {
                a.extend(b);
                an.extend(bn);
                (a, an)
            },
        );
    drop(by_user);
    drop(evs);
    names.resize(next.into_inner() as usize, &[]);
    for (p, u) in new_ids {
        names[p as usize] = u;
        person_of.insert(u, p);
    }
    sv.par_sort_unstable_by_key(|s| s.key);
    lap("events sort");

    // Clicks: parse, validate, resolve the person. Dedup is skipped on purpose (ponytail): copies are identical,
    // so they can't change the latest click or the set of assisting campaigns. A click whose ID is in no link
    // and no surviving event belongs to a person with no sessions, so it is dropped.
    let cws = ws - WEEK;
    let mut cks: Vec<Ck> = chunks(&clicks_buf)
        .par_iter()
        .flat_map_iter(|c| {
            lines(c).filter_map(|l| {
                let [id, user, camp, ts] = parse::fields::<4>(l)?;
                let ts = parse::ts(ts).filter(|t| (cws..we).contains(t))?;
                if id.is_empty() || user.is_empty() {
                    return None;
                }
                let camp = *camp_of.get(camp)?;
                let p = *person_of.get(user)?;
                Some(Ck { key: (p as u64) << 32 | (ts - cws) as u64, camp, id })
            })
        })
        .collect();
    cks.par_sort_unstable_by(|a, b| a.key.cmp(&b.key).then(a.id.cmp(b.id)));
    lap("clicks");

    // Per person: sessionize, user_daily rows, then credit and assists for each session.
    let ncat = cats.len();
    let acc = sv
        .par_chunk_by(|a, b| a.key >> 32 == b.key >> 32)
        .fold(
            || (Acc { users: Vec::new(), camp: vec![[0; 4]; (ncamp + 1) * ndays] }, Scratch::new(ncat, ncamp)),
            |(mut acc, mut s), evs| {
                let person = (evs[0].key >> 32) as u32;
                person_pass(person, evs, ws, &mut acc, &mut s);
                let lo = cks.partition_point(|c| ((c.key >> 32) as u32) < person);
                let hi = lo + cks[lo..].partition_point(|c| ((c.key >> 32) as u32) == person);
                attribute(&s.sessions, &cks[lo..hi], cws, day0, ndays, ncamp, &mut acc, &mut s.win);
                (acc, s)
            },
        )
        .map(|(acc, _)| acc)
        .reduce_with(|mut a, b| {
            a.users.extend(b.users);
            a.camp.iter_mut().zip(&b.camp).for_each(|(x, y)| (0..4).for_each(|k| x[k] += y[k]));
            a
        })
        .unwrap_or(Acc { users: Vec::new(), camp: vec![[0; 4]; (ncamp + 1) * ndays] });
    lap("sessions + attribution");

    // Output.
    let s = |b: &[u8]| std::str::from_utf8(b).unwrap().to_owned();
    let u = &acc.users;
    write(
        &out.join("user_daily"),
        vec![
            ("user_id", Arc::new(StringArray::from_iter_values(u.iter().map(|r| s(names[r.person as usize]))))),
            ("day", Arc::new(Date32Array::from_iter_values(u.iter().map(|r| r.day)))),
            ("sessions", Arc::new(Int64Array::from_iter_values(u.iter().map(|r| r.sessions)))),
            ("events", Arc::new(Int64Array::from_iter_values(u.iter().map(|r| r.events)))),
            ("revenue_cents", Arc::new(Int64Array::from_iter_values(u.iter().map(|r| r.rev)))),
            (
                "top_category",
                Arc::new(StringArray::from_iter(u.iter().map(|r| (r.top != NULL_CAT).then(|| s(cats[r.top as usize]))))),
            ),
        ],
    );
    let cells: Vec<(usize, usize, [i64; 4])> = (0..=ncamp)
        .flat_map(|c| (0..ndays).map(move |d| (c, d)))
        .map(|(c, d)| (c, d, acc.camp[c * ndays + d]))
        .filter(|(_, _, v)| v[0] > 0 || v[3] > 0)
        .collect();
    let id = |c: usize| if c == ncamp { "organic".to_owned() } else { s(campaigns[c][0]) };
    let chan = |c: usize| if c == ncamp { "organic".to_owned() } else { s(campaigns[c][1]) };
    let col = |k: usize| Arc::new(Int64Array::from_iter_values(cells.iter().map(|v| v.2[k]))) as ArrayRef;
    write(
        &out.join("campaign_daily"),
        vec![
            ("campaign_id", Arc::new(StringArray::from_iter_values(cells.iter().map(|v| id(v.0))))),
            ("channel", Arc::new(StringArray::from_iter_values(cells.iter().map(|v| chan(v.0))))),
            ("day", Arc::new(Date32Array::from_iter_values(cells.iter().map(|v| (day0 + v.1 as i64) as i32)))),
            ("sessions", col(0)),
            ("purchasing_sessions", col(1)),
            ("revenue_cents", col(2)),
            ("assisted_sessions", col(3)),
        ],
    );
    let (us, ur) = u.iter().fold((0, 0), |(a, b), r| (a + r.sessions, b + r.rev));
    let (cs, cr) = cells.iter().fold((0, 0), |(a, b), v| (a + v.2[0], b + v.2[2]));
    lap(&format!("write: {} user rows, {} campaign rows; sessions {us}/{cs}, revenue {ur}/{cr}", u.len(), cells.len()));
}

fn parse_event<'a>(l: &'a [u8], ws: i64, we: i64, cat_of: &HashMap<&[u8], u32>, unknown: u32) -> Option<Ev<'a>> {
    let [id, user, ts, ingest, kind, product, price] = parse::fields::<7>(l)?;
    if id.is_empty() {
        return None;
    }
    // ponytail: rules.md guarantees ingest_ts is valid F1; a bad one would sort last and fail the late check.
    let ingest = parse::ts(ingest).unwrap_or(i64::MAX);
    let ts = parse::ts(ts).filter(|t| (ws..we).contains(t));
    let mut keep = !user.is_empty()
        && !product.is_empty()
        && matches!(kind, b"view" | b"cart" | b"purchase")
        && ts.is_some_and(|t| ingest - t <= DAY);
    let (mut p, mut cat) = (0, 0);
    if keep && kind == b"purchase" {
        match parse::price(price) {
            Some(v) => (p, cat) = (v, *cat_of.get(product).unwrap_or(&unknown)),
            None => keep = false,
        }
    }
    Some(Ev { id, user, ingest, ts: ts.unwrap_or(0), price: p, cat, keep })
}

/// Reusable per-thread buffers.
struct Scratch {
    sessions: Vec<Sess>,
    cat_rev: Vec<u64>,
    win: Window,
}

impl Scratch {
    fn new(ncat: usize, ncamp: usize) -> Self {
        Scratch {
            sessions: Vec::new(),
            cat_rev: vec![0; ncat],
            win: Window { count: vec![0; ncamp], pos: vec![0; ncamp], active: Vec::new() },
        }
    }
}

/// Sessionize one person's ts-sorted events (gap > 30 min starts a session) and emit their user_daily rows.
fn person_pass(person: u32, evs: &[Sv], ws: i64, acc: &mut Acc, s: &mut Scratch) {
    s.sessions.clear();
    let mut row = UserRow { person, day: i32::MIN, sessions: 0, events: 0, rev: 0, top: NULL_CAT };
    let mut last = i64::MIN;
    for e in evs {
        let t = ws + (e.key & 0xffff_ffff) as i64;
        if s.sessions.is_empty() || t - last > GAP {
            let day = t.div_euclid(DAY) as i32;
            if day != row.day {
                flush_day(&mut row, &mut s.cat_rev, acc);
                row.day = day;
            }
            row.sessions += 1;
            s.sessions.push(Sess { start: t, rev: 0 });
        }
        last = t;
        row.events += 1;
        if e.price > 0 {
            row.rev += e.price as i64;
            s.cat_rev[e.cat as usize] += e.price as u64;
            s.sessions.last_mut().unwrap().rev += e.price as u64;
        }
    }
    flush_day(&mut row, &mut s.cat_rev, acc);
}

fn flush_day(row: &mut UserRow, cat_rev: &mut [u64], acc: &mut Acc) {
    if row.sessions > 0 {
        // Highest category revenue; strict `>` keeps the smallest index (= byte-wise smallest name) on a tie.
        let mut top = NULL_CAT;
        for (i, &r) in cat_rev.iter().enumerate() {
            if r > 0 && (top == NULL_CAT || r > cat_rev[top as usize]) {
                top = i as u32;
            }
        }
        acc.users.push(UserRow { top, ..*row });
        cat_rev.fill(0);
    }
    (row.sessions, row.events, row.rev) = (0, 0, 0);
}

/// Campaigns with at least one click in the sliding window, with O(1) add/remove.
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

/// Credit each session to the latest click in [start - 7d, start] (tie: smallest click_id, which sorts first
/// among equal ts), else organic; every campaign with a click in that range assists once.
#[allow(clippy::too_many_arguments)]
fn attribute(sessions: &[Sess], cks: &[Ck], cws: i64, day0: i64, ndays: usize, ncamp: usize, acc: &mut Acc, w: &mut Window) {
    let ts = |c: &Ck| cws + (c.key & 0xffff_ffff) as i64;
    let (mut lo, mut hi) = (0, 0);
    for s in sessions {
        while hi < cks.len() && ts(&cks[hi]) <= s.start {
            w.add(cks[hi].camp);
            hi += 1;
        }
        while lo < hi && ts(&cks[lo]) < s.start - WEEK {
            w.remove(cks[lo].camp);
            lo += 1;
        }
        let day = (s.start.div_euclid(DAY) - day0) as usize;
        let credit = if lo < hi {
            let latest = ts(&cks[hi - 1]);
            let first = lo + cks[lo..hi].partition_point(|c| ts(c) < latest);
            cks[first].camp as usize
        } else {
            ncamp
        };
        let cell = &mut acc.camp[credit * ndays + day];
        cell[0] += 1;
        cell[1] += (s.rev > 0) as i64;
        cell[2] += s.rev as i64;
        for &c in &w.active {
            acc.camp[c as usize * ndays + day][3] += 1;
        }
    }
    while lo < hi {
        w.remove(cks[lo].camp);
        lo += 1;
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        parent[x as usize] = parent[parent[x as usize] as usize];
        x = parent[x as usize];
    }
    x
}

fn hash(b: &[u8]) -> u64 {
    foldhash::fast::FixedState::default().hash_one(b)
}

fn manifest_ts(m: &[u8], key: &str) -> i64 {
    let key = format!("\"{key}\"");
    let at = m.windows(key.len()).position(|w| w == key.as_bytes()).unwrap() + key.len();
    let v = &m[at..];
    let v = &v[v.iter().position(|&b| b == b'"').unwrap() + 1..];
    parse::ts(&v[..v.iter().position(|&b| b == b'"').unwrap()]).unwrap()
}

/// Every `*.csv` in `dir`, read in parallel.
fn read_csvs(dir: &Path) -> Vec<Vec<u8>> {
    let paths: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "csv"))
        .collect();
    paths.par_iter().map(|p| fs::read(p).unwrap()).collect()
}

/// A file without its header line (only the first line; a later header-equal row is data).
fn body(f: &[u8]) -> &[u8] {
    f.iter().position(|&b| b == b'\n').map_or(&[], |i| &f[i + 1..])
}

/// Every file's body cut into ~CHUNK pieces at line boundaries.
fn chunks(files: &[Vec<u8>]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    for f in files {
        let mut rest = body(f);
        while rest.len() > CHUNK {
            let cut = rest[CHUNK..].iter().position(|&b| b == b'\n').map_or(rest.len(), |i| CHUNK + i + 1);
            out.push(&rest[..cut]);
            rest = &rest[cut..];
        }
        if !rest.is_empty() {
            out.push(rest);
        }
    }
    out
}

/// Lines split on `\n` only; a final `\n` does not start another (empty) line.
fn lines(c: &[u8]) -> impl Iterator<Item = &[u8]> {
    c.strip_suffix(b"\n").unwrap_or(c).split(|&b| b == b'\n').filter(|_| !c.is_empty())
}

fn write(dir: &Path, cols: Vec<(&str, ArrayRef)>) {
    fs::create_dir_all(dir).unwrap();
    let batch = RecordBatch::try_from_iter(cols).unwrap();
    let mut w = ArrowWriter::try_new(fs::File::create(dir.join("part-0.parquet")).unwrap(), batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}
