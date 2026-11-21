//! Linked IDs: a lock-free union-find over packed keys, each person named by its byte-wise smallest ID. A
//! union always hangs the root with the larger key under the root with the smaller one, so every parent chain
//! strictly decreases (no cycles, whatever the interleaving) and the root of a tree is the person's name. The
//! result is a read-only table from linked ID to name in 64-byte lines of 4 IDs then their 4 names: a lookup
//! reads one line, which callers prefetch ahead of time, compares the 4 IDs at once and takes the name without
//! a branch; only a full line sends it on to the next.
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

use crate::key::{self, Key};
use crate::mem::{self, map_huge};
use crate::parallel;

/// A table line: IDs (0 = empty) in `[0..4]`, their names in `[4..8]`.
type Line = [u64; 8];

pub struct Links {
    table: &'static [Line], // Huge pages: lookups are random and prefetched.
    mask: usize,            // line index mask
    shift: u32,             // 64 - line index bits
}

/// The home line of a key: the top bits of a Fibonacci multiply (two instructions; computed once to prefetch
/// and once to look up).
#[inline(always)]
fn home(k: Key, shift: u32) -> usize {
    (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> shift) as usize
}

/// A zeroed, huge-page-backed slice that lives until exit.
fn huge_slice<T>(n: usize) -> &'static mut [T] {
    unsafe { std::slice::from_raw_parts_mut(map_huge(n * std::mem::size_of::<T>()) as *mut T, n) }
}

/// Faults a slice's pages in (written, in bulk) where huge pages would not; see `mem::populate`.
fn populate<T>(s: &[T]) {
    mem::populate(s.as_ptr() as *mut u8, std::mem::size_of_val(s));
}

fn release<T>(s: &[T]) {
    mem::release(s.as_ptr() as *mut u8, std::mem::size_of_val(s));
}

#[inline(always)]
fn prefetch(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(p as *const i8)
    };
}

impl Links {
    pub fn build(edges: &[Vec<(Key, Key)>], threads: usize) -> Links {
        let n: usize = edges.iter().map(|e| e.len()).sum();
        let cap = (n * 4).next_power_of_two().max(16); // slots: at most 2 IDs per edge, at most half full
        let lines = cap / 4;
        let mask = lines - 1;
        let shift = 64 - lines.trailing_zeros();
        // The IDs go straight into the final table's lines (as atomics while the threads insert); the names
        // half is filled at the end. Slot s = line s / 4, entry s % 4.
        let table: &[[AtomicU64; 8]] = huge_slice(lines);
        let parent: &[AtomicU32] = huge_slice(cap);
        let chunk = cap.div_ceil(threads).max(1);
        let span = |t: usize| (t * chunk).min(cap)..((t + 1) * chunk).min(cap); // empty for spare threads
        parallel(threads, |t| {
            populate(&table[span(t).start / 4..span(t).end.div_ceil(4)]);
            populate(&parent[span(t)]);
            for i in span(t) {
                parent[i].store(i as u32, Relaxed);
            }
        });
        let key_at = |s: u32| table[s as usize / 4][s as usize % 4].load(Relaxed);
        // The slot of `k`: the first entry of its home line (or the next ones) that takes it or already holds
        // it. A key moves past a line only when all 4 entries hold other keys, and occupancy only grows, so a
        // lookup that stops at the first line with an empty entry sees every key.
        let slot = |k: Key| -> u32 {
            let mut line = home(k, shift);
            loop {
                for (j, e) in table[line][..4].iter().enumerate() {
                    match e.compare_exchange(0, k, Relaxed, Relaxed) {
                        Ok(_) => return (line * 4 + j) as u32,
                        Err(cur) if cur == k => return (line * 4 + j) as u32,
                        Err(_) => {}
                    }
                }
                line = (line + 1) & mask;
            }
        };
        let find = |mut x: u32| -> u32 {
            loop {
                let p = parent[x as usize].load(Relaxed);
                if p == x {
                    return x;
                }
                let gp = parent[p as usize].load(Relaxed);
                if gp != p {
                    let _ = parent[x as usize].compare_exchange_weak(p, gp, Relaxed, Relaxed); // path halving
                }
                x = gp;
            }
        };
        // Edges go through in batches of 16: the key lines of the next batch and the parent lines of this
        // one are prefetched, so the table's cache misses overlap instead of stalling one by one.
        parallel(threads.min(edges.len().max(1)), |t| {
            let batches: Vec<&[(Key, Key)]> = edges[t].chunks(16).collect();
            let mut slots = [(0u32, 0u32); 16];
            for (i, batch) in batches.iter().enumerate() {
                if let Some(next) = batches.get(i + 1) {
                    for &(a, b) in *next {
                        prefetch(table[home(a, shift)].as_ptr() as *const u8);
                        prefetch(table[home(b, shift)].as_ptr() as *const u8);
                    }
                }
                for (j, &(a, b)) in batch.iter().enumerate() {
                    let (sa, sb) = (slot(a), slot(b));
                    prefetch(parent[sa as usize].as_ptr() as *const u8);
                    prefetch(parent[sb as usize].as_ptr() as *const u8);
                    slots[j] = (sa, sb);
                }
                for &(sa, sb) in &slots[..batch.len()] {
                    loop {
                        let (ra, rb) = (find(sa), find(sb));
                        if ra == rb {
                            break;
                        }
                        let (ka, kb) = (key_at(ra), key_at(rb));
                        let (lo, hi) = if key::cmp(ka, kb).is_lt() { (ra, rb) } else { (rb, ra) };
                        if parent[hi as usize].compare_exchange(hi, lo, Relaxed, Relaxed).is_ok() {
                            break;
                        }
                    }
                }
            }
        });
        // Names: each thread fills the lines of its chunk (the keys and parents it reads are final now).
        let lchunk = lines.div_ceil(threads).max(1);
        parallel(threads, |t| {
            for line in t * lchunk..((t + 1) * lchunk).min(lines) {
                for j in 0..4 {
                    let s = (line * 4 + j) as u32;
                    if key_at(s) != 0 {
                        table[line][4 + j].store(key_at(find(s)), Relaxed);
                    }
                }
            }
        });
        // Every thread's writes are complete (joined); the atomics are read as plain words from here on.
        let table: &'static [Line] = unsafe { std::slice::from_raw_parts(table.as_ptr() as *const Line, lines) };
        // The union-find is dead now: its pages go back on all threads rather than at exit on one.
        parallel(threads, |t| release(&parent[span(t)]));
        Links { table, mask, shift }
    }

    /// Gives the table's pages back; the table must not be used afterwards.
    pub fn release(&self) {
        release(self.table);
    }

    #[inline(always)]
    pub fn prefetch(&self, k: Key) {
        prefetch(self.table[home(k, self.shift)].as_ptr() as *const u8);
    }

    /// The person's name: the root of a linked ID, or the ID itself. Whether an ID is linked is a coin toss
    /// per line, so the usual line is resolved without a branch on it.
    #[inline(always)]
    pub fn lookup(&self, k: Key) -> Key {
        let mut line = home(k, self.shift);
        loop {
            let l = &self.table[line];
            let (hit, empty) = matches(l, k);
            // Entry 0..=3 of a hit, else any entry: the name is taken either way and chosen afterwards.
            let name = l[4 + ((hit | 16).trailing_zeros() as usize & 3)];
            if hit | empty != 0 {
                return std::hint::select_unpredictable(hit != 0, name, k);
            }
            line = (line + 1) & self.mask;
        }
    }
}

/// Bit j set where entry j of the line holds `k`, and where it is empty.
#[inline(always)]
fn matches(l: &Line, k: Key) -> (u32, u32) {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let keys = _mm256_loadu_si256(l.as_ptr() as *const __m256i);
        let m = |v: __m256i| _mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpeq_epi64(keys, v))) as u32;
        (m(_mm256_set1_epi64x(k as i64)), m(_mm256_setzero_si256()))
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let (mut hit, mut empty) = (0, 0);
        for j in 0..4 {
            hit |= ((l[j] == k) as u32) << j;
            empty |= ((l[j] == 0) as u32) << j;
        }
        (hit, empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serial_names(edges: &[(Key, Key)]) -> Vec<(Key, Key)> {
        let mut ids: Vec<Key> = edges.iter().flat_map(|&(a, b)| [a, b]).collect();
        ids.sort_unstable();
        ids.dedup();
        let mut root: Vec<usize> = (0..ids.len()).collect();
        let at = |k: Key| ids.binary_search(&k).unwrap();
        fn f(r: &mut [usize], mut x: usize) -> usize {
            while r[x] != x {
                r[x] = r[r[x]];
                x = r[x];
            }
            x
        }
        for &(a, b) in edges {
            let (ra, rb) = (f(&mut root, at(a)), f(&mut root, at(b)));
            root[ra] = rb;
        }
        let mut comp: Vec<(usize, Key)> = (0..ids.len()).map(|i| (f(&mut root, i), ids[i])).collect();
        comp.sort_by(|a, b| a.0.cmp(&b.0).then(key::cmp(a.1, b.1)));
        let mut best = std::collections::HashMap::new();
        for &(r, k) in &comp {
            best.entry(r).or_insert(k);
        }
        (0..ids.len()).map(|i| (ids[i], best[&f(&mut root, i)])).collect()
    }

    #[test]
    fn chains_and_names() {
        let p = |s: &str| key::pack(s.as_bytes());
        // A 400-link chain in shuffled order, a self-link, a repeated link, a tab-named ID (interned).
        let mut edges: Vec<(Key, Key)> = (0..400).map(|i| (p(&format!("u{i}")), p(&format!("u{}", i + 1)))).collect();
        edges.reverse();
        edges.push((p("u7"), p("u7")));
        edges.push((p("u3"), p("u2")));
        edges.push((p("A\tB"), p("u100")));
        let links = Links::build(&[edges], 8);
        for i in 0..=400 {
            assert_eq!(links.lookup(p(&format!("u{i}"))), p("A\tB"));
        }
        assert_eq!(links.lookup(p("zzz")), p("zzz"));
        assert_eq!(links.lookup(0), 0);
        let links = Links::build(&[vec![(p("u3"), p("u9")), (p("u9"), p("u2"))], vec![(p("U7"), p("u9"))]], 2);
        assert_eq!(links.lookup(p("u2")), p("U7"));
        assert_eq!(links.lookup(p("u3")), p("U7"));
    }

    #[test]
    fn parallel_matches_serial() {
        let mut rng = 0xdead_beef_cafe_f00du64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for round in 0..20 {
            let ids = 2000 + round * 500;
            let mut all: Vec<(Key, Key)> = Vec::new();
            // Random edges, a long chain, a star, duplicates both ways round.
            for _ in 0..ids {
                all.push((key::pack(format!("u{}", next() % ids).as_bytes()), key::pack(format!("u{}", next() % ids).as_bytes())));
            }
            for i in 0..600 {
                all.push((key::pack(format!("c{i}").as_bytes()), key::pack(format!("c{}", i + 1).as_bytes())));
            }
            for i in 0..3000 {
                all.push((key::pack(format!("s{i}").as_bytes()), key::pack(b"hub")));
            }
            let want = serial_names(&all);
            for threads in [1, 3, 8] {
                let per: Vec<Vec<(Key, Key)>> = (0..threads).map(|t| all.iter().skip(t).step_by(threads).copied().collect()).collect();
                let links = Links::build(&per, threads);
                for &(id, name) in &want {
                    assert_eq!(links.lookup(id), name, "threads {threads}");
                }
                // IDs that are not linked name themselves, through full and partly filled lines alike.
                for i in 0..5000 {
                    let k = key::pack(format!("x{i}").as_bytes());
                    assert_eq!(links.lookup(k), k);
                }
            }
        }
    }
}
