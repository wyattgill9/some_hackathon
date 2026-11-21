//! Linked IDs: a lock-free union-find over packed keys, each person named by its byte-wise smallest ID. A
//! union always hangs the root with the larger key under the root with the smaller one, so every parent chain
//! strictly decreases (no cycles, whatever the interleaving) and the root of a tree is the person's name. The
//! result is a read-only open-addressing table from linked ID to name, 16 bytes per slot so a lookup costs
//! one cache line, which callers prefetch ahead of time.
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

use crate::key::{self, Key, mix};
use crate::parallel;
use crate::pool::map_huge;

pub struct Links {
    table: &'static [[Key; 2]], // (id, name); id 0 = empty. Huge pages: lookups are random and prefetched.
    mask: usize,
}

/// A zeroed, huge-page-backed slice that lives until exit.
fn huge_slice<T>(n: usize) -> &'static mut [T] {
    unsafe { std::slice::from_raw_parts_mut(map_huge(n * std::mem::size_of::<T>()) as *mut T, n) }
}

impl Links {
    pub fn build(edges: &[Vec<(Key, Key)>], threads: usize) -> Links {
        let n: usize = edges.iter().map(|e| e.len()).sum();
        let cap = (n * 4).next_power_of_two().max(16); // at most 2 IDs per edge, at most half full
        let mask = cap - 1;
        let keys: &[AtomicU64] = huge_slice(cap); // zero = empty
        let parent: &[AtomicU32] = huge_slice(cap);
        let chunk = cap.div_ceil(threads).max(1);
        parallel(threads, |t| {
            for i in t * chunk..((t + 1) * chunk).min(cap) {
                parent[i].store(i as u32, Relaxed);
            }
        });
        let slot = |k: Key| -> usize {
            let mut s = (mix(k) >> 32) as usize & mask;
            loop {
                match keys[s].compare_exchange(0, k, Relaxed, Relaxed) {
                    Ok(_) => return s,
                    Err(cur) if cur == k => return s,
                    Err(_) => s = (s + 1) & mask,
                }
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
        parallel(threads.min(edges.len().max(1)), |t| {
            for &(a, b) in &edges[t] {
                let (sa, sb) = (slot(a) as u32, slot(b) as u32);
                loop {
                    let (ra, rb) = (find(sa), find(sb));
                    if ra == rb {
                        break;
                    }
                    let (ka, kb) = (keys[ra as usize].load(Relaxed), keys[rb as usize].load(Relaxed));
                    let (lo, hi) = if key::cmp(ka, kb).is_lt() { (ra, rb) } else { (rb, ra) };
                    if parent[hi as usize].compare_exchange(hi, lo, Relaxed, Relaxed).is_ok() {
                        break;
                    }
                }
            }
        });
        let table: &'static mut [[Key; 2]] = huge_slice(cap);
        std::thread::scope(|s| {
            for (c, out) in table.chunks_mut(chunk).enumerate() {
                let (keys, find) = (&keys, &find);
                s.spawn(move || {
                    for (i, o) in out.iter_mut().enumerate() {
                        let k = keys[c * chunk + i].load(Relaxed);
                        if k != 0 {
                            *o = [k, keys[find((c * chunk + i) as u32) as usize].load(Relaxed)];
                        }
                    }
                });
            }
        });
        Links { table, mask }
    }

    #[inline]
    pub fn prefetch(&self, k: Key) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let s = (mix(k) >> 32) as usize & self.mask;
            _mm_prefetch::<_MM_HINT_T0>(self.table.as_ptr().add(s) as *const i8);
        }
    }

    /// The person's name: the root of a linked ID, or the ID itself.
    #[inline]
    pub fn lookup(&self, k: Key) -> Key {
        if k == 0 {
            return 0;
        }
        let mut s = (mix(k) >> 32) as usize & self.mask;
        // Whether the ID is linked is a coin toss per line: the first slot is resolved without a branch on it.
        let [id, name] = self.table[s];
        if id == k || id == 0 {
            return std::hint::select_unpredictable(id == k, name, k);
        }
        loop {
            s = (s + 1) & self.mask;
            let [id, name] = self.table[s];
            if id == k {
                return name;
            }
            if id == 0 {
                return k;
            }
        }
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
            }
        }
    }
}
