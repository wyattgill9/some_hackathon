//! Linked IDs: union-find over packed keys, each person named by its byte-wise smallest ID. The union always
//! hangs the root with the larger key under the root with the smaller one, so every chain strictly decreases
//! and the root of a tree is the person's name. The result is a read-only map from linked ID to name.
use crate::key::{self, Key};
use crate::u64map::U64Map;

pub struct Links {
    slot_of: U64Map,
    names: Vec<Key>,
}

impl Links {
    // ponytail: serial union-find, ~0.2 s at 20M events; a CAS-parallel version is the step-4 upgrade.
    pub fn build(edges: &[Vec<(Key, Key)>]) -> Links {
        let n: usize = edges.iter().map(|e| e.len()).sum();
        let mut slot_of = U64Map::new(n * 2);
        let mut names: Vec<Key> = Vec::with_capacity(n * 2);
        let mut parent: Vec<u32> = Vec::with_capacity(n * 2);
        for &(a, b) in edges.iter().flatten() {
            let [a, b] = [a, b].map(|k| {
                slot_of.get_or_insert_with(k, || {
                    names.push(k);
                    parent.push(parent.len() as u32);
                    parent.len() as u32 - 1
                })
            });
            let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
            if ra == rb {
                continue;
            }
            let (lo, hi) = if key::cmp(names[ra as usize], names[rb as usize]).is_lt() { (ra, rb) } else { (rb, ra) };
            parent[hi as usize] = lo;
        }
        for i in 0..parent.len() {
            let r = find(&mut parent, i as u32);
            names[i] = names[r as usize];
        }
        Links { slot_of, names }
    }

    /// The person's name: the root of a linked ID, or the ID itself.
    #[inline]
    pub fn lookup(&self, k: Key) -> Key {
        match self.slot_of.get(k) {
            Some(s) => self.names[s as usize],
            None => k,
        }
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        parent[x as usize] = parent[parent[x as usize] as usize];
        x = parent[x as usize];
    }
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chains_and_names() {
        let p = |s: &str| key::pack(s.as_bytes());
        // A 400-link chain in shuffled order, a self-link, a repeated link, a tab-named ID (interned).
        let mut edges: Vec<(Key, Key)> = (0..400).map(|i| (p(&format!("u{i}")), p(&format!("u{}", i + 1)))).collect();
        edges.reverse();
        edges.push((p("u7"), p("u7")));
        edges.push((p("u3"), p("u2")));
        edges.push((p("A\tB"), p("u100")));
        let links = Links::build(&[edges]);
        for i in 0..=400 {
            assert_eq!(links.lookup(p(&format!("u{i}"))), p("A\tB"));
        }
        assert_eq!(links.lookup(p("zzz")), p("zzz"));
        let links = Links::build(&[vec![(p("u3"), p("u9")), (p("u9"), p("u2"))], vec![(p("U7"), p("u9"))]]);
        assert_eq!(links.lookup(p("u2")), p("U7"));
        assert_eq!(links.lookup(p("u3")), p("U7"));
    }
}
