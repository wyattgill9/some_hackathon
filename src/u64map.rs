//! An open-addressing map from non-zero `u64` keys to `u32` values: linear probing, power-of-two capacity,
//! at most half full. Key 0 (the empty ID) is the empty-slot marker and is never stored.
use crate::key::mix;

pub struct U64Map {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
    len: usize,
}

impl U64Map {
    pub fn new(expected: usize) -> Self {
        let cap = (expected * 2).next_power_of_two().max(16);
        // Written pages, not calloc: lookups read every slot, and reading a calloc page before writing it
        // maps the zero page, which the insert then copies (and flushes TLBs for).
        U64Map { keys: crate::mem::zeroed(cap), vals: crate::mem::zeroed(cap), mask: cap - 1, len: 0 }
    }

    #[inline]
    fn slot(&self, k: u64) -> usize {
        let mut i = (mix(k) >> 32) as usize & self.mask;
        loop {
            let s = self.keys[i];
            if s == k || s == 0 {
                return i;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline]
    pub fn get(&self, k: u64) -> Option<u32> {
        if k == 0 {
            return None;
        }
        let i = self.slot(k);
        (self.keys[i] == k).then(|| self.vals[i])
    }

    pub fn insert(&mut self, k: u64, v: u32) {
        assert!(k != 0);
        if self.len * 2 >= self.keys.len() {
            self.grow();
        }
        let i = self.slot(k);
        if self.keys[i] == 0 {
            self.keys[i] = k;
            self.len += 1;
        }
        self.vals[i] = v;
    }

    fn grow(&mut self) {
        let cap = self.keys.len() * 2;
        let old_keys = std::mem::replace(&mut self.keys, vec![0; cap]);
        let old_vals = std::mem::replace(&mut self.vals, vec![0; cap]);
        self.mask = self.keys.len() - 1;
        for (k, v) in old_keys.into_iter().zip(old_vals) {
            if k != 0 {
                let i = self.slot(k);
                self.keys[i] = k;
                self.vals[i] = v;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_and_finds() {
        let mut m = U64Map::new(1);
        for i in 1..5000u64 {
            m.insert(i * 0x9E37_79B9, i as u32);
        }
        for i in 1..5000u64 {
            assert_eq!(m.get(i * 0x9E37_79B9), Some(i as u32));
        }
        assert_eq!(m.get(7), None);
        assert_eq!(m.get(0), None);
    }
}
