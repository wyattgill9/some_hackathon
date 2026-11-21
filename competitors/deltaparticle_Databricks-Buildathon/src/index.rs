//! Two-stage CSV parsing, stage 1: one SIMD pass over a batch of whole lines
//! records where every field starts and every line ends, and flags quoting
//! errors, without branching on the bytes.
//!
//! Per 64-byte block, with separator mask S (comma or newline) and quote
//! mask Q:
//! - `P`, the bytes right after a separator, are where fields begin;
//! - a quote at a field's beginning opens it (`OQ`); every other quote must be
//!   followed by a separator (it closes its field);
//! - an opening quote must not be followed by a separator (`"` alone);
//! - the separator that ends a field has a quote right before it exactly when
//!   the field opened with one. Adding each opening quote bit to `!S` carries it
//!   along the field's bytes onto the field's ending separator, in one addition.
//!
//! A field's recorded start skips its opening quote. Stage 2 (in main.rs) then
//! walks the lines with these positions.

use crate::csv::masks;

pub struct Index {
    /// Field starts, in order (content start: past an opening quote).
    pub st: Vec<u32>,
    /// Newline positions, in order.
    pub nl: Vec<u32>,
    /// Positions of quoting errors, in order.
    pub bad: Vec<u32>,
}

/// Sentinel entries after the real ones, so stage 2 can always look ahead.
pub const PAD: usize = 16;

impl Index {
    pub fn new() -> Self {
        Self { st: Vec::new(), nl: Vec::new(), bad: Vec::new() }
    }

    /// Index `t` (which starts at a line start). Positions are offsets into `t`.
    pub fn build(&mut self, t: &[u8]) {
        let len = t.len();
        let nb = len.div_ceil(64);
        self.st.clear();
        self.nl.clear();
        self.bad.clear();
        self.st.reserve(nb * 64 + 2 * PAD);
        self.nl.reserve(nb * 64 + 2 * PAD);
        let st0 = self.st.as_mut_ptr();
        let nl0 = self.nl.as_mut_ptr();
        let mut st = st0;
        let mut nl = nl0;
        let (mut prev_s, mut c_oq, mut c_qn, mut carry) = (1u64, 0u64, 0u64, false);
        let mut tail = [0u8; 64];
        for b in 0..nb {
            let base = b * 64;
            let block: &[u8; 64] = if base + 64 <= len {
                t[base..base + 64].try_into().unwrap()
            } else {
                tail[..len - base].copy_from_slice(&t[base..]);
                &tail
            };
            let (n, c, q) = masks(block);
            let s = c | n;
            let p = (s << 1) | prev_s;
            let oq = q & p;
            let qn = q & !p;
            let u = (oq << 1) | c_oq;
            let tt = (qn << 1) | c_qn;
            let (sum1, k1) = (!s).overflowing_add(oq);
            let (sum, k2) = sum1.overflowing_add(carry as u64);
            let badm = (tt & !s) | (u & s) | ((sum ^ (tt | u)) & s);
            let bm = (p & !q) | u;
            carry = k1 | k2;
            prev_s = s >> 63;
            c_oq = oq >> 63;
            c_qn = qn >> 63;
            let base32 = base as u32;
            unsafe {
                // Field starts: eight unconditional writes cover most blocks.
                let cnt = bm.count_ones() as usize;
                let mut m = bm;
                for i in 0..8 {
                    *st.add(i) = base32 + m.trailing_zeros();
                    m &= m.wrapping_sub(1);
                }
                if cnt > 8 {
                    for i in 8..cnt {
                        *st.add(i) = base32 + m.trailing_zeros();
                        m &= m.wrapping_sub(1);
                    }
                }
                st = st.add(cnt);
                let cnt = n.count_ones() as usize;
                let mut m = n;
                for i in 0..2 {
                    *nl.add(i) = base32 + m.trailing_zeros();
                    m &= m.wrapping_sub(1);
                }
                if cnt > 2 {
                    for i in 2..cnt {
                        *nl.add(i) = base32 + m.trailing_zeros();
                        m &= m.wrapping_sub(1);
                    }
                }
                nl = nl.add(cnt);
            }
            if badm != 0 {
                let mut m = badm;
                while m != 0 {
                    self.bad.push(base32 + m.trailing_zeros());
                    m &= m - 1;
                }
            }
        }
        unsafe {
            let ns = st.offset_from(st0) as usize;
            let nn = nl.offset_from(nl0) as usize;
            for i in 0..PAD {
                *st.add(i) = u32::MAX;
                *nl.add(i) = u32::MAX;
            }
            self.st.set_len(ns + PAD);
            self.nl.set_len(nn + PAD);
        }
        self.bad.extend(std::iter::repeat_n(u32::MAX, PAD));
    }

    /// Number of real newlines.
    pub fn lines(&self) -> usize {
        self.nl.len() - PAD
    }
}

/// Bytes indexed per batch (a batch is then extended to the next newline).
const BATCH: usize = 64 << 10;

/// Call `f` with the `N` unquoted fields of every well-formed line of `data`,
/// in order: the same lines and fields as `csv::for_lines` + `csv::split`.
/// Regular lines come straight from the index; others (wrong field count,
/// quoting errors, a last line without `\n`) go through `csv::split`.
#[inline(always)]
pub fn scan_indexed<'a, const N: usize>(data: &'a [u8], idx: &mut Index, mut f: impl FnMut(&[&'a [u8]; N])) {
    let mut pos = 0usize;
    while pos < data.len() {
        let mut end = (pos + BATCH).min(data.len());
        if end < data.len() {
            end = match memchr::memchr(b'\n', &data[end..]) {
                Some(i) => end + i + 1,
                None => data.len(),
            };
        }
        let t = &data[pos..end];
        idx.build(t);
        let (st, nlv, bad) = (&idx.st, &idx.nl, &idx.bad);
        let (mut sp, mut bp, mut ls) = (0usize, 0usize, 0usize);
        let quoted = |a: u32| -> u32 { (a > 0 && t[a as usize - 1] == b'"') as u32 };
        for i in 0..idx.lines() {
            let nl = nlv[i];
            if nl as usize > ls && st[sp + N - 1] <= nl && st[sp + N] > nl && bad[bp] > nl {
                let mut out: [&'a [u8]; N] = [&[]; N];
                let mut q = quoted(st[sp]);
                for k in 0..N {
                    let a = st[sp + k];
                    let (e, qn) = if k + 1 < N {
                        let qn = quoted(st[sp + k + 1]);
                        (st[sp + k + 1] - 1 - qn, qn)
                    } else {
                        (nl, 0)
                    };
                    out[k] = &t[a as usize..(e - q) as usize];
                    q = qn;
                }
                f(&out);
                sp += N;
            } else {
                let mut out: [&'a [u8]; N] = [&[]; N];
                if crate::csv::split(&t[ls..nl as usize], &mut out) {
                    f(&out);
                }
                while st[sp] <= nl {
                    sp += 1;
                }
                while bad[bp] <= nl {
                    bp += 1;
                }
            }
            ls = nl as usize + 1;
        }
        if ls < t.len() {
            let mut out: [&'a [u8]; N] = [&[]; N];
            if crate::csv::split(&t[ls..], &mut out) {
                f(&out);
            }
        }
        pos = end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The indexed scan must agree with for_lines + split on arbitrary input.
    #[test]
    fn indexed_matches_split() {
        let alphabet = b"ab,,,\"\"\"\n\r 1x";
        let mut state = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut idx = Index::new();
        for round in 0..40000 {
            let n = if round % 100 == 0 { 3000 } else { (next() % 400) as usize };
            let data: Vec<u8> = (0..n).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            for_n::<3>(&data, &mut idx);
            for_n::<1>(&data, &mut idx);
            for_n::<7>(&data, &mut idx);
        }
        // Long realistic lines crossing many blocks and batches.
        let mut big = Vec::new();
        for i in 0..200000u32 {
            let q = |s: &str, k: u32| if (i + k) % 5 == 0 { format!("\"{s}\"") } else { s.to_string() };
            big.extend_from_slice(format!("{},{},{},x,{}\n", q("e1", 1), q("u22", 2), q("2026", 3), q("", 4)).as_bytes());
            if i % 977 == 0 {
                big.extend_from_slice(b"bad\"line,1,2,3,4\n\n");
            }
        }
        for_n::<5>(&big, &mut idx);
    }

    fn for_n<const N: usize>(data: &[u8], idx: &mut Index) {
        let mut a: Vec<Vec<Vec<u8>>> = Vec::new();
        crate::csv::for_lines(data, |line| {
            let mut o = [&b""[..]; N];
            if crate::csv::split::<N>(line, &mut o) {
                a.push(o.iter().map(|x| x.to_vec()).collect());
            }
        });
        let mut b: Vec<Vec<Vec<u8>>> = Vec::new();
        scan_indexed::<N>(data, idx, |o| b.push(o.iter().map(|x| x.to_vec()).collect()));
        assert_eq!(a, b, "N={N} {:?}", String::from_utf8_lossy(data));
    }
}
