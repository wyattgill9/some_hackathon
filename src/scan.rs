//! The CSV index: the positions of every `\n`, `,` and `"` in a buffer, found 64 bytes at a time with AVX2.
//! The reader walks it line by line; sentinels at the end keep every lookahead in bounds.

#[derive(Default)]
pub struct Index {
    pub delims: Vec<u32>, // commas and newlines, in order
    qmask: Vec<u64>,   // per 64-byte block: the quote bits
    qprefix: Vec<u32>, // per block: quotes before it
    nmask: Vec<u64>,   // the same for newlines
    nprefix: Vec<u32>,
}

pub const SENTINELS: usize = 16;

impl Index {
    /// Indexes `buf[..n]`; `buf` must extend at least 64 bytes past `n`.
    pub fn build(&mut self, buf: &[u8], n: usize) {
        assert!(buf.len() >= n + 64);
        // Sized once per range and written through pointers: no capacity check per block.
        let blocks = n.div_ceil(64);
        self.delims.clear();
        self.delims.reserve(n + 64 + SENTINELS); // every byte a delimiter, at most
        for v in [&mut self.qmask, &mut self.nmask] {
            v.clear();
            v.reserve(blocks + 1);
        }
        for v in [&mut self.qprefix, &mut self.nprefix] {
            v.clear();
            v.reserve(blocks + 1);
        }
        let dp = self.delims.as_mut_ptr();
        let (qm, qp, nm, np) = (self.qmask.as_mut_ptr(), self.qprefix.as_mut_ptr(), self.nmask.as_mut_ptr(), self.nprefix.as_mut_ptr());
        let (mut len, mut quotes, mut newlines) = (0usize, 0u32, 0u32);
        // In bounds: blocks + 1 entries were reserved for each per-block array, n + 64 + SENTINELS for delims.
        unsafe {
            for b in 0..blocks {
                let o = b * 64;
                let (d, l, q) = masks(&buf[o..o + 64]);
                let valid = if n - o >= 64 { u64::MAX } else { (1u64 << (n - o)) - 1 };
                len = flatten::<10>(d & valid, o, dp, len);
                qm.add(b).write(q & valid);
                qp.add(b).write(quotes);
                quotes += (q & valid).count_ones();
                nm.add(b).write(l & valid);
                np.add(b).write(newlines);
                newlines += (l & valid).count_ones();
            }
            // So the counts are defined at position n when n is a multiple of 64.
            qm.add(blocks).write(0);
            qp.add(blocks).write(quotes);
            nm.add(blocks).write(0);
            np.add(blocks).write(newlines);
            for i in 0..SENTINELS {
                dp.add(len + i).write(u32::MAX);
            }
            self.delims.set_len(len + SENTINELS);
            self.qmask.set_len(blocks + 1);
            self.qprefix.set_len(blocks + 1);
            self.nmask.set_len(blocks + 1);
            self.nprefix.set_len(blocks + 1);
        }
    }

    /// The number of quotes in `buf[..pos]`, for `pos` within the indexed data.
    #[inline(always)]
    pub fn quotes_before(&self, pos: usize) -> u32 {
        // In bounds: pos <= n, and both arrays hold an entry per 64-byte block of the data plus one.
        unsafe {
            let (b, i) = (pos >> 6, pos & 63);
            *self.qprefix.get_unchecked(b) + (*self.qmask.get_unchecked(b) & ((1u64 << i) - 1)).count_ones()
        }
    }

    /// The number of newlines in `buf[..pos]`.
    #[inline(always)]
    pub fn newlines_before(&self, pos: usize) -> u32 {
        unsafe {
            let (b, i) = (pos >> 6, pos & 63);
            *self.nprefix.get_unchecked(b) + (*self.nmask.get_unchecked(b) & ((1u64 << i) - 1)).count_ones()
        }
    }
}

/// Writes the position of every set bit at `p[base..]` and returns the new length. The first N slots are
/// written unconditionally (a cleared mask yields harmless values past the end), so the usual block costs no
/// data-dependent branch.
///
/// # Safety
/// `p[base..base + 64]` must be writable.
#[inline(always)]
unsafe fn flatten<const N: usize>(mut m: u64, o: usize, p: *mut u32, base: usize) -> usize {
    let cnt = m.count_ones() as usize;
    unsafe {
        for i in 0..N {
            p.add(base + i).write((o + m.trailing_zeros() as usize) as u32);
            m &= m.wrapping_sub(1);
        }
        if cnt > N {
            let mut i = N;
            while m != 0 {
                p.add(base + i).write((o + m.trailing_zeros() as usize) as u32);
                m &= m - 1;
                i += 1;
            }
        }
    }
    base + cnt
}

/// Bit i set where block[i] is a delimiter (`,` or `\n`), a newline, a quote.
#[inline]
fn masks(block: &[u8]) -> (u64, u64, u64) {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let p = block.as_ptr();
        let (a, b) = (_mm256_loadu_si256(p as *const __m256i), _mm256_loadu_si256(p.add(32) as *const __m256i));
        let m = |c: u8| {
            let v = _mm256_set1_epi8(c as i8);
            (_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64)
                | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64) << 32)
        };
        let (nl, comma, q) = (m(b'\n'), m(b','), m(b'"'));
        (nl | comma, nl, q)
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let (mut d, mut l, mut q) = (0u64, 0u64, 0u64);
        for (i, &c) in block[..64].iter().enumerate() {
            d |= ((c == b',' || c == b'\n') as u64) << i;
            l |= ((c == b'\n') as u64) << i;
            q |= ((c == b'"') as u64) << i;
        }
        (d, l, q)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions() {
        let mut data: Vec<u8> = b"a,b\n\"q\",,\n".repeat(37);
        data.push(b'x');
        let n = data.len();
        data.resize(n + 64, b','); // readable tail that must not be indexed
        let mut idx = Index::default();
        idx.build(&data, n);
        let want = |c: u8| -> Vec<u32> { (0..n).filter(|&i| data[i] == c).map(|i| i as u32).collect() };
        for pos in 0..=n {
            assert_eq!(idx.quotes_before(pos) as usize, data[..pos].iter().filter(|&&c| c == b'"').count(), "{pos}");
            assert_eq!(idx.newlines_before(pos) as usize, data[..pos].iter().filter(|&&c| c == b'\n').count(), "{pos}");
        }
        let mut d: Vec<u32> = want(b',');
        d.extend(want(b'\n'));
        d.sort_unstable();
        assert_eq!(&idx.delims[..idx.delims.len() - SENTINELS], d);
    }
}
