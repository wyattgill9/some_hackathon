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
        self.delims.clear();
        self.delims.reserve(n / 6 + SENTINELS);
        for v in [&mut self.qmask, &mut self.nmask] {
            v.clear();
            v.reserve(n / 64 + 2);
        }
        for v in [&mut self.qprefix, &mut self.nprefix] {
            v.clear();
            v.reserve(n / 64 + 2);
        }
        let (mut o, mut quotes, mut newlines) = (0, 0u32, 0u32);
        while o < n {
            let (d, l, q) = masks(&buf[o..o + 64]);
            let valid = if n - o >= 64 { u64::MAX } else { (1u64 << (n - o)) - 1 };
            flatten::<10>(d & valid, o, &mut self.delims);
            self.qmask.push(q & valid);
            self.qprefix.push(quotes);
            quotes += (q & valid).count_ones();
            self.nmask.push(l & valid);
            self.nprefix.push(newlines);
            newlines += (l & valid).count_ones();
            o += 64;
        }
        // So the counts are defined at position n when n is a multiple of 64.
        self.qmask.push(0);
        self.qprefix.push(quotes);
        self.nmask.push(0);
        self.nprefix.push(newlines);
        self.delims.extend(std::iter::repeat_n(u32::MAX, SENTINELS));
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

/// Appends the position of every set bit. The first N slots are written unconditionally (a cleared mask
/// yields harmless values past the end), so the usual block costs no data-dependent branch.
#[inline]
fn flatten<const N: usize>(mut m: u64, o: usize, out: &mut Vec<u32>) {
    out.reserve(64);
    let (base, p) = (out.len(), out.as_mut_ptr());
    let cnt = m.count_ones() as usize;
    // In bounds: at most 64 bits are set and 64 slots were reserved.
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
        out.set_len(base + cnt);
    }
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
