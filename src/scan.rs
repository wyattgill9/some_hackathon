//! The CSV index: the positions of every `\n`, `,` and `"` in a buffer, found 64 bytes at a time with AVX2.
//! The reader walks it line by line; sentinels at the end keep every lookahead in bounds.

#[derive(Default)]
pub struct Index {
    pub delims: Vec<u32>, // commas and newlines, in order
    pub nl: Vec<u32>,
    pub quotes: Vec<u32>,
}

pub const SENTINELS: usize = 16;

impl Index {
    /// Indexes `buf[..n]`; `buf` must extend at least 64 bytes past `n`.
    pub fn build(&mut self, buf: &[u8], n: usize) {
        assert!(buf.len() >= n + 64);
        self.delims.clear();
        self.nl.clear();
        self.quotes.clear();
        self.delims.reserve(n / 6 + SENTINELS);
        self.nl.reserve(n / 32 + 1);
        self.quotes.reserve(n / 32 + 1);
        let mut o = 0;
        while o < n {
            let (d, l, q) = masks(&buf[o..o + 64]);
            let valid = if n - o >= 64 { u64::MAX } else { (1u64 << (n - o)) - 1 };
            flatten(d & valid, o, &mut self.delims);
            flatten(l & valid, o, &mut self.nl);
            flatten(q & valid, o, &mut self.quotes);
            o += 64;
        }
        self.delims.extend(std::iter::repeat_n(u32::MAX, SENTINELS));
        self.nl.push(u32::MAX);
        self.quotes.push(u32::MAX);
    }
}

#[inline]
fn flatten(mut m: u64, o: usize, out: &mut Vec<u32>) {
    out.reserve(64);
    let (base, p) = (out.len(), out.as_mut_ptr());
    let mut n = 0;
    while m != 0 {
        // In bounds: at most 64 bits are set and 64 slots were reserved.
        unsafe { p.add(base + n).write((o + m.trailing_zeros() as usize) as u32) };
        n += 1;
        m &= m - 1;
    }
    unsafe { out.set_len(base + n) };
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
        assert_eq!(&idx.nl[..idx.nl.len() - 1], want(b'\n'));
        assert_eq!(&idx.quotes[..idx.quotes.len() - 1], want(b'"'));
        let mut d: Vec<u32> = want(b',');
        d.extend(want(b'\n'));
        d.sort_unstable();
        assert_eq!(&idx.delims[..idx.delims.len() - SENTINELS], d);
    }
}
