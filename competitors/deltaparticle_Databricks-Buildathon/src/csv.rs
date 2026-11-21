//! The CSV dialect of rules.md: only `\n` ends a line, a field may be wrapped
//! in quotes as a whole, any other quote makes the line malformed, a line needs
//! exactly the expected number of fields, and an empty line is malformed.

use memchr::memchr;

/// Split `line` into exactly `N` unquoted fields. False if malformed.
#[inline(always)]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let n = line.len();
    if n == 0 {
        return false;
    }
    // Every comma separates fields; a quote may only wrap a whole field.
    let mut f = 0usize;
    let mut start = 0usize;
    loop {
        let end = match memchr(b',', &line[start..]) {
            Some(i) => start + i,
            None => n,
        };
        if f == N {
            return false;
        }
        let v = &line[start..end];
        out[f] = if v.len() >= 2 && v[0] == b'"' && v[v.len() - 1] == b'"' {
            let inner = &v[1..v.len() - 1];
            if memchr(b'"', inner).is_some() {
                return false;
            }
            inner
        } else {
            if memchr(b'"', v).is_some() {
                return false;
            }
            v
        };
        f += 1;
        if end == n {
            break;
        }
        start = end + 1;
    }
    f == N
}

/// Call `f` on every line of `data` (without its `\n`). Text after the last
/// `\n` is a line too.
#[inline(always)]
pub fn for_lines<'a>(data: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut start = 0;
    for end in memchr::memchr_iter(b'\n', data) {
        f(&data[start..end]);
        start = end + 1;
    }
    if start < data.len() {
        f(&data[start..]);
    }
}

/// Bitmasks of `\n`, `,` and `"` in the 64 bytes at `p`.
#[inline(always)]
pub fn masks(p: &[u8; 64]) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let cm = _mm256_set1_epi8(b',' as i8);
        let qt = _mm256_set1_epi8(b'"' as i8);
        let a = _mm256_loadu_si256(p.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(p.as_ptr().add(32) as *const __m256i);
        let m = |x: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, c)) as u32 as u64;
        (m(a, nl) | m(b, nl) << 32, m(a, cm) | m(b, cm) << 32, m(a, qt) | m(b, qt) << 32)
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    unsafe {
        use std::arch::x86_64::*;
        let nl = _mm_set1_epi8(b'\n' as i8);
        let cm = _mm_set1_epi8(b',' as i8);
        let qt = _mm_set1_epi8(b'"' as i8);
        let (mut mn, mut mc, mut mq) = (0u64, 0u64, 0u64);
        for i in 0..4 {
            let v = _mm_loadu_si128(p.as_ptr().add(16 * i) as *const __m128i);
            let m = |c: __m128i| _mm_movemask_epi8(_mm_cmpeq_epi8(v, c)) as u32 as u64;
            mn |= m(nl) << (16 * i);
            mc |= m(cm) << (16 * i);
            mq |= m(qt) << (16 * i);
        }
        (mn, mc, mq)
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let (mut mn, mut mc, mut mq) = (0u64, 0u64, 0u64);
        for (i, &b) in p.iter().enumerate() {
            mn |= ((b == b'\n') as u64) << i;
            mc |= ((b == b',') as u64) << i;
            mq |= ((b == b'"') as u64) << i;
        }
        (mn, mc, mq)
    }
}

/// The state of one scan over a byte range: per-line comma positions and
/// quote count, advanced one 64-byte block at a time.
struct Stream<'a, const N: usize> {
    data: &'a [u8],
    base: usize,
    line_start: usize,
    commas: [usize; N],
    /// Commas seen in the current line; N marks a line with too many.
    nc: usize,
    /// Quotes seen in the current line.
    quotes: u32,
}

impl<'a, const N: usize> Stream<'a, N> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, base: 0, line_start: 0, commas: [0; N], nc: 0, quotes: 0 }
    }

    #[inline(always)]
    fn more(&self) -> bool {
        self.base < self.data.len()
    }

    /// Commas of the current line in `c`: record them, or mark the line bad.
    #[inline(always)]
    fn add_commas(&mut self, c: u64) {
        let cnt = c.count_ones() as usize;
        if self.nc + cnt > N - 1 {
            self.nc = N;
            return;
        }
        let mut m = c;
        while m != 0 {
            self.commas[self.nc] = self.base + m.trailing_zeros() as usize;
            self.nc += 1;
            m &= m - 1;
        }
    }

    /// One line [start, end) is complete: split it and call `f` if well formed.
    #[inline(always)]
    fn finish(&self, end: usize, f: &mut impl FnMut(&[&'a [u8]; N])) {
        let (data, start) = (self.data, self.line_start);
        if end == start || self.nc != N - 1 {
            return;
        }
        // Branch-free: each field strips one byte from each end when it is
        // wrapped in quotes; the line is valid only if that accounts for
        // every quote in it.
        let mut out: [&'a [u8]; N] = [&[]; N];
        let mut s = start;
        let mut wrapped = 0u32;
        for i in 0..N {
            let e = if i + 1 < N { self.commas[i] } else { end };
            // SAFETY: start <= commas[0] < ... < commas[N-2] < end <= data.len()
            // and end > start, so both indexes are clamped into [start, end).
            // They only matter when e >= s + 2, where they are s and e - 1.
            let fi = s.min(end - 1);
            let li = (e.max(s + 1) - 1).min(end - 1);
            let (first, last) = unsafe { (*data.get_unchecked(fi), *data.get_unchecked(li)) };
            let w = ((e >= s + 2) & (first == b'"') & (last == b'"')) as usize;
            wrapped += 2 * w as u32;
            out[i] = unsafe { data.get_unchecked(s + w..e - w) };
            s = e + 1;
        }
        if wrapped != self.quotes {
            return;
        }
        f(&out);
    }

    /// Process the next 64-byte block.
    #[inline(always)]
    fn block(&mut self, f: &mut impl FnMut(&[&'a [u8]; N])) {
        let len = self.data.len();
        let mut tail = [0u8; 64];
        let block: &[u8; 64] = if self.base + 64 <= len {
            self.data[self.base..self.base + 64].try_into().unwrap()
        } else {
            tail[..len - self.base].copy_from_slice(&self.data[self.base..]);
            &tail
        };
        let (mut nl, mut cm, mut qt) = masks(block);
        while nl != 0 {
            let bit = nl & nl.wrapping_neg();
            let below = bit - 1;
            self.add_commas(cm & below);
            self.quotes += (qt & below).count_ones();
            cm &= !below;
            qt &= !below;
            let pos = self.base + bit.trailing_zeros() as usize;
            self.finish(pos, f);
            self.line_start = pos + 1;
            self.nc = 0;
            self.quotes = 0;
            nl ^= bit;
        }
        self.add_commas(cm);
        self.quotes += qt.count_ones();
        self.base += 64;
    }

    /// Flush a last line that has no `\n`.
    #[inline(always)]
    fn end(&self, f: &mut impl FnMut(&[&'a [u8]; N])) {
        if self.line_start < self.data.len() {
            self.finish(self.data.len(), f);
        }
    }
}

/// Call `f` with the `N` unquoted fields of every well-formed line of
/// `data` (the same dialect as `for_lines` + `split`), in one SIMD pass.
#[inline(always)]
pub fn scan<'a, const N: usize>(data: &'a [u8], mut f: impl FnMut(&[&'a [u8]; N])) {
    let mut a = Stream::<N>::new(data);
    while a.more() {
        a.block(&mut f);
    }
    a.end(&mut f);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `scan` must agree with `for_lines` + `split` on arbitrary input.
    #[test]
    fn scan_matches_split() {
        let alphabet = b"ab,,\"\"\n\r 1";
        let mut state = 12345u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20000 {
            let n = (next() % 300) as usize;
            let data: Vec<u8> = (0..n).map(|_| alphabet[(next() % alphabet.len() as u64) as usize]).collect();
            let mut a: Vec<Vec<Vec<u8>>> = Vec::new();
            for_lines(&data, |line| {
                let mut o = [&b""[..]; 3];
                if split::<3>(line, &mut o) {
                    a.push(o.iter().map(|x| x.to_vec()).collect());
                }
            });
            let mut b: Vec<Vec<Vec<u8>>> = Vec::new();
            scan::<3>(&data, |o| b.push(o.iter().map(|x| x.to_vec()).collect()));
            assert_eq!(a, b, "{:?}", String::from_utf8_lossy(&data));
        }
    }
    fn s<const N: usize>(l: &str) -> Option<Vec<String>> {
        let mut o = [&b""[..]; N];
        split::<N>(l.as_bytes(), &mut o)
            .then(|| o.iter().map(|x| String::from_utf8_lossy(x).into_owned()).collect())
    }
    #[test]
    fn dialect() {
        assert_eq!(s::<2>("a,b"), Some(vec!["a".into(), "b".into()]));
        assert_eq!(s::<2>("\"a\",\"\""), Some(vec!["a".into(), "".into()]));
        assert_eq!(s::<2>("a,"), Some(vec!["a".into(), "".into()]));
        assert_eq!(s::<2>(",a"), Some(vec!["".into(), "a".into()]));
        assert_eq!(s::<2>("a,\""), None);
        assert_eq!(s::<2>("a,b\""), None);
        assert_eq!(s::<2>("\"a\"b,c"), None);
        assert_eq!(s::<2>("a,b,c"), None);
        assert_eq!(s::<2>("a"), None);
        assert_eq!(s::<2>(""), None);
        assert_eq!(s::<1>(""), None);
        assert_eq!(s::<2>(" a,b \r"), Some(vec![" a".into(), "b \r".into()]));
        assert_eq!(s::<2>("\"a,b"), None);
        assert_eq!(s::<2>("\"a,b\",c"), None);
        assert_eq!(s::<3>("\"a,b\",c"), None);
    }
}
