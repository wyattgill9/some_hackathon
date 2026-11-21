//! Files cut into byte ranges that threads take from a shared counter. A range owns the lines that start in it
//! (a line starts at `p` iff `p == 0` or the byte before it is `\n`); the line starting at 0 is the header.
//! Each range is read with `pread` into a reused per-thread buffer, indexed with `scan`, and walked line by
//! line: a line with exactly N fields whose quotes are all whole-field wrappers yields its N unquoted fields.
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::scan::Index;

const TAIL: usize = 8 << 10;
/// Readable bytes before and after the data: the split loads 4 bytes around every delimiter, the index 64.
const FRONT: usize = 64;
const PAD: usize = 64;

pub struct Range {
    pub file: usize,
    pub start: u64,
    pub end: u64,
}

/// Every `*.csv` in `dir`, with its size.
pub fn open_dir(dir: &Path) -> Vec<(File, u64)> {
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .map(|d| d.map(|e| e.unwrap().path()).filter(|p| p.extension().is_some_and(|x| x == "csv")).collect())
        .unwrap_or_default();
    paths.sort();
    paths
        .iter()
        .map(|p| {
            let f = File::open(p).unwrap();
            let n = f.metadata().unwrap().len();
            (f, n)
        })
        .collect()
}

/// Ranges of about `target` bytes, largest file first.
pub fn ranges(files: &[(File, u64)], target: u64) -> Vec<Range> {
    let mut order: Vec<usize> = (0..files.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(files[i].1));
    let mut out = Vec::new();
    for i in order {
        let n = files[i].1;
        let parts = n.div_ceil(target).max(1);
        for k in 0..parts {
            out.push(Range { file: i, start: n * k / parts, end: n * (k + 1) / parts });
        }
    }
    out
}

#[derive(Default)]
pub struct Reader {
    buf: Vec<u8>,
    index: Index,
}

impl Reader {
    /// Calls `f` with the fields of every well-formed N-field line the range owns, in order.
    pub fn fields<const N: usize>(&mut self, file: &File, r: &Range, mut f: impl FnMut([&[u8]; N])) {
        let from = r.start.saturating_sub(1);
        let owned_end = (r.end - from) as usize; // data index of file offset r.end
        let mut want = owned_end + TAIL;
        let n = loop {
            let (mut n, eof) = self.fill(file, from, want);
            if eof {
                if n == 0 {
                    return;
                }
                if self.buf[FRONT + n - 1] != b'\n' {
                    self.buf[FRONT + n] = b'\n'; // a missing final newline still ends the last line
                    n += 1;
                }
                break n;
            }
            // Every owned line ends inside the buffer iff some newline sits at or after owned_end - 1.
            if self.buf[FRONT + owned_end - 1..FRONT + n].contains(&b'\n') {
                break n;
            }
            want += TAIL;
        };
        let data = &self.buf[FRONT..FRONT + n + PAD];
        // The first owned line starts after the first newline at or after `from` (that skips the header at 0).
        let Some(first) = data[..n].iter().position(|&b| b == b'\n') else { return };
        let pos = first + 1;
        if pos >= owned_end {
            return;
        }
        self.index.build(data, n);
        let idx = &self.index;
        let dp = data.as_ptr();
        let mut d = idx.delims.partition_point(|&x| (x as usize) < pos) - 1; // the newline at pos - 1
        let mut start = pos;
        let (mut q0, mut l0) = (idx.quotes_before(start), idx.newlines_before(start));
        while start < owned_end {
            // A line of exactly N fields ends at the N-th delimiter after its start, which must be a newline
            // with no newline before it.
            // In bounds: d is at a real delimiter and SENTINELS entries follow the last one; cand < n is checked.
            let cand = unsafe { *idx.delims.get_unchecked(d + N) } as usize;
            if cand < n && unsafe { *data.get_unchecked(cand) } == b'\n' && idx.newlines_before(cand) == l0 {
                let q1 = idx.quotes_before(cand);
                // In bounds: the delimiter positions are < n, FRONT bytes precede the data and PAD follow it.
                let (starts, lens, wrapped) = unsafe { split::<N>(dp, idx.delims.as_ptr().add(d)) };
                if q1 - q0 == wrapped {
                    f(std::array::from_fn(|i| unsafe {
                        std::slice::from_raw_parts(dp.add(starts[i] as usize), lens[i] as usize)
                    }));
                }
                d += N;
                q0 = q1;
                start = cand + 1;
                l0 += 1;
            } else {
                // Wrong field count or empty line: skip to its newline.
                d += 1;
                while data[idx.delims[d] as usize] != b'\n' {
                    d += 1;
                }
                let end = idx.delims[d] as usize; // rare path: checked indexing is fine
                q0 = idx.quotes_before(end);
                l0 = idx.newlines_before(end) + 1;
                start = end + 1;
            }
        }
    }

    fn fill(&mut self, file: &File, from: u64, want: usize) -> (usize, bool) {
        if self.buf.len() < FRONT + want + PAD {
            self.buf.resize(FRONT + want + PAD, 0);
        }
        let mut n = 0;
        while n < want {
            match file.read_at(&mut self.buf[FRONT + n..FRONT + want], from + n as u64) {
                Ok(0) => return (n, true),
                Ok(k) => n += k,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => panic!("read: {e}"),
            }
        }
        (n, false)
    }
}

/// The N fields between the N + 1 delimiters at `pp` (positions into `dp`): starts and lengths with any
/// whole-field quotes stripped, and twice the number of such quoted fields (what the line's quote count must
/// equal). The 4 bytes around each delimiter are compared with `"` at once.
///
/// # Safety
/// `pp[..9]` must be readable and `dp` readable from one byte before the first position to 2 past the last.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn split<const N: usize>(dp: *const u8, pp: *const u32) -> ([u32; 8], [u32; 8], u32) {
    use std::arch::x86_64::*;
    unsafe {
        let p = _mm256_loadu_si256(pp as *const __m256i);
        let pn = _mm256_loadu_si256(pp.add(1) as *const __m256i);
        let around = |i: usize| -> i32 {
            if i <= N { (dp.add(*pp.add(i) as usize - 1) as *const i32).read_unaligned() } else { 0 }
        };
        let a = _mm256_setr_epi32(around(0), around(1), around(2), around(3), around(4), around(5), around(6), around(7));
        let q = _mm256_cmpeq_epi8(a, _mm256_set1_epi8(b'"' as i8));
        // Field i is quoted iff the byte after delimiter i (byte 2 of lane i) and the byte before delimiter
        // i + 1 (byte 0 of lane i + 1) are quotes, and it is at least 2 bytes long.
        let after = _mm256_srli_epi32::<16>(q);
        let before_next = _mm256_permutevar8x32_epi32(q, _mm256_setr_epi32(1, 2, 3, 4, 5, 6, 7, 7));
        let both = _mm256_and_si256(_mm256_and_si256(after, before_next), _mm256_set1_epi32(1));
        let len = _mm256_sub_epi32(_mm256_sub_epi32(pn, p), _mm256_set1_epi32(1));
        let w = _mm256_and_si256(both, _mm256_cmpgt_epi32(len, _mm256_set1_epi32(1)));
        let starts = _mm256_add_epi32(_mm256_add_epi32(p, _mm256_set1_epi32(1)), w);
        let lens = _mm256_sub_epi32(len, _mm256_slli_epi32::<1>(w));
        let bits = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_slli_epi32::<31>(w))) as u32 & ((1 << N) - 1);
        let (mut s, mut l) = ([0u32; 8], [0u32; 8]);
        _mm256_storeu_si256(s.as_mut_ptr() as *mut __m256i, starts);
        _mm256_storeu_si256(l.as_mut_ptr() as *mut __m256i, lens);
        (s, l, 2 * bits.count_ones())
    }
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn split<const N: usize>(dp: *const u8, pp: *const u32) -> ([u32; 8], [u32; 8], u32) {
    let (mut s, mut l, mut wrapped) = ([0u32; 8], [0u32; 8], 0);
    for i in 0..N {
        unsafe {
            let (a, b) = (*pp.add(i) as usize + 1, *pp.add(i + 1) as usize);
            let w = ((b - a >= 2) & (*dp.add(a) == b'"') & (*dp.add(b - 1) == b'"')) as usize;
            wrapped += w as u32;
            s[i] = (a + w) as u32;
            l[i] = (b - a - 2 * w) as u32;
        }
    }
    (s, l, 2 * wrapped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;
    use std::io::Write;

    fn through_reader<const N: usize>(file: &File, size: u64, target: u64) -> Vec<Vec<Vec<u8>>> {
        let files = [(file.try_clone().unwrap(), size)];
        let mut out = Vec::new();
        let mut rd = Reader::default();
        for r in ranges(&files, target) {
            rd.fields::<N>(&files[0].0, &r, |l| out.push(l.iter().map(|f| f.to_vec()).collect()));
        }
        out
    }

    #[test]
    fn every_split_matches_the_scalar_parser() {
        let mut rng = 0x1234_5678_9abc_def1u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for (n_fields, trailing_newline) in [(3usize, true), (3, false), (7, true), (4, false)] {
            let mut data = b"header\n".to_vec();
            let mut lines = Vec::new();
            for _ in 0..900 {
                // Around n_fields fields of random junk, so well-formed and malformed lines both occur.
                let k = (n_fields as i64 + [-1, 0, 0, 0, 1][(next() % 5) as usize]).max(0) as usize;
                let mut line = Vec::new();
                for i in 0..k {
                    if i > 0 {
                        line.push(b',');
                    }
                    let n = (next() % 6) as usize;
                    line.extend((0..n).map(|_| b"aaa\"\r\0\xc3"[(next() % 7) as usize]));
                }
                data.extend_from_slice(&line);
                data.push(b'\n');
                lines.push(line);
            }
            if !trailing_newline {
                data.pop();
            }
            let path = std::env::temp_dir().join(format!("reader-test-{}-{n_fields}-{trailing_newline}", std::process::id()));
            File::create(&path).unwrap().write_all(&data).unwrap();
            let file = File::open(&path).unwrap();
            let want = |n: usize| -> Vec<Vec<Vec<u8>>> {
                lines
                    .iter()
                    .filter_map(|l| match n {
                        3 => parse::fields::<3>(l).map(|f| f.to_vec()),
                        4 => parse::fields::<4>(l).map(|f| f.to_vec()),
                        _ => parse::fields::<7>(l).map(|f| f.to_vec()),
                    })
                    .map(|fs| fs.iter().map(|f| f.to_vec()).collect())
                    .collect()
            };
            let w = want(n_fields);
            assert!(w.len() > 10, "the generated lines should include well-formed ones ({n_fields} fields)");
            for target in [1u64, 2, 3, 7, 64, 1000, 1 << 20] {
                let got = match n_fields {
                    3 => through_reader::<3>(&file, data.len() as u64, target),
                    4 => through_reader::<4>(&file, data.len() as u64, target),
                    _ => through_reader::<7>(&file, data.len() as u64, target),
                };
                assert_eq!(got, w, "fields {n_fields} target {target}");
            }
            std::fs::remove_file(&path).unwrap();
        }
        for data in [&b"header\n"[..], b"header", b""] {
            let path = std::env::temp_dir().join(format!("reader-test-{}-h", std::process::id()));
            File::create(&path).unwrap().write_all(data).unwrap();
            let file = File::open(&path).unwrap();
            assert!(through_reader::<2>(&file, data.len() as u64, 3).is_empty());
            std::fs::remove_file(&path).unwrap();
        }
    }
}
