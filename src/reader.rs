//! Files cut into byte ranges that threads take from a shared counter. A range owns the lines that start in it
//! (a line starts at `p` iff `p == 0` or the byte before it is `\n`); the line starting at 0 is the header.
//! Each range is read with `pread` into a reused per-thread buffer, indexed with `scan`, and walked line by
//! line: a line with exactly N fields whose quotes are all whole-field wrappers yields its N unquoted fields.
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::scan::Index;

const TAIL: usize = 8 << 10;
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
        let owned_end = (r.end - from) as usize; // buffer index of file offset r.end
        let mut want = owned_end + TAIL;
        let n = loop {
            let (mut n, eof) = self.fill(file, from, want);
            if eof {
                if n == 0 {
                    return;
                }
                if self.buf[n - 1] != b'\n' {
                    self.buf[n] = b'\n'; // a missing final newline still ends the last line
                    n += 1;
                }
                break n;
            }
            // Every owned line ends inside the buffer iff some newline sits at or after owned_end - 1.
            if self.buf[owned_end - 1..n].contains(&b'\n') {
                break n;
            }
            want += TAIL;
        };
        // The first owned line starts after the first newline at or after `from` (that skips the header at 0).
        let Some(first) = self.buf[..n].iter().position(|&b| b == b'\n') else { return };
        let pos = first + 1;
        if pos >= owned_end {
            return;
        }
        self.index.build(&self.buf, n);
        let (buf, idx) = (&self.buf[..], &self.index);
        let mut k = idx.nl.partition_point(|&x| (x as usize) < pos);
        let mut d = idx.delims.partition_point(|&x| (x as usize) < pos) - 1; // the newline at pos - 1
        let mut start = pos;
        let mut quotes_at_start = idx.quotes_before(start);
        while start < owned_end {
            let end = idx.nl[k] as usize;
            let quotes_at_end = idx.quotes_before(end);
            let quotes = quotes_at_end - quotes_at_start;
            quotes_at_start = quotes_at_end;
            if idx.delims[d + N] as usize == end {
                let mut out = [&buf[..0]; N];
                let mut wrapped = 0;
                for (i, o) in out.iter_mut().enumerate() {
                    // In bounds: the index ends in SENTINELS entries past any d + N, and positions are < n.
                    // The bytes at a and b - 1 are the field's ends, or its delimiters when it is empty.
                    unsafe {
                        let (a, b) = (*idx.delims.get_unchecked(d + i) as usize + 1, *idx.delims.get_unchecked(d + i + 1) as usize);
                        let w = ((b - a >= 2) & (*buf.get_unchecked(a) == b'"') & (*buf.get_unchecked(b - 1) == b'"')) as usize;
                        wrapped += w;
                        *o = buf.get_unchecked(a + w..b - w);
                    }
                }
                if quotes as usize == 2 * wrapped {
                    f(out);
                }
                d += N;
            } else {
                while idx.delims[d] as usize != end {
                    d += 1;
                }
            }
            start = end + 1;
            k += 1;
        }
    }

    fn fill(&mut self, file: &File, from: u64, want: usize) -> (usize, bool) {
        if self.buf.len() < want + PAD {
            self.buf.resize(want + PAD, 0);
        }
        let mut n = 0;
        while n < want {
            match file.read_at(&mut self.buf[n..want], from + n as u64) {
                Ok(0) => return (n, true),
                Ok(k) => n += k,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => panic!("read: {e}"),
            }
        }
        (n, false)
    }
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
        for trailing_newline in [true, false] {
            let mut data = b"header\n".to_vec();
            let mut lines = Vec::new();
            for _ in 0..600 {
                let n = (next() % 24) as usize;
                let line: Vec<u8> = (0..n).map(|_| b"aaaaa,,\"\r\0\xc3"[(next() % 11) as usize]).collect();
                data.extend_from_slice(&line);
                data.push(b'\n');
                lines.push(line);
            }
            if !trailing_newline {
                data.pop();
            }
            let want: Vec<Vec<Vec<u8>>> = lines
                .iter()
                .filter_map(|l| parse::fields::<3>(l))
                .map(|fs| fs.iter().map(|f| f.to_vec()).collect())
                .collect();
            assert!(want.len() > 10, "the generated lines should include well-formed ones");
            let path = std::env::temp_dir().join(format!("reader-test-{}-{trailing_newline}", std::process::id()));
            File::create(&path).unwrap().write_all(&data).unwrap();
            let file = File::open(&path).unwrap();
            for target in [1u64, 2, 3, 7, 64, 1000, 1 << 20] {
                assert_eq!(through_reader::<3>(&file, data.len() as u64, target), want, "target {target}");
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
