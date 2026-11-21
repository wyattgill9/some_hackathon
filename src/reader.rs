//! Files cut into byte ranges that threads take from a shared counter. A range owns the lines that start in it
//! (a line starts at `p` iff `p == 0` or the byte before it is `\n`); the line starting at 0 is the header.
//! Each range is read with `pread` into a reused per-thread buffer, plus a tail for its last line.
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

const TAIL: usize = 64 << 10;

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
}

impl Reader {
    /// Calls `f` on every line the range owns, in order, without its `\n`. A missing final `\n` still ends a line.
    pub fn lines(&mut self, file: &File, r: &Range, mut f: impl FnMut(&[u8])) {
        let from = r.start.saturating_sub(1);
        let owned_end = (r.end - from) as usize; // buffer index of file offset r.end
        let mut want = owned_end + TAIL;
        let (mut n, mut eof) = self.fill(file, from, want);
        // The first owned line starts after the first newline at or after `from` (that skips the header at 0).
        let mut pos = match self.buf[..n].iter().position(|&b| b == b'\n') {
            Some(i) => i + 1,
            None => return,
        };
        while pos < owned_end {
            match self.buf[pos..n].iter().position(|&b| b == b'\n') {
                Some(i) => {
                    f(&self.buf[pos..pos + i]);
                    pos += i + 1;
                }
                None if eof => {
                    if pos < n {
                        f(&self.buf[pos..n]);
                    }
                    return;
                }
                None => {
                    // The line runs past the tail: read more of the file, from the same place.
                    want += TAIL;
                    (n, eof) = self.fill(file, from, want);
                }
            }
        }
    }

    fn fill(&mut self, file: &File, from: u64, want: usize) -> (usize, bool) {
        if self.buf.len() < want {
            self.buf.resize(want, 0);
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
    use std::io::Write;

    fn owned_lines(file: &File, size: u64, target: u64) -> Vec<Vec<u8>> {
        let files = [(file.try_clone().unwrap(), size)];
        let mut out = Vec::new();
        let mut rd = Reader::default();
        for r in ranges(&files, target) {
            rd.lines(&files[0].0, &r, |l| out.push(l.to_vec()));
        }
        out
    }

    #[test]
    fn every_split_sees_every_line_once() {
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
            for _ in 0..60 {
                let n = (next() % 40) as usize;
                let line: Vec<u8> = (0..n).map(|_| [b'a', b',', b'"', b'\r', 0, 0xc3][(next() % 6) as usize]).collect();
                data.extend_from_slice(&line);
                data.push(b'\n');
                lines.push(line);
            }
            if !trailing_newline {
                data.pop();
            }
            let path = std::env::temp_dir().join(format!("reader-test-{}", std::process::id()));
            File::create(&path).unwrap().write_all(&data).unwrap();
            let file = File::open(&path).unwrap();
            for target in [1u64, 2, 3, 7, 64, 1000, 1 << 20] {
                assert_eq!(owned_lines(&file, data.len() as u64, target), lines, "target {target}");
            }
            std::fs::remove_file(&path).unwrap();
        }
        // Header only, with and without a newline; empty file.
        for data in [&b"header\n"[..], b"header", b""] {
            let path = std::env::temp_dir().join(format!("reader-test-{}-h", std::process::id()));
            File::create(&path).unwrap().write_all(data).unwrap();
            let file = File::open(&path).unwrap();
            assert!(owned_lines(&file, data.len() as u64, 3).is_empty());
            std::fs::remove_file(&path).unwrap();
        }
    }
}
