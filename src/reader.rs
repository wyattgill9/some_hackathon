//! Files cut into byte ranges that threads take from a shared counter. A range owns the lines that start in it
//! (a line starts at `p` iff `p == 0` or the byte before it is `\n`); the line starting at 0 is the header.
//! Each range is read with `pread` into a reused per-thread buffer and walked line by line. A line is split
//! from delimiter masks taken at fixed places (its first 64 bytes, then the 64 bits after its third comma),
//! with the field starts and ends popped as bits; a line that does not fit, or is malformed, goes through
//! the scalar dialect parser. A line with exactly N fields whose quotes are all whole-field wrappers yields
//! its N unquoted fields, in order.
use std::fs::File;
use std::marker::PhantomData;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::parse;
use crate::scan::{masks32, masks64};

const TAIL: usize = 8 << 10;
/// Readable bytes after the data: the masks reach 128 bytes past a line start, value parsers 16 past a field.
pub const PAD: usize = 128;

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

/// A well-formed line's fields, taken in order. They point into the reader's buffer, which keeps `PAD`
/// readable bytes past the data.
pub trait Fields<'a> {
    fn next(&mut self) -> &'a [u8];
}

/// What the reader calls with every well-formed line.
pub trait Handler<const N: usize> {
    fn line<'a>(&mut self, f: impl Fields<'a>);
}

/// The fields of a window still to come: field i runs from the i-th set bit of `s` to the i-th of `e`.
#[derive(Clone, Copy)]
struct Cut {
    s: u64,
    e: u64,
}

impl Cut {
    /// The next field as (start, length), its quotes already left out.
    #[inline(always)]
    fn next(&mut self) -> (usize, usize) {
        let (s, e) = (self.s.trailing_zeros() as usize, self.e.trailing_zeros() as usize);
        self.s &= self.s.wrapping_sub(1);
        self.e &= self.e.wrapping_sub(1);
        (s, e - s)
    }
}

/// A line split from its masks: the first `split` fields from `line`, the rest from `rest`.
pub struct Fast<'a> {
    line: *const u8,
    rest: *const u8,
    a: Cut,
    b: Cut,
    k: usize,
    split: usize,
    _buf: PhantomData<&'a [u8]>,
}

impl<'a> Fields<'a> for Fast<'a> {
    #[inline(always)]
    fn next(&mut self) -> &'a [u8] {
        let (p, (s, l)) = if self.k < self.split { (self.line, self.a.next()) } else { (self.rest, self.b.next()) };
        self.k += 1;
        // In bounds: the cuts came from delimiters inside the buffer.
        unsafe { std::slice::from_raw_parts(p.add(s), l) }
    }
}

/// A line split by the scalar parser.
pub struct Slow<'a, const N: usize>([&'a [u8]; N], usize);

impl<'a, const N: usize> Fields<'a> for Slow<'a, N> {
    #[inline(always)]
    fn next(&mut self) -> &'a [u8] {
        let f = self.0[self.1];
        self.1 += 1;
        f
    }
}

impl Reader {
    /// Calls `h.line` with every well-formed N-field line the range owns, in order.
    pub fn fields<const N: usize>(&mut self, file: &File, r: &Range, h: &mut impl Handler<N>) {
        let from = r.start.saturating_sub(1);
        let owned_end = (r.end - from) as usize; // data index of file offset r.end
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
        let data = &self.buf[..n + PAD];
        // The first owned line starts after the first newline at or after `from` (that skips the header at 0).
        let Some(first) = data[..n].iter().position(|&b| b == b'\n') else { return };
        let mut p = first + 1;
        let dp = data.as_ptr();
        while p < owned_end {
            // In bounds: p < n and PAD bytes follow the data.
            match unsafe { split::<N>(dp.add(p)) } {
                Some((f, len)) => {
                    h.line(f);
                    p += len;
                }
                None => {
                    // Malformed, or too long for the windows: the exact parser. Its newline is before n.
                    let e = p + data[p..n].iter().position(|&b| b == b'\n').unwrap();
                    if let Some(f) = parse::fields::<N>(&data[p..e]) {
                        h.line(Slow(f, 0));
                    }
                    p = e + 1;
                }
            }
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

/// The fields ending at the boundaries `b` (commas, or the line's newline) of a window that starts where a
/// field starts, given the window's quotes `q` below its last boundary: (field starts, field ends, nonzero
/// when a quote is out of place), the starts and ends moved inside any whole-field quotes.
///
/// Well quoted <=> every quote is a field's first byte (`s`) or last byte (`z`), none is both (a lone `"`),
/// and per field a first-byte quote (`x`) comes with a last-byte quote (`y`, moved onto the boundary): then
/// `y - x` borrows across exactly the quoted fields' bytes and clears every boundary, while a field with only
/// one of the two leaves a bit on the boundary that ends it (or on a higher one).
#[inline(always)]
fn window(b: u64, q: u64) -> (u64, u64, u64) {
    let s = (b << 1) | 1;
    let z = b >> 1;
    let x = q & s;
    let y = (q << 1) & b;
    let bad = (q & !(s | z)) | (x & z) | (y.wrapping_sub(x) & b);
    ((s ^ x) | (x << 1), (b & !y) | (y >> 1), bad)
}

/// The N fields of the line at `p` and its length with its newline, when the line fits: N <= 4 fields all
/// in the first 64 bytes, or 3 fields in the first 64 bytes and N - 3 in the 64 bytes after the third comma.
/// None sends the line to the scalar parser: it is malformed, or does not fit.
///
/// # Safety
/// `p[..128]` must be readable.
#[inline(always)]
unsafe fn split<'a, const N: usize>(p: *const u8) -> Option<(Fast<'a>, usize)> {
    let [ca, qa, na] = unsafe { masks64(p) };
    let ba = ca | na;
    let fast = |line, rest, a, b, split, len| Some((Fast { line, rest, a, b, k: 0, split, _buf: PhantomData }, len));
    if N <= 4 {
        let mut x = ba;
        for _ in 1..N {
            x &= x.wrapping_sub(1);
        }
        if x == 0 {
            return None;
        }
        let m = x ^ (x - 1); // through the N-th boundary
        let (s, e, bad) = window(ba & m, qa & m);
        // Of the N boundaries only the last is the newline.
        if ((na & m) ^ (x & x.wrapping_neg())) | bad != 0 {
            return None;
        }
        return fast(p, p, Cut { s, e }, Cut { s: 0, e: 0 }, N, x.trailing_zeros() as usize + 1);
    }
    let mut x = ba;
    x &= x.wrapping_sub(1);
    x &= x.wrapping_sub(1);
    if x == 0 {
        return None;
    }
    let ma = x ^ (x - 1); // through the third boundary
    let (sa, ea, bad_a) = window(ba & ma, qa & ma);
    let k2 = x.trailing_zeros();
    // The second window starts after the third comma: the masks of bytes 64.. moved down, taken at a fixed
    // place so they do not wait for the comma. Nearly every line ends in its first 96 bytes: the last 32 of
    // the 128 are looked at only when it does not.
    let [c1, q1, n1] = {
        let [mut c1, mut q1, mut n1] = unsafe { masks32(p.add(64)) };
        if na | n1 == 0 {
            let [c, q, n] = unsafe { masks32(p.add(96)) };
            (c1, q1, n1) = (c1 | c << 32, q1 | q << 32, n1 | n << 32);
        }
        [c1, q1, n1]
    };
    let down = |m0: u64, m1: u64| ((m0 >> 1) >> k2) | (m1 << (63 - k2));
    let (cb, qb, nb) = (down(ca, c1), down(qa, q1), down(na, n1));
    let bb = cb | nb;
    let mut y = bb;
    for _ in 1..N - 3 {
        y &= y.wrapping_sub(1);
    }
    if y == 0 {
        return None;
    }
    let mb = y ^ (y - 1); // through the (N - 3)-th boundary of the second window
    let (sb, eb, bad_b) = window(bb & mb, qb & mb);
    // The first three boundaries are commas; of the rest only the last is the newline.
    if (ba & ma & na) | bad_a | ((nb & mb) ^ (y & y.wrapping_neg())) | bad_b != 0 {
        return None;
    }
    let o = k2 as usize + 1;
    fast(p, unsafe { p.add(o) }, Cut { s: sa, e: ea }, Cut { s: sb, e: eb }, 3, o + y.trailing_zeros() as usize + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct Collect<const N: usize>(Vec<Vec<Vec<u8>>>);

    impl<const N: usize> Handler<N> for Collect<N> {
        fn line<'a>(&mut self, mut f: impl Fields<'a>) {
            self.0.push((0..N).map(|_| f.next().to_vec()).collect());
        }
    }

    fn through_reader<const N: usize>(file: &File, size: u64, target: u64) -> Vec<Vec<Vec<u8>>> {
        let files = [(file.try_clone().unwrap(), size)];
        let mut out = Collect::<N>(Vec::new());
        let mut rd = Reader::default();
        for r in ranges(&files, target) {
            rd.fields::<N>(&files[0].0, &r, &mut out);
        }
        out.0
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
        for (n_fields, trailing_newline, long) in
            [(3usize, true, false), (3, false, false), (7, true, false), (4, false, false), (7, true, true), (4, true, true), (2, true, true)]
        {
            let mut data = b"header\n".to_vec();
            let mut lines = Vec::new();
            for _ in 0..3000 {
                // Around n_fields fields of random junk, so well-formed and malformed lines both occur; long
                // fields put the commas and the newline on either side of the 64/96/128-byte window edges.
                let k = (n_fields as i64 + [-1, 0, 0, 0, 1][(next() % 5) as usize]).max(0) as usize;
                let mut line = Vec::new();
                for i in 0..k {
                    if i > 0 {
                        line.push(b',');
                    }
                    let n = if long { (next() % 40) as usize } else { (next() % 6) as usize };
                    let quoted = long && next() % 4 == 0;
                    if quoted {
                        line.push(b'"');
                    }
                    line.extend((0..n).map(|_| b"aaa\"\r\0\xc3"[(next() % if long { 3 } else { 7 }) as usize]));
                    if quoted && next() % 8 != 0 {
                        line.push(b'"');
                    }
                }
                data.extend_from_slice(&line);
                data.push(b'\n');
                lines.push(line);
            }
            if !trailing_newline {
                data.pop();
            }
            let path = std::env::temp_dir().join(format!("reader-test-{}-{n_fields}-{trailing_newline}-{long}", std::process::id()));
            File::create(&path).unwrap().write_all(&data).unwrap();
            let file = File::open(&path).unwrap();
            let want = |n: usize| -> Vec<Vec<Vec<u8>>> {
                lines
                    .iter()
                    .filter_map(|l| match n {
                        2 => parse::fields::<2>(l).map(|f| f.to_vec()),
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
                    2 => through_reader::<2>(&file, data.len() as u64, target),
                    3 => through_reader::<3>(&file, data.len() as u64, target),
                    4 => through_reader::<4>(&file, data.len() as u64, target),
                    _ => through_reader::<7>(&file, data.len() as u64, target),
                };
                assert_eq!(got, w, "fields {n_fields} target {target} long {long}");
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

    #[test]
    fn window_split_matches_the_scalar_parser() {
        // Every line of up to 7 fields of up to 30 bytes from a small alphabet rich in quotes, so that quote
        // placements of every kind land on both paths.
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut buf = vec![0u8; 512];
        for _ in 0..200_000 {
            let k = 1 + (next() % 8) as usize;
            let mut line = Vec::new();
            for i in 0..k {
                if i > 0 {
                    line.push(b',');
                }
                let n = (next() % 4) as usize * (next() % 8) as usize;
                line.extend((0..n).map(|_| b"a\"\"b"[(next() % 4) as usize]));
            }
            buf[..line.len()].copy_from_slice(&line);
            buf[line.len()] = b'\n';
            for b in &mut buf[line.len() + 1..] {
                *b = b"x,\"\n"[(next() % 4) as usize]; // what follows must not matter
            }
            let got = unsafe { split::<7>(buf.as_ptr()) }.map(|(mut f, len)| {
                assert_eq!(len, line.len() + 1);
                (0..7).map(|_| f.next().to_vec()).collect::<Vec<_>>()
            });
            let want = parse::fields::<7>(&line).map(|f| f.iter().map(|x| x.to_vec()).collect::<Vec<_>>());
            if got.is_some() || line.len() <= 60 {
                assert_eq!(got, want, "{:?}", String::from_utf8_lossy(&line));
            }
            let got = unsafe { split::<4>(buf.as_ptr()) }.map(|(mut f, len)| {
                assert_eq!(len, line.len() + 1);
                (0..4).map(|_| f.next().to_vec()).collect::<Vec<_>>()
            });
            let want = parse::fields::<4>(&line).map(|f| f.iter().map(|x| x.to_vec()).collect::<Vec<_>>());
            if got.is_some() || line.len() <= 60 {
                assert_eq!(got, want, "{:?}", String::from_utf8_lossy(&line));
            }
        }
    }
}
