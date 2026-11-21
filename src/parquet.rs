//! A minimal Parquet writer: one row group (none for 0 rows), one uncompressed v1 data page per column,
//! Thrift compact footer, the whole file written with one `writev`. Integers go DELTA_BINARY_PACKED or PLAIN,
//! strings DELTA_LENGTH_BYTE_ARRAY or PLAIN. Just enough for the two output tables; `scripts/check_parquet.py`
//! reads the result back with polars, pyarrow and duckdb.
use std::io::{IoSlice, Write};
use std::path::Path;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Int32,
    Int64,
    Date,
    Str,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Enc {
    Plain,
    /// DELTA_BINARY_PACKED (integers), DELTA_LENGTH_BYTE_ARRAY (strings).
    Delta,
}

/// `parts` concatenated are the page body: the definition levels (if optional) followed by the values.
pub struct Column<'a> {
    pub name: &'static str,
    pub kind: Kind,
    pub enc: Enc,
    pub optional: bool,
    pub parts: &'a [&'a [u8]],
}

/// A PLAIN BYTE_ARRAY value.
#[inline]
pub fn put_str(out: &mut Vec<u8>, s: &[u8]) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s);
}

/// Integer columns are PLAIN little-endian: the slice's own bytes.
pub fn plain<T: Copy>(v: &[T]) -> &[u8] {
    // Only ever called on i32/i64/u32 slices, which have no padding and are little-endian on x86.
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// Definition levels of an optional column from `n` packed presence bits (bit i of byte i / 8), as
/// `[i32 byte length][one bit-packed run of width 1]` in groups of 8.
pub fn def_levels(bits: &[u8], n: usize) -> Vec<u8> {
    let groups = n.div_ceil(8);
    let mut run = Vec::new();
    varint(&mut run, (groups as u64) << 1 | 1);
    let mut out = Vec::with_capacity(4 + run.len() + groups);
    out.extend_from_slice(&((run.len() + groups) as u32).to_le_bytes());
    out.extend_from_slice(&run);
    out.extend_from_slice(&bits[..groups]);
    out
}
/// DELTA_BINARY_PACKED: blocks of 128 deltas in 4 miniblocks of 32, each bit-packed at its own width.
pub fn delta<T: Copy + Into<i64>>(vals: &[T]) -> Vec<u8> {
    let n = vals.len();
    let mut out = Vec::with_capacity(n * 2 + 16);
    varint(&mut out, 128);
    varint(&mut out, 4);
    varint(&mut out, n as u64);
    // The first value is written even for an empty column: readers may take it as part of the header.
    let mut prev = vals.first().map_or(0, |&v| v.into());
    zigzag(&mut out, prev);
    let mut i = 1;
    while i < n {
        let chunk = &vals[i..n.min(i + 128)];
        let mut blk = [0i64; 128];
        let mut min = i64::MAX;
        for (b, &v) in blk.iter_mut().zip(chunk) {
            let v: i64 = v.into();
            *b = v.wrapping_sub(prev);
            prev = v;
            min = min.min(*b);
        }
        zigzag(&mut out, min);
        // Deltas less the minimum; the padding past the chunk stays 0 and never raises a width.
        let mut u = [0u64; 128];
        for (x, &d) in u.iter_mut().zip(&blk[..chunk.len()]) {
            *x = d.wrapping_sub(min) as u64;
        }
        let used = chunk.len().div_ceil(32);
        let mut widths = [0u8; 4];
        for (m, w) in widths.iter_mut().enumerate().take(used) {
            *w = (64 - u[m * 32..m * 32 + 32].iter().fold(0, |a, &x| a | x).leading_zeros()) as u8;
        }
        out.extend_from_slice(&widths);
        for (m, &w) in widths.iter().enumerate().take(used) {
            pack32(&mut out, &u[m * 32..m * 32 + 32], w as u32);
        }
        i += chunk.len();
    }
    out
}

/// 32 values bit-packed LSB first at width `w`: exactly `4 * w` bytes.
fn pack32(out: &mut Vec<u8>, v: &[u64], w: u32) {
    if w == 0 {
        return;
    }
    out.reserve(4 * w as usize);
    if w <= 32 {
        let (mut acc, mut bits) = (0u64, 0);
        for &x in v {
            acc |= x << bits;
            bits += w;
            if bits >= 32 {
                out.extend_from_slice(&(acc as u32).to_le_bytes());
                acc >>= 32;
                bits -= 32;
            }
        }
    } else {
        let (mut acc, mut bits) = (0u128, 0);
        for &x in v {
            acc |= (x as u128) << bits;
            bits += w;
            while bits >= 32 {
                out.extend_from_slice(&(acc as u32).to_le_bytes());
                acc >>= 32;
                bits -= 32;
            }
        }
    }
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn zigzag(out: &mut Vec<u8>, v: i64) {
    varint(out, ((v << 1) ^ (v >> 63)) as u64);
}

/// Thrift compact protocol, the parts Parquet metadata needs.
struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn zigzag(&mut self, v: i64) {
        zigzag(&mut self.buf, v);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            self.buf.push((delta as u8) << 4 | ty);
        } else {
            self.buf.push(ty);
            let v = id as i64;
            varint(&mut self.buf, ((v << 1) ^ (v >> 63)) as u64);
        }
        *last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zigzag(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.field(id, 8);
        varint(&mut self.buf, v.len() as u64);
        self.buf.extend_from_slice(v);
    }
    fn begin_struct(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    /// A struct that is a list element: no field header.
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.buf.push((n as u8) << 4 | elem);
        } else {
            self.buf.push(0xF0 | elem);
            varint(&mut self.buf, n as u64);
        }
    }
}

fn physical(kind: Kind) -> i32 {
    match kind {
        Kind::Int32 | Kind::Date => 1,
        Kind::Int64 => 2,
        Kind::Str => 6,
    }
}

fn encoding(c: &Column) -> i32 {
    match (c.enc, c.kind) {
        (Enc::Plain, _) => 0,
        (Enc::Delta, Kind::Str) => 6, // DELTA_LENGTH_BYTE_ARRAY
        (Enc::Delta, _) => 5,         // DELTA_BINARY_PACKED
    }
}

/// Writes one file holding `rows` rows of `cols`: headers and footer built in memory, the bodies written from
/// where they are, all in one `writev`.
pub fn write(path: &Path, rows: usize, cols: &[Column]) -> std::io::Result<()> {
    let mut heads = Vec::new(); // page header per column
    let mut chunks = Vec::new(); // (offset, size) of each column's page
    let mut off = 4usize;
    if rows > 0 {
        for c in cols {
            let size: usize = c.parts.iter().map(|p| p.len()).sum();
            let mut h = Thrift::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, size as i32);
            h.i32(3, size as i32);
            h.begin_struct(5); // DataPageHeader
            h.i32(1, rows as i32);
            h.i32(2, encoding(c));
            h.i32(3, 3); // definition levels: RLE
            h.i32(4, 3); // repetition levels: RLE
            h.end_struct();
            h.end_struct();
            let total = h.buf.len() + size;
            chunks.push((off as i64, total as i64));
            off += total;
            heads.push(h.buf);
        }
    }
    let mut t = Thrift::new(); // FileMetaData
    t.i32(1, 1);
    t.list(2, 12, cols.len() + 1); // schema
    t.begin_elem();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end_struct();
    for c in cols {
        t.begin_elem();
        t.i32(1, physical(c.kind));
        t.i32(3, c.optional as i32); // REQUIRED / OPTIONAL
        t.bin(4, c.name.as_bytes());
        let (converted, logical) = match c.kind {
            Kind::Str => (Some(0), Some(1)), // UTF8, StringType
            Kind::Date => (Some(6), Some(6)), // DATE, DateType
            _ => (None, None),
        };
        if let (Some(cv), Some(lg)) = (converted, logical) {
            t.i32(6, cv);
            t.begin_struct(10);
            t.begin_struct(lg);
            t.end_struct();
            t.end_struct();
        }
        t.end_struct();
    }
    t.i64(3, rows as i64);
    t.list(4, 12, chunks.len().min(1)); // row groups
    if !chunks.is_empty() {
        t.begin_elem();
        t.list(1, 12, cols.len());
        for (c, &(off, size)) in cols.iter().zip(&chunks) {
            t.begin_elem();
            t.i64(2, off);
            t.begin_struct(3); // ColumnMetaData
            t.i32(1, physical(c.kind));
            t.list(2, 5, 2); // encodings: the data encoding, RLE
            t.zigzag(encoding(c) as i64);
            t.zigzag(3);
            t.list(3, 8, 1); // path_in_schema
            varint(&mut t.buf, c.name.len() as u64);
            t.buf.extend_from_slice(c.name.as_bytes());
            t.i32(4, 0); // UNCOMPRESSED
            t.i64(5, rows as i64);
            t.i64(6, size);
            t.i64(7, size);
            t.i64(9, off);
            t.end_struct();
            t.end_struct();
        }
        t.i64(2, chunks.iter().map(|c| c.1).sum());
        t.i64(3, rows as i64);
        t.end_struct();
    }
    t.bin(6, b"pipeline");
    t.end_struct();
    let len = (t.buf.len() as u32).to_le_bytes();
    let mut iov: Vec<IoSlice> = vec![IoSlice::new(b"PAR1")];
    if rows > 0 {
        for (c, h) in cols.iter().zip(&heads) {
            iov.push(IoSlice::new(h));
            iov.extend(c.parts.iter().map(|p| IoSlice::new(p)));
        }
    }
    iov.extend([IoSlice::new(&t.buf), IoSlice::new(&len), IoSlice::new(b"PAR1")]);
    let mut f = std::fs::File::create(path)?;
    let mut bufs = &mut iov[..];
    while !bufs.is_empty() {
        let n = f.write_vectored(bufs)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut bufs, n);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes sample tables under target/parquet-test{,-empty}; `scripts/check_parquet.py` reads them back.
    /// The delta-encoded user table spans several 128-value blocks with negative and wide deltas.
    #[test]
    fn sample_tables() {
        for (dir, rows) in [("target/parquet-test", 300usize), ("target/parquet-test-empty", 0)] {
            let (mut idlen, mut idbytes, mut toplen, mut topbytes, mut bits) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), vec![0u8; rows.div_ceil(8)]);
            let (mut ids, mut days, mut sess, mut rev) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for i in 0..rows {
                let id = [&b"u1"[..], b"A\tB", "é".as_bytes()][i % 3];
                put_str(&mut ids, id);
                idlen.push(id.len() as i32);
                idbytes.extend_from_slice(id);
                if i % 2 == 0 {
                    let c = [&b"shoes"[..], b"", b"unknown"][i % 3];
                    toplen.push(c.len() as i32);
                    topbytes.extend_from_slice(c);
                    bits[i / 8] |= 1 << (i % 8);
                }
                days.push(20730 + (i as i32 % 7) - 3);
                sess.push((i as i32 * 7919) % 1000);
                rev.push(if i % 5 == 0 { (i as i64) << 40 } else { -(i as i64) });
            }
            let (idl, topl, dayd, sessd, revd) = (
                delta(&idlen),
                delta(&toplen),
                delta(&days),
                delta(&sess),
                delta(&rev),
            );
            let def = def_levels(&bits, rows);
            let user = [
                Column { name: "user_id", kind: Kind::Str, enc: Enc::Delta, optional: false, parts: &[&idl, &idbytes] },
                Column { name: "day", kind: Kind::Date, enc: Enc::Delta, optional: false, parts: &[&dayd] },
                Column { name: "sessions", kind: Kind::Int32, enc: Enc::Delta, optional: false, parts: &[&sessd] },
                Column { name: "events", kind: Kind::Int32, enc: Enc::Plain, optional: false, parts: &[plain(&sess)] },
                Column { name: "revenue_cents", kind: Kind::Int64, enc: Enc::Delta, optional: false, parts: &[&revd] },
                Column { name: "top_category", kind: Kind::Str, enc: Enc::Delta, optional: true, parts: &[&def, &topl, &topbytes] },
            ];
            let camp = [
                Column { name: "campaign_id", kind: Kind::Str, enc: Enc::Plain, optional: false, parts: &[&ids] },
                Column { name: "channel", kind: Kind::Str, enc: Enc::Plain, optional: false, parts: &[&ids] },
                Column { name: "day", kind: Kind::Date, enc: Enc::Plain, optional: false, parts: &[plain(&days)] },
                Column { name: "sessions", kind: Kind::Int64, enc: Enc::Plain, optional: false, parts: &[plain(&rev)] },
                Column { name: "purchasing_sessions", kind: Kind::Int64, enc: Enc::Plain, optional: false, parts: &[plain(&rev)] },
                Column { name: "revenue_cents", kind: Kind::Int64, enc: Enc::Plain, optional: false, parts: &[plain(&rev)] },
                Column { name: "assisted_sessions", kind: Kind::Int64, enc: Enc::Plain, optional: false, parts: &[plain(&rev)] },
            ];
            for (table, cols) in [("user_daily", &user[..]), ("campaign_daily", &camp[..])] {
                let d = Path::new(dir).join(table);
                std::fs::create_dir_all(&d).unwrap();
                write(&d.join("part-0.parquet"), rows, cols).unwrap();
            }
        }
    }
}
