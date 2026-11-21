//! A minimal Parquet writer: one row group (none for 0 rows), one PLAIN uncompressed v1 data page per column,
//! Thrift compact footer. Just enough for the two output tables; `scripts/check_parquet.py` reads the result
//! back with polars, pyarrow and duckdb.
use std::io::Write;
use std::path::Path;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Int32,
    Int64,
    Date,
    Str,
}

/// `body` is the finished page body: the definition levels (if optional) followed by the PLAIN values.
pub struct Column<'a> {
    pub name: &'static str,
    pub kind: Kind,
    pub optional: bool,
    pub body: &'a [u8],
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

/// Definition levels of an optional column: `[i32 byte length][one bit-packed run of width 1]`, in groups of 8.
pub fn def_levels(present: &[bool]) -> Vec<u8> {
    let groups = present.len().div_ceil(8);
    let mut run = Vec::new();
    varint(&mut run, (groups as u64) << 1 | 1);
    let mut out = Vec::with_capacity(4 + run.len() + groups);
    out.extend_from_slice(&((run.len() + groups) as u32).to_le_bytes());
    out.extend_from_slice(&run);
    for g in present.chunks(8) {
        out.push(g.iter().enumerate().fold(0u8, |b, (j, &p)| b | (p as u8) << j));
    }
    out
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
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
        varint(&mut self.buf, ((v << 1) ^ (v >> 63)) as u64);
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

/// Writes one file holding `rows` rows of `cols`, built in memory and written with one call.
pub fn write(path: &Path, rows: usize, cols: &[Column]) -> std::io::Result<()> {
    let mut f = Vec::with_capacity(cols.iter().map(|c| c.body.len()).sum::<usize>() + 4096);
    f.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new(); // (offset, size) of each column's page
    if rows > 0 {
        for c in cols {
            let off = f.len();
            let mut h = Thrift::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, c.body.len() as i32);
            h.i32(3, c.body.len() as i32);
            h.begin_struct(5); // DataPageHeader
            h.i32(1, rows as i32);
            h.i32(2, 0); // PLAIN
            h.i32(3, 3); // definition levels: RLE
            h.i32(4, 3); // repetition levels: RLE
            h.end_struct();
            h.end_struct();
            f.extend_from_slice(&h.buf);
            f.extend_from_slice(c.body);
            chunks.push((off as i64, (f.len() - off) as i64));
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
            t.list(2, 5, 2); // encodings: PLAIN, RLE
            t.zigzag(0);
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
    f.extend_from_slice(&t.buf);
    f.extend_from_slice(&(t.buf.len() as u32).to_le_bytes());
    f.extend_from_slice(b"PAR1");
    std::fs::File::create(path)?.write_all(&f)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes sample tables under target/parquet-test{,-empty}; `scripts/check_parquet.py` reads them back.
    #[test]
    fn sample_tables() {
        for (dir, rows) in [("target/parquet-test", 3usize), ("target/parquet-test-empty", 0)] {
            let (mut ids, mut cats) = (Vec::new(), Vec::new());
            let present = [true, false, true];
            for (i, &p) in present.iter().enumerate().take(rows) {
                put_str(&mut ids, [&b"u1"[..], b"A\tB", "é".as_bytes()][i]);
                if p {
                    put_str(&mut cats, [&b"shoes"[..], b"", b"unknown"][i]);
                }
            }
            let mut top = def_levels(&present[..rows]);
            top.extend_from_slice(&cats);
            let (days, sess, rev) = (vec![20730i32; rows], vec![1u32; rows], vec![1500i64; rows]);
            let user = [
                Column { name: "user_id", kind: Kind::Str, optional: false, body: &ids },
                Column { name: "day", kind: Kind::Date, optional: false, body: plain(&days) },
                Column { name: "sessions", kind: Kind::Int32, optional: false, body: plain(&sess) },
                Column { name: "events", kind: Kind::Int32, optional: false, body: plain(&sess) },
                Column { name: "revenue_cents", kind: Kind::Int64, optional: false, body: plain(&rev) },
                Column { name: "top_category", kind: Kind::Str, optional: true, body: &top },
            ];
            let camp = [
                Column { name: "campaign_id", kind: Kind::Str, optional: false, body: &ids },
                Column { name: "channel", kind: Kind::Str, optional: false, body: &ids },
                Column { name: "day", kind: Kind::Date, optional: false, body: plain(&days) },
                Column { name: "sessions", kind: Kind::Int64, optional: false, body: plain(&rev) },
                Column { name: "purchasing_sessions", kind: Kind::Int64, optional: false, body: plain(&rev) },
                Column { name: "revenue_cents", kind: Kind::Int64, optional: false, body: plain(&rev) },
                Column { name: "assisted_sessions", kind: Kind::Int64, optional: false, body: plain(&rev) },
            ];
            for (table, cols) in [("user_daily", &user[..]), ("campaign_daily", &camp[..])] {
                let d = Path::new(dir).join(table);
                std::fs::create_dir_all(&d).unwrap();
                write(&d.join("part-0.parquet"), rows, cols).unwrap();
            }
        }
    }
}
