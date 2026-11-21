//! A small Parquet writer: flat schema, PLAIN encoding, uncompressed, one data
//! page per column chunk, Thrift compact metadata.

use std::io::Write;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Int32,
    Int64,
    Date,
    Utf8,
}

/// A column whose page body is already encoded (PLAIN values, preceded by the
/// definition levels for an optional column), given as consecutive byte slices.
pub struct Column<'a> {
    pub name: &'a str,
    pub kind: Kind,
    pub optional: bool,
    pub parts: Vec<&'a [u8]>,
}

pub fn as_bytes<T: Copy>(v: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(v.as_ptr() as *const u8, std::mem::size_of_val(v)) }
}

/// PLAIN BYTE_ARRAY value: 4-byte length then the bytes.
#[inline(always)]
pub fn push_bytes(out: &mut Vec<u8>, v: &[u8]) {
    out.extend_from_slice(&(v.len() as u32).to_le_bytes());
    out.extend_from_slice(v);
}

/// Definition levels for an optional column (bit width 1): 4-byte length,
/// then one bit-packed run.
pub fn encode_def(def: &[bool]) -> Vec<u8> {
    let groups = def.len().div_ceil(8);
    let mut t = Thrift::new();
    t.varint(((groups as u64) << 1) | 1);
    let mut out = Vec::with_capacity(groups + 16);
    out.extend_from_slice(&((t.buf.len() + groups) as u32).to_le_bytes());
    out.extend_from_slice(&t.buf);
    for g in 0..groups {
        let mut b = 0u8;
        for j in 0..8 {
            let i = g * 8 + j;
            if i < def.len() && def[i] {
                b |= 1 << j;
            }
        }
        out.push(b);
    }
    out
}

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let delta = id - last;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
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
        self.varint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }
    fn begin_struct(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn begin_list_elem_struct(&mut self) {
        self.last.push(0);
    }
    fn end_struct(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

fn physical(kind: Kind) -> i32 {
    match kind {
        Kind::Int32 | Kind::Date => 1,
        Kind::Int64 => 2,
        Kind::Utf8 => 6,
    }
}

/// Writes one Parquet file with a single row group holding all `rows`.
pub fn write_file(path: &std::path::Path, rows: usize, cols: &[Column]) -> std::io::Result<()> {
    let mut f = std::io::BufWriter::with_capacity(1 << 16, std::fs::File::create(path)?);
    f.write_all(b"PAR1")?;
    let mut pos = 4i64;
    struct Meta {
        offset: i64,
        size: i64,
    }
    let mut metas = Vec::new();
    for c in cols {
        let body: usize = c.parts.iter().map(|p| p.len()).sum();
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, body as i32);
        h.i32(3, body as i32);
        h.begin_struct(5);
        h.i32(1, rows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3); // RLE
        h.end_struct();
        h.buf.push(0); // end PageHeader
        f.write_all(&h.buf)?;
        for p in &c.parts {
            f.write_all(p)?;
        }
        let size = (h.buf.len() + body) as i64;
        metas.push(Meta { offset: pos, size });
        pos += size;
    }
    let mut t = Thrift::new();
    t.i32(1, 1);
    t.list(2, 12, cols.len() + 1);
    // root
    t.begin_list_elem_struct();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end_struct();
    for c in cols {
        t.begin_list_elem_struct();
        t.i32(1, physical(c.kind));
        t.i32(3, if c.optional { 1 } else { 0 });
        t.bin(4, c.name.as_bytes());
        match c.kind {
            Kind::Utf8 => {
                t.i32(6, 0);
                t.begin_struct(10);
                t.begin_struct(1);
                t.end_struct();
                t.end_struct();
            }
            Kind::Date => {
                t.i32(6, 6);
                t.begin_struct(10);
                t.begin_struct(6);
                t.end_struct();
                t.end_struct();
            }
            _ => {}
        }
        t.end_struct();
    }
    t.i64(3, rows as i64);
    t.list(4, 12, 1);
    t.begin_list_elem_struct();
    t.list(1, 12, cols.len());
    let total: i64 = metas.iter().map(|m| m.size).sum();
    for (c, m) in cols.iter().zip(&metas) {
        t.begin_list_elem_struct();
        t.i64(2, m.offset);
        t.begin_struct(3);
        t.i32(1, physical(c.kind));
        t.list(2, 5, 2);
        t.zigzag(0);
        t.zigzag(3);
        t.list(3, 8, 1);
        t.varint(c.name.len() as u64);
        t.buf.extend_from_slice(c.name.as_bytes());
        t.i32(4, 0);
        t.i64(5, rows as i64);
        t.i64(6, m.size);
        t.i64(7, m.size);
        t.i64(9, m.offset);
        t.end_struct();
        t.end_struct();
    }
    t.i64(2, total);
    t.i64(3, rows as i64);
    t.end_struct();
    t.bin(6, b"sessions");
    t.buf.push(0);
    f.write_all(&t.buf)?;
    f.write_all(&(t.buf.len() as u32).to_le_bytes())?;
    f.write_all(b"PAR1")?;
    f.flush()?;
    Ok(())
}
