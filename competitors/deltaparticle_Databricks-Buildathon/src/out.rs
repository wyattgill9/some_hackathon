//! A minimal Parquet writer: one row group, one uncompressed PLAIN data page
//! (format v1) per column, and the Thrift compact-protocol footer. That is all
//! the two output tables need, and it costs almost nothing per row.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub enum Col {
    /// Required strings: concatenated bytes and end offsets.
    Str(Vec<u8>, Vec<u32>),
    /// Optional strings: bytes and ends of present values, a def level per row.
    OptStr(Vec<u8>, Vec<u32>, Vec<i16>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

#[derive(Clone, Copy)]
pub enum Logical {
    None,
    String,
    Date,
}

pub struct Field {
    pub name: &'static str,
    pub optional: bool,
    pub logical: Logical,
}

pub const USER_SCHEMA: &[Field] = &[
    Field { name: "user_id", optional: false, logical: Logical::String },
    Field { name: "day", optional: false, logical: Logical::Date },
    Field { name: "sessions", optional: false, logical: Logical::None },
    Field { name: "events", optional: false, logical: Logical::None },
    Field { name: "revenue_cents", optional: false, logical: Logical::None },
    Field { name: "top_category", optional: true, logical: Logical::String },
];

pub const CAMPAIGN_SCHEMA: &[Field] = &[
    Field { name: "campaign_id", optional: false, logical: Logical::String },
    Field { name: "channel", optional: false, logical: Logical::String },
    Field { name: "day", optional: false, logical: Logical::Date },
    Field { name: "sessions", optional: false, logical: Logical::None },
    Field { name: "purchasing_sessions", optional: false, logical: Logical::None },
    Field { name: "revenue_cents", optional: false, logical: Logical::None },
    Field { name: "assisted_sessions", optional: false, logical: Logical::None },
];

// Parquet enums.
const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const CONVERTED_UTF8: i32 = 0;
const CONVERTED_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;
const CODEC_NONE: i32 = 0;
const PAGE_DATA: i32 = 0;

// Thrift compact protocol types.
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

/// Thrift compact-protocol encoder (just what the footer and page headers use).
struct Thrift {
    buf: Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

impl Thrift {
    fn new() -> Self {
        Self { buf: Vec::new(), last: 0, stack: Vec::new() }
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
        let delta = id - self.last;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
        self.last = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn binary(&mut self, id: i16, v: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        self.list_header(elem, n);
    }
    fn list_header(&mut self, elem: u8, n: usize) {
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn begin(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }
    fn struct_field(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.begin();
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.last = self.stack.pop().unwrap();
    }
}

/// Encode one column's data page body; returns (body, physical type).
fn page_body(col: &Col, rows: usize) -> (Vec<u8>, i32) {
    fn strings(out: &mut Vec<u8>, buf: &[u8], ends: &[u32]) {
        out.reserve(buf.len() + 4 * ends.len());
        let mut start = 0usize;
        for &e in ends {
            let e = e as usize;
            out.extend_from_slice(&((e - start) as u32).to_le_bytes());
            out.extend_from_slice(&buf[start..e]);
            start = e;
        }
    }
    let mut out = Vec::new();
    match col {
        Col::Str(buf, ends) => {
            strings(&mut out, buf, ends);
            (out, BYTE_ARRAY)
        }
        Col::OptStr(buf, ends, defs) => {
            // Definition levels: 4-byte length, then one bit-packed run
            // (RLE/bit-packing hybrid, bit width 1, 8 values per byte).
            let groups = rows.div_ceil(8);
            let mut rle = Thrift::new();
            rle.varint(((groups as u64) << 1) | 1);
            let mut levels = rle.buf;
            levels.resize(levels.len() + groups, 0);
            let base = levels.len() - groups;
            for (i, &d) in defs.iter().enumerate() {
                if d != 0 {
                    levels[base + i / 8] |= 1 << (i % 8);
                }
            }
            out.extend_from_slice(&(levels.len() as u32).to_le_bytes());
            out.extend_from_slice(&levels);
            strings(&mut out, buf, ends);
            (out, BYTE_ARRAY)
        }
        Col::I32(v) => {
            out.reserve(v.len() * 4);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            (out, INT32)
        }
        Col::I64(v) => {
            out.reserve(v.len() * 8);
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            (out, INT64)
        }
    }
}

fn col_rows(col: &Col) -> usize {
    match col {
        Col::Str(_, ends) => ends.len(),
        Col::OptStr(_, _, defs) => defs.len(),
        Col::I32(v) => v.len(),
        Col::I64(v) => v.len(),
    }
}

pub fn write(path: &Path, schema: &[Field], cols: Vec<Col>) -> std::io::Result<()> {
    assert_eq!(schema.len(), cols.len(), "column count");
    let rows = cols.first().map_or(0, col_rows);
    let mut file = BufWriter::with_capacity(1 << 22, File::create(path)?);
    file.write_all(b"PAR1")?;
    let mut offset = 4u64;
    // (physical type, chunk offset, chunk size) per column.
    let mut chunks: Vec<(i32, u64, u64)> = Vec::with_capacity(cols.len());
    if rows > 0 {
        for (field, col) in schema.iter().zip(&cols) {
            debug_assert_eq!(col_rows(col), rows);
            let (body, ty) = page_body(col, rows);
            let mut h = Thrift::new();
            h.begin();
            h.i32(1, PAGE_DATA);
            h.i32(2, body.len() as i32);
            h.i32(3, body.len() as i32);
            h.struct_field(5);
            h.i32(1, rows as i32);
            h.i32(2, ENC_PLAIN);
            h.i32(3, ENC_RLE);
            h.i32(4, ENC_RLE);
            h.end();
            h.end();
            let _ = field;
            file.write_all(&h.buf)?;
            file.write_all(&body)?;
            let size = (h.buf.len() + body.len()) as u64;
            chunks.push((ty, offset, size));
            offset += size;
        }
    }
    // Footer: FileMetaData.
    let mut t = Thrift::new();
    t.begin();
    t.i32(1, 1);
    t.list(2, T_STRUCT, schema.len() + 1);
    t.begin();
    t.binary(4, b"schema");
    t.i32(5, schema.len() as i32);
    t.end();
    for (field, col) in schema.iter().zip(&cols) {
        let ty = match col {
            Col::Str(..) | Col::OptStr(..) => BYTE_ARRAY,
            Col::I32(_) => INT32,
            Col::I64(_) => INT64,
        };
        t.begin();
        t.i32(1, ty);
        t.i32(3, if field.optional { OPTIONAL } else { REQUIRED });
        t.binary(4, field.name.as_bytes());
        match field.logical {
            Logical::String => t.i32(6, CONVERTED_UTF8),
            Logical::Date => t.i32(6, CONVERTED_DATE),
            Logical::None => {}
        }
        t.end();
    }
    t.i64(3, rows as i64);
    if rows > 0 {
        t.list(4, T_STRUCT, 1);
        t.begin();
        t.list(1, T_STRUCT, chunks.len());
        for (field, &(ty, off, size)) in schema.iter().zip(&chunks) {
            t.begin();
            t.i64(2, off as i64);
            t.struct_field(3);
            t.i32(1, ty);
            t.list(2, T_I32, 2);
            t.zigzag(ENC_PLAIN as i64);
            t.zigzag(ENC_RLE as i64);
            t.list(3, T_BINARY, 1);
            t.varint(field.name.len() as u64);
            t.buf.extend_from_slice(field.name.as_bytes());
            t.i32(4, CODEC_NONE);
            t.i64(5, rows as i64);
            t.i64(6, size as i64);
            t.i64(7, size as i64);
            t.i64(9, off as i64);
            t.end();
            t.end();
        }
        t.i64(2, chunks.iter().map(|c| c.2 as i64).sum());
        t.i64(3, rows as i64);
        t.end();
    } else {
        t.list(4, T_STRUCT, 0);
    }
    t.binary(6, b"shopping-sessions");
    t.end();
    file.write_all(&t.buf)?;
    file.write_all(&(t.buf.len() as u32).to_le_bytes())?;
    file.write_all(b"PAR1")?;
    file.flush()
}
