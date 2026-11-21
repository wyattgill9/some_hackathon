//! A minimal Parquet writer: one row group, one PLAIN, uncompressed data page
//! per column. That is all two flat tables need, it is the most widely read
//! corner of the format, and writing it is little more than copying the columns.
//!
//! The file layout and the Thrift compact encoding of the footer follow
//! https://parquet.apache.org/docs/file-format/ and parquet.thrift.

use std::io::Write;
use std::path::Path;

// parquet.thrift: Type
const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
// parquet.thrift: ConvertedType
const UTF8: i32 = 0;
const DATE: i32 = 6;
// parquet.thrift: Encoding
const PLAIN: i32 = 0;
const RLE: i32 = 3;

// Thrift compact protocol type codes.
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Text,
    Date,
    Int32,
    Int64,
}

struct Column {
    name: &'static str,
    kind: Kind,
    optional: bool,
    values: i64,
    offset: i64,
    size: i64,
}

/// A Thrift compact-protocol writer, enough for Parquet's page headers and footer.
struct Thrift<'a> {
    out: &'a mut Vec<u8>,
    last: i16,
    stack: Vec<i16>,
}

impl<'a> Thrift<'a> {
    fn new(out: &'a mut Vec<u8>) -> Self {
        Self { out, last: 0, stack: Vec::new() }
    }

    fn varint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.out.push(value as u8 | 0x80);
            value >>= 7;
        }
        self.out.push(value as u8);
    }

    fn zigzag(&mut self, value: i64) {
        self.varint(((value << 1) ^ (value >> 63)) as u64);
    }

    fn header(&mut self, id: i16, kind: u8) {
        let delta = id - self.last;
        if (1..=15).contains(&delta) {
            self.out.push(((delta as u8) << 4) | kind);
        } else {
            self.out.push(kind);
            self.zigzag(i64::from(id));
        }
        self.last = id;
    }

    fn i32(&mut self, id: i16, value: i32) {
        self.header(id, T_I32);
        self.zigzag(i64::from(value));
    }

    fn i64(&mut self, id: i16, value: i64) {
        self.header(id, T_I64);
        self.zigzag(value);
    }

    fn string(&mut self, id: i16, value: &str) {
        self.header(id, T_BINARY);
        self.varint(value.len() as u64);
        self.out.extend_from_slice(value.as_bytes());
    }

    fn list(&mut self, id: i16, element: u8, len: usize) {
        self.header(id, T_LIST);
        if len < 15 {
            self.out.push(((len as u8) << 4) | element);
        } else {
            self.out.push(0xf0 | element);
            self.varint(len as u64);
        }
    }

    /// Starts a struct that is a field of the current one.
    fn begin(&mut self, id: i16) {
        self.header(id, T_STRUCT);
        self.stack.push(self.last);
        self.last = 0;
    }

    /// Starts a struct that is a list element.
    fn element(&mut self) {
        self.stack.push(self.last);
        self.last = 0;
    }

    fn end(&mut self) {
        self.out.push(0);
        self.last = self.stack.pop().unwrap();
    }
}

/// PLAIN encoding of fixed-width integers is their little-endian bytes: on the
/// little-endian targets this builds for, the slice itself.
#[cfg(target_endian = "little")]
fn little_endian<T: Copy>(values: &[T]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(values.as_ptr() as *const u8, std::mem::size_of_val(values)) }
}

/// Builds one Parquet file in memory. Add every column with the same number of rows, then `write`.
pub struct File {
    bytes: Vec<u8>,
    columns: Vec<Column>,
    rows: i64,
}

impl File {
    pub fn new(rows: usize, capacity: usize) -> Self {
        let mut bytes = Vec::with_capacity(capacity + 1024);
        bytes.extend_from_slice(b"PAR1");
        Self { bytes, columns: Vec::new(), rows: rows as i64 }
    }

    /// A column whose page body `fill` appends: for a required column the PLAIN values, nothing else.
    fn column(&mut self, name: &'static str, kind: Kind, optional: bool, body_len: usize, fill: impl FnOnce(&mut Vec<u8>)) {
        let offset = self.bytes.len();
        if self.rows > 0 {
            let mut header = Thrift::new(&mut self.bytes);
            header.i32(1, 0); // DATA_PAGE
            header.i32(2, body_len as i32);
            header.i32(3, body_len as i32);
            header.begin(5);
            header.i32(1, self.rows as i32);
            header.i32(2, PLAIN);
            header.i32(3, RLE);
            header.i32(4, RLE);
            header.end();
            header.out.push(0);
            let body = self.bytes.len();
            fill(&mut self.bytes);
            assert_eq!(self.bytes.len() - body, body_len, "page body of {name}");
        }
        let size = (self.bytes.len() - offset) as i64;
        self.columns.push(Column { name, kind, optional, values: self.rows, offset: offset as i64, size });
    }

    pub fn int32(&mut self, name: &'static str, kind: Kind, values: &[i32]) {
        assert_eq!(values.len() as i64, self.rows);
        self.column(name, kind, false, values.len() * 4, |out| out.extend_from_slice(little_endian(values)));
    }

    pub fn int64(&mut self, name: &'static str, values: &[i64]) {
        assert_eq!(values.len() as i64, self.rows);
        self.column(name, Kind::Int64, false, values.len() * 8, |out| out.extend_from_slice(little_endian(values)));
    }

    /// Non-negative counts: 32-bit when they all fit (the checker takes any integer width), 64-bit otherwise.
    pub fn counts(&mut self, name: &'static str, values: &[u32]) {
        assert_eq!(values.len() as i64, self.rows);
        if values.iter().all(|&v| v <= i32::MAX as u32) {
            self.column(name, Kind::Int32, false, values.len() * 4, |out| out.extend_from_slice(little_endian(values)));
        } else {
            let wide: Vec<i64> = values.iter().map(|&v| i64::from(v)).collect();
            self.int64(name, &wide);
        }
    }

    /// A required string column. `plain` is the values already PLAIN-encoded: a 4-byte length, then the bytes, each.
    pub fn text(&mut self, name: &'static str, plain: &[u8]) {
        self.column(name, Kind::Text, false, plain.len(), |out| out.extend_from_slice(plain));
    }

    /// A nullable string column: `present[i]` says whether row i has a value; `plain` holds the present ones.
    pub fn optional_text(&mut self, name: &'static str, present: &[bool], plain: &[u8]) {
        assert_eq!(present.len() as i64, self.rows);
        // Definition levels, 1 bit each, as one bit-packed run of the RLE/bit-packing hybrid.
        let groups = present.len().div_ceil(8);
        let mut levels = Vec::with_capacity(groups + 8);
        let mut run = (groups as u64) << 1 | 1;
        while run >= 0x80 {
            levels.push(run as u8 | 0x80);
            run >>= 7;
        }
        levels.push(run as u8);
        for group in present.chunks(8) {
            let mut byte = 0u8;
            for (bit, &is) in group.iter().enumerate() {
                byte |= u8::from(is) << bit;
            }
            levels.push(byte);
        }
        self.column(name, Kind::Text, true, 4 + levels.len() + plain.len(), |out| {
            out.extend_from_slice(&(levels.len() as u32).to_le_bytes());
            out.extend_from_slice(&levels);
            out.extend_from_slice(plain);
        });
    }

    pub fn write(mut self, path: &Path) {
        let footer = self.bytes.len();
        let total: i64 = self.columns.iter().map(|c| c.size).sum();
        let mut meta = Thrift::new(&mut self.bytes);
        meta.i32(1, 1); // version
        meta.list(2, T_STRUCT, self.columns.len() + 1);
        meta.element();
        meta.i32(3, 0); // REQUIRED
        meta.string(4, "schema");
        meta.i32(5, self.columns.len() as i32);
        meta.end();
        for column in &self.columns {
            meta.element();
            meta.i32(1, match column.kind {
                Kind::Text => BYTE_ARRAY,
                Kind::Date | Kind::Int32 => INT32,
                Kind::Int64 => INT64,
            });
            meta.i32(3, i32::from(column.optional));
            meta.string(4, column.name);
            match column.kind {
                Kind::Text => {
                    meta.i32(6, UTF8);
                    meta.begin(10); // LogicalType
                    meta.begin(1); // STRING
                    meta.end();
                    meta.end();
                }
                Kind::Date => {
                    meta.i32(6, DATE);
                    meta.begin(10);
                    meta.begin(6); // DATE
                    meta.end();
                    meta.end();
                }
                Kind::Int32 | Kind::Int64 => {}
            }
            meta.end();
        }
        meta.i64(3, self.rows);
        if self.rows == 0 {
            meta.list(4, T_STRUCT, 0);
        } else {
            meta.list(4, T_STRUCT, 1);
            meta.element(); // RowGroup
            meta.list(1, T_STRUCT, self.columns.len());
            for column in &self.columns {
                meta.element(); // ColumnChunk
                meta.i64(2, column.offset);
                meta.begin(3); // ColumnMetaData
                meta.i32(1, match column.kind {
                    Kind::Text => BYTE_ARRAY,
                    Kind::Date | Kind::Int32 => INT32,
                    Kind::Int64 => INT64,
                });
                meta.list(2, T_I32, 2);
                meta.zigzag(i64::from(PLAIN));
                meta.zigzag(i64::from(RLE));
                meta.list(3, T_BINARY, 1);
                meta.varint(column.name.len() as u64);
                meta.out.extend_from_slice(column.name.as_bytes());
                meta.i32(4, 0); // UNCOMPRESSED
                meta.i64(5, column.values);
                meta.i64(6, column.size);
                meta.i64(7, column.size);
                meta.i64(9, column.offset);
                meta.end();
                meta.end();
            }
            meta.i64(2, total);
            meta.i64(3, self.rows);
            meta.end();
        }
        meta.string(6, "shopping-sessions");
        meta.out.push(0);
        let footer_len = (self.bytes.len() - footer) as u32;
        self.bytes.extend_from_slice(&footer_len.to_le_bytes());
        self.bytes.extend_from_slice(b"PAR1");
        use std::os::unix::fs::OpenOptionsExt;
        let written = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o666)
            .open(path)
            .and_then(|mut file| file.write_all(&self.bytes));
        if let Err(e) = written {
            eprintln!("pipeline: cannot write {}: {e}", path.display());
            std::process::exit(1);
        }
    }
}
