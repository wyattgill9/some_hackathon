//! Reads one byte range of a CSV file and hands every line that starts in it,
//! split at its commas, to a sink.
//!
//! A file is cut into ranges so several threads can share it. A line belongs to
//! the range its first byte is in: a range skips the line it starts inside (or
//! the header, for the range at offset 0) and reads past its own end to finish
//! its last line. That is exact because only `\n` ends a line.

use std::fs::File;
use std::os::unix::fs::FileExt;

use crate::scan::Index;

/// Bytes kept readable before the data, so `data[-1]` is always in bounds.
const FRONT: usize = 64;
/// Bytes read per call: small enough to stay in the L2 cache while it is parsed.
const BLOCK: usize = 1 << 18;

pub struct ReadBuf {
    bytes: Vec<u8>,
    index: Index,
    /// Bytes per read: BLOCK, unless a test asks for something tiny to put block edges everywhere.
    block: usize,
}

impl ReadBuf {
    pub fn new() -> Self {
        let block = std::env::var("SS_BLOCK_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(BLOCK).clamp(16, 1 << 24);
        let capacity = 2 * block;
        Self { bytes: vec![0; FRONT + capacity + 128], index: Index::new(capacity + 64), block }
    }

    /// Room for `len` bytes of data plus a newline and the vector overread.
    fn reserve(&mut self, len: usize) {
        if FRONT + len + 128 > self.bytes.len() {
            let capacity = (len + 1).next_power_of_two();
            self.bytes.resize(FRONT + capacity + 128, 0);
            self.index = Index::new(capacity + 64);
        }
    }
}

pub trait Lines {
    /// Commas in a well-formed line: one fewer than its fields.
    const COMMAS: usize;

    /// One line with the right number of commas, which holds `quotes` double
    /// quotes. `delims[0]` is the offset of the newline before it (`u32::MAX`,
    /// which is -1, for the line at offset 0), `delims[1..=COMMAS]` are its
    /// commas and `delims[COMMAS + 1]` is its own newline.
    unsafe fn line(&mut self, data: *const u8, delims: *const u32, quotes: u32);
}

/// A field's bounds once the quotes around it, if it is wrapped in a pair, are
/// left out; and 1 if it was wrapped. By value, so the bounds stay in registers.
#[inline(always)]
pub unsafe fn unquoted(data: *const u8, start: usize, end: usize) -> (usize, usize, u32) {
    let wrapped = (end - start >= 2) & (*data.add(start) == b'"') & (*data.add(end).sub(1) == b'"');
    (start + wrapped as usize, end - wrapped as usize, wrapped as u32)
}

/// Where each field of a line is, the quotes around the wrapped ones left out.
#[repr(C, align(32))]
pub struct Fields {
    pub start: [u32; 8],
    pub len: [u32; 8],
}

impl Fields {
    #[inline(always)]
    pub fn new() -> Self {
        Self { start: [0; 8], len: [0; 8] }
    }

    #[inline(always)]
    pub fn at(&self, field: usize) -> usize {
        self.start[field] as usize
    }

    #[inline(always)]
    pub fn len(&self, field: usize) -> usize {
        self.len[field] as usize
    }
}

/// Splits a line of `N` fields (at most 7) at its delimiters, `delims` as
/// `Lines::line` gets them, and returns how many fields were wrapped in quotes.
/// All fields at once: the four bytes around each delimiter hold the last
/// character of the field before it and the first of the field after it, and
/// with those side by side the seven "is this field wrapped" questions are a
/// handful of vector instructions instead of seven rounds of loads and compares.
/// Reads `delims[0..9]`, and the 4 bytes from one before each of those offsets.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
pub unsafe fn split<const N: usize>(data: *const u8, delims: *const u32, fields: &mut Fields) -> u32 {
    use std::arch::x86_64::*;
    let around = |i: usize| (data.offset(*delims.add(i) as i32 as isize - 1) as *const i32).read_unaligned();
    let low = _mm_insert_epi32(
        _mm_insert_epi32(_mm_insert_epi32(_mm_cvtsi32_si128(around(0)), around(1), 1), around(2), 2),
        around(3),
        3,
    );
    let high = _mm_insert_epi32(
        _mm_insert_epi32(_mm_insert_epi32(_mm_cvtsi32_si128(around(4)), around(5), 1), around(6), 2),
        around(7),
        3,
    );
    // Lane i: the bytes at delimiter i's offset - 1, +0, +1, +2.
    let quote = _mm256_cmpeq_epi8(_mm256_inserti128_si256(_mm256_castsi128_si256(low), high, 1), _mm256_set1_epi8(b'"' as i8));
    // Field i lies between delimiters i and i + 1: it opens with the byte after
    // the first (byte 2 of lane i) and closes with the byte before the second
    // (byte 0 of lane i + 1).
    let closes = _mm256_permutevar8x32_epi32(quote, _mm256_setr_epi32(1, 2, 3, 4, 5, 6, 7, 7));
    let both = _mm256_and_si256(_mm256_srli_epi32(quote, 16), closes);
    let before = _mm256_loadu_si256(delims as *const __m256i);
    let after = _mm256_loadu_si256(delims.add(1) as *const __m256i);
    let one = _mm256_set1_epi32(1);
    let len = _mm256_sub_epi32(_mm256_sub_epi32(after, before), one);
    // Wrapped: a quote at both ends, and they are two different bytes.
    let wrapped = _mm256_and_si256(_mm256_and_si256(both, _mm256_cmpgt_epi32(len, one)), one);
    _mm256_store_si256(fields.start.as_mut_ptr() as *mut __m256i, _mm256_add_epi32(_mm256_add_epi32(before, one), wrapped));
    _mm256_store_si256(fields.len.as_mut_ptr() as *mut __m256i, _mm256_sub_epi32(len, _mm256_add_epi32(wrapped, wrapped)));
    let bits = _mm256_movemask_ps(_mm256_castsi256_ps(_mm256_slli_epi32(wrapped, 31))) as u32;
    (bits & ((1 << N) - 1)).count_ones()
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
pub unsafe fn split<const N: usize>(data: *const u8, delims: *const u32, fields: &mut Fields) -> u32 {
    let mut count = 0;
    for field in 0..N {
        let start = (*delims.add(field)).wrapping_add(1) as usize;
        let (from, to, wrapped) = unquoted(data, start, *delims.add(field + 1) as usize);
        fields.start[field] = from as u32;
        fields.len[field] = (to - from) as u32;
        count += wrapped;
    }
    count
}

fn read_fully(file: &File, mut into: &mut [u8], mut at: u64) -> usize {
    let mut total = 0;
    while !into.is_empty() {
        match file.read_at(into, at) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                at += n as u64;
                into = &mut into[n..];
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => {
                eprintln!("pipeline: read failed: {e}");
                std::process::abort();
            }
        }
    }
    total
}

/// Feeds `sink` every data line of `file` that starts in `offset..offset + length`.
pub fn walk<L: Lines>(file: &File, offset: u64, length: u64, buf: &mut ReadBuf, sink: &mut L) {
    let range_end = offset + length;
    // Start one byte early: if that byte is a newline, a line starts exactly at `offset`.
    let mut next_read = offset.saturating_sub(1);
    let mut base = next_read; // file offset of data[0]
    let mut len = 0usize; // bytes of data held
    let mut skip = true; // the header, or the line this range starts inside
    let mut at_end = false;
    loop {
        if !at_end {
            let block = buf.block;
            buf.reserve(len + block);
            let got = read_fully(file, &mut buf.bytes[FRONT + len..FRONT + len + block], next_read);
            next_read += got as u64;
            len += got;
            if got < block {
                at_end = true;
                // A last line with no newline is still a line.
                if len > 0 && buf.bytes[FRONT + len - 1] != b'\n' {
                    buf.bytes[FRONT + len] = b'\n';
                    len += 1;
                }
            }
        }
        let data = unsafe { buf.bytes.as_ptr().add(FRONT) };
        unsafe { buf.index.build(data, len) };
        let index = &buf.index;
        let count = index.newline_count;
        let newlines = index.newlines.as_ptr();
        let delims = index.delims();

        let mut line = 0usize; // which newline ends the next line
        let mut start = 0usize; // where the next line starts
        // Which delimiter is the one just before `start`: -1 is the slot in front, the delimiter at offset -1.
        let mut before = -1isize;
        if skip {
            if count == 0 {
                if at_end {
                    return;
                }
                continue; // still inside the line to skip: read on
            }
            let first = unsafe { *newlines };
            start = first as usize + 1;
            line = 1;
            skip = false;
            before = 0;
            unsafe {
                while *delims.offset(before) < first {
                    before += 1;
                }
            }
        }
        // Lines starting at or after `stop` belong to the next range.
        let stop = usize::try_from(range_end - base).unwrap_or(usize::MAX);
        let mut done = false;
        unsafe {
            let mut quotes_before = index.quotes_before(start);
            while line < count {
                if start >= stop {
                    done = true;
                    break;
                }
                let end = *newlines.add(line);
                let quotes_at_end = index.quotes_before(end as usize);
                let quotes = quotes_at_end - quotes_before;
                quotes_before = quotes_at_end;
                // The line's own newline is the delimiter COMMAS + 1 after the one
                // before it exactly when COMMAS commas lie between the two.
                if *delims.offset(before + L::COMMAS as isize + 1) == end {
                    sink.line(data, delims.offset(before), quotes);
                    before += L::COMMAS as isize + 1;
                } else {
                    // Wrong number of fields (an empty line has none): malformed, dropped.
                    before += 1;
                    while *delims.offset(before) < end {
                        before += 1;
                    }
                }
                start = end as usize + 1;
                line += 1;
            }
        }
        if done || at_end || base + start as u64 >= range_end {
            return;
        }
        // Keep the unfinished line and read more behind it.
        buf.bytes.copy_within(FRONT + start..FRONT + len, FRONT);
        len -= start;
        base += start as u64;
    }
}

/// An input file and the ranges it is read in.
pub struct Input {
    pub file: File,
    pub len: u64,
}

/// The `*.csv` files directly in `dir`, opened. A missing folder holds none.
pub fn open_folder(dir: &std::path::Path) -> Vec<Input> {
    let mut paths = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.as_encoded_bytes();
            if name.ends_with(b".csv") && !name.starts_with(b".") {
                paths.push(entry.path());
            }
        }
    }
    paths.sort();
    let mut inputs = Vec::new();
    for path in paths {
        let Ok(meta) = std::fs::metadata(&path) else { continue };
        if !meta.is_file() {
            continue;
        }
        match File::open(&path) {
            Ok(file) => inputs.push(Input { file, len: meta.len() }),
            Err(e) => {
                eprintln!("pipeline: cannot open {}: {e}", path.display());
                std::process::abort();
            }
        }
    }
    inputs
}

/// A test's override of how many bytes each range of a file holds.
pub fn job_bytes() -> Option<u64> {
    std::env::var("SS_JOB_BYTES").ok()?.parse::<u64>().ok().map(|bytes| bytes.max(1))
}

#[derive(Clone, Copy)]
pub struct Job {
    pub file: u32,
    pub offset: u64,
    pub length: u64,
}

/// Cuts every file into ranges of about `target` bytes, largest files first so
/// the long ones are not what the last thread is left holding.
pub fn jobs(inputs: &[Input], target: u64) -> Vec<Job> {
    let mut order: Vec<usize> = (0..inputs.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(inputs[i].len));
    let mut jobs = Vec::new();
    for i in order {
        let len = inputs[i].len;
        let pieces = len.div_ceil(target).max(1);
        let size = len.div_ceil(pieces).max(1);
        let mut offset = 0;
        while offset < len {
            let length = size.min(len - offset);
            jobs.push(Job { file: i as u32, offset, length });
            offset += length;
        }
    }
    jobs
}
