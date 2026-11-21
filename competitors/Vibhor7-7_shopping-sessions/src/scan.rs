//! Stage one of the CSV reader: where the newlines, commas and quotes are.
//!
//! rules.md makes this exact and simple. Only `\n` ends a line and every comma
//! separates fields, quoted or not, so both can be found with byte compares, 64
//! bytes at a time, with no state carried between blocks. Quotes are only
//! counted: a line is well formed when its quotes are exactly the ones wrapping
//! whole fields, which stage two checks from the count.

/// Positions found in one buffer.
pub struct Index {
    /// Offsets of every `\n`, ascending.
    pub newlines: Vec<u32>,
    pub newline_count: usize,
    /// One slot of `u32::MAX`, then the offsets of every `,` and `\n`, ascending,
    /// then 8 sentinels of `u32::MAX`. The slot in front stands for a delimiter at
    /// offset -1, so the line at offset 0 has one before it like every other line.
    delims: Vec<u32>,
    /// Quotes before each 64-byte block, and each block's quote bitmap.
    quotes_before: Vec<u32>,
    quote_bits: Vec<u64>,
}

impl Index {
    pub fn new(capacity: usize) -> Self {
        Self {
            newlines: vec![0; capacity + 80],
            newline_count: 0,
            delims: vec![u32::MAX; capacity + 81],
            quotes_before: vec![0; capacity / 64 + 3],
            quote_bits: vec![0; capacity / 64 + 3],
        }
    }

    pub fn capacity(&self) -> usize {
        self.newlines.len() - 80
    }

    /// The first delimiter's slot. The slot before it is readable and holds `u32::MAX`.
    #[inline(always)]
    pub fn delims(&self) -> *const u32 {
        unsafe { self.delims.as_ptr().add(1) }
    }

    /// How many `"` lie before offset `at`.
    #[inline(always)]
    pub fn quotes_before(&self, at: usize) -> u32 {
        unsafe {
            let block = at >> 6;
            let below = (1u64 << (at & 63)) - 1;
            *self.quotes_before.get_unchecked(block) + (*self.quote_bits.get_unchecked(block) & below).count_ones()
        }
    }

    /// Indexes `len` bytes at `data`. The 64 bytes after them must be readable.
    pub unsafe fn build(&mut self, data: *const u8, len: usize) {
        debug_assert!(len <= self.capacity());
        let mut newline_out = self.newlines.as_mut_ptr();
        let mut delim_out = self.delims.as_mut_ptr().add(1);
        let blocks = len.div_ceil(64);
        let mut quotes = 0u32;
        for block in 0..blocks {
            let (mut newlines, mut commas, mut quote_bits) = masks(data.add(block * 64));
            if block + 1 == blocks && len & 63 != 0 {
                let keep = (1u64 << (len & 63)) - 1;
                newlines &= keep;
                commas &= keep;
                quote_bits &= keep;
            }
            *self.quotes_before.get_unchecked_mut(block) = quotes;
            *self.quote_bits.get_unchecked_mut(block) = quote_bits;
            quotes += quote_bits.count_ones();
            let base = (block * 64) as u32;
            newline_out = flatten::<2>(newlines, base, newline_out);
            delim_out = flatten::<8>(commas | newlines, base, delim_out);
        }
        // So `quotes_before(len)` works when len is a multiple of 64.
        *self.quotes_before.get_unchecked_mut(blocks) = quotes;
        *self.quote_bits.get_unchecked_mut(blocks) = 0;
        self.newline_count = newline_out.offset_from(self.newlines.as_ptr()) as usize;
        for i in 0..8 {
            *delim_out.add(i) = u32::MAX;
        }
    }
}

/// Writes the offsets of the set bits. The first `N` are written without a
/// branch (the bits past the last are garbage that the next call overwrites),
/// so the only branch is the rare block with more than `N`.
#[inline(always)]
unsafe fn flatten<const N: usize>(mut bits: u64, base: u32, out: *mut u32) -> *mut u32 {
    let count = bits.count_ones() as usize;
    for i in 0..N {
        *out.add(i) = base + bits.trailing_zeros();
        bits &= bits.wrapping_sub(1);
    }
    if count > N {
        let mut at = N;
        while bits != 0 {
            *out.add(at) = base + bits.trailing_zeros();
            bits &= bits - 1;
            at += 1;
        }
    }
    out.add(count)
}

/// Bitmaps of `\n`, `,` and `"` in the 64 bytes at `p`.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let low = _mm256_loadu_si256(p as *const __m256i);
    let high = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let find = |byte: u8| -> u64 {
        let needle = _mm256_set1_epi8(byte as i8);
        let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(low, needle)) as u32;
        let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(high, needle)) as u32;
        u64::from(a) | (u64::from(b) << 32)
    };
    (find(b'\n'), find(b','), find(b'"'))
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let v = [
        _mm_loadu_si128(p as *const __m128i),
        _mm_loadu_si128(p.add(16) as *const __m128i),
        _mm_loadu_si128(p.add(32) as *const __m128i),
        _mm_loadu_si128(p.add(48) as *const __m128i),
    ];
    let find = |byte: u8| -> u64 {
        let needle = _mm_set1_epi8(byte as i8);
        let mut bits = 0u64;
        for (i, part) in v.iter().enumerate() {
            bits |= u64::from(_mm_movemask_epi8(_mm_cmpeq_epi8(*part, needle)) as u16) << (16 * i);
        }
        bits
    };
    (find(b'\n'), find(b','), find(b'"'))
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    use std::arch::aarch64::*;
    let v = [vld1q_u8(p), vld1q_u8(p.add(16)), vld1q_u8(p.add(32)), vld1q_u8(p.add(48))];
    const WEIGHTS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
    let weights = vld1q_u8(WEIGHTS.as_ptr());
    let find = |byte: u8| -> u64 {
        let needle = vdupq_n_u8(byte);
        let t0 = vandq_u8(vceqq_u8(v[0], needle), weights);
        let t1 = vandq_u8(vceqq_u8(v[1], needle), weights);
        let t2 = vandq_u8(vceqq_u8(v[2], needle), weights);
        let t3 = vandq_u8(vceqq_u8(v[3], needle), weights);
        let sum = vpaddq_u8(vpaddq_u8(t0, t1), vpaddq_u8(t2, t3));
        let sum = vpaddq_u8(sum, sum);
        vgetq_lane_u64(vreinterpretq_u64_u8(sum), 0)
    };
    (find(b'\n'), find(b','), find(b'"'))
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
unsafe fn masks(p: *const u8) -> (u64, u64, u64) {
    masks_plain(p)
}

/// The definition the vector versions are tested against.
#[allow(dead_code)]
unsafe fn masks_plain(p: *const u8) -> (u64, u64, u64) {
    let (mut newlines, mut commas, mut quotes) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let byte = *p.add(i);
        newlines |= u64::from(byte == b'\n') << i;
        commas |= u64::from(byte == b',') << i;
        quotes |= u64::from(byte == b'"') << i;
    }
    (newlines, commas, quotes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small deterministic generator, so the test needs no crate.
    fn next(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[test]
    fn vector_masks_match_the_plain_ones() {
        let mut state = 1;
        for _ in 0..20_000 {
            let mut block = [0u8; 64];
            for byte in &mut block {
                *byte = match next(&mut state) % 9 {
                    0 => b'\n',
                    1 | 2 => b',',
                    3 => b'"',
                    4 => (next(&mut state) & 0xff) as u8,
                    _ => b'a',
                };
            }
            unsafe { assert_eq!(masks(block.as_ptr()), masks_plain(block.as_ptr())) };
        }
    }

    #[test]
    fn index_lists_every_delimiter() {
        let mut state = 7;
        for round in 0..300 {
            let len = (next(&mut state) % 700) as usize + usize::from(round % 3 == 0) * 64;
            let mut data = vec![b'x'; len + 64];
            for byte in data.iter_mut().take(len) {
                *byte = match next(&mut state) % 12 {
                    0 => b'\n',
                    1..=4 => b',',
                    5 => b'"',
                    _ => b'q',
                };
            }
            // Delimiters past `len` must not be reported.
            for byte in data.iter_mut().skip(len) {
                *byte = b',';
            }
            let mut index = Index::new(1024);
            unsafe { index.build(data.as_ptr(), len) };
            let newlines: Vec<u32> = (0..len).filter(|&i| data[i] == b'\n').map(|i| i as u32).collect();
            let delims: Vec<u32> = (0..len).filter(|&i| data[i] == b',' || data[i] == b'\n').map(|i| i as u32).collect();
            assert_eq!(&index.newlines[..index.newline_count], &newlines[..]);
            let found = unsafe { std::slice::from_raw_parts(index.delims().sub(1), delims.len() + 9) };
            assert_eq!(found[0], u32::MAX);
            assert_eq!(&found[1..=delims.len()], &delims[..]);
            assert_eq!(&found[delims.len() + 1..], &[u32::MAX; 8]);
            let mut quotes = 0;
            for at in 0..=len {
                assert_eq!(index.quotes_before(at), quotes, "quotes before {at} of {len}");
                if at < len && data[at] == b'"' {
                    quotes += 1;
                }
            }
        }
    }
}
