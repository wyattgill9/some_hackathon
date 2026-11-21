//! IDs as 64-bit keys.
//!
//! An ID of up to 10 characters from `[0-9A-Z_a-z]` packs exactly into a u64,
//! 6 bits a character, first character highest. The codes rise with the ASCII
//! values and 0 is the padding, so comparing two packed keys as integers is
//! comparing the IDs byte by byte: the order rules.md asks for when it names a
//! person or breaks a tie between clicks.
//!
//! Every ID the generator writes fits. Anything else (longer, or holding a
//! space, a tab, a `\r`, non-ASCII) is "exotic": interned in a table and given
//! a key with the low tag bit set. That path is slow and exact; it exists so
//! the rules hold for any input, not because scored data takes it.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

pub type Key = u64;

const fn encode_table() -> [u8; 256] {
    let mut table = [0u8; 256];
    let mut code = 1u8;
    let mut c = b'0';
    while c <= b'9' {
        table[c as usize] = code;
        code += 1;
        c += 1;
    }
    c = b'A';
    while c <= b'Z' {
        table[c as usize] = code;
        code += 1;
        c += 1;
    }
    table[b'_' as usize] = code;
    code += 1;
    c = b'a';
    while c <= b'z' {
        table[c as usize] = code;
        code += 1;
        c += 1;
    }
    table
}

const fn decode_table() -> [u8; 64] {
    let encode = encode_table();
    let mut table = [0u8; 64];
    let mut c = 0;
    while c < 256 {
        if encode[c] != 0 {
            table[encode[c] as usize] = c as u8;
        }
        c += 1;
    }
    table
}

static ENCODE: [u8; 256] = encode_table();
static DECODE: [u8; 64] = decode_table();

struct Exotic {
    index: HashMap<Vec<u8>, u32>,
    ids: Vec<Vec<u8>>,
}

static EXOTIC: LazyLock<Mutex<Exotic>> =
    LazyLock::new(|| Mutex::new(Exotic { index: HashMap::new(), ids: Vec::new() }));

/// The packed key of `id`, or 0 if it does not pack (empty, too long, or a character outside the alphabet).
#[inline(always)]
pub fn pack(id: &[u8]) -> Key {
    let len = id.len();
    if len == 0 || len > 10 {
        return 0;
    }
    let mut key = 0u64;
    let mut ok = true;
    for &byte in id {
        let code = ENCODE[byte as usize] as u64;
        ok &= code != 0;
        key = (key << 6) | code;
    }
    if ok {
        key << (64 - 6 * len as u32)
    } else {
        0
    }
}

/// Packs eight 6-bit codes, one per byte with the first character's in the
/// lowest byte, into 48 bits with the first character's highest.
#[inline(always)]
fn squeeze(codes: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    unsafe {
        std::arch::x86_64::_pext_u64(codes.swap_bytes(), 0x3f3f_3f3f_3f3f_3f3f)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let x = codes.swap_bytes();
        let x = ((x >> 2) & 0x0fc0_0fc0_0fc0_0fc0) | (x & 0x003f_003f_003f_003f);
        let x = ((x >> 4) & 0x00ff_f000_00ff_f000) | (x & 0x0000_0fff_0000_0fff);
        ((x >> 8) & 0x0000_ffff_ff00_0000) | (x & 0x0000_0000_00ff_ffff)
    }
}

/// `len` bytes of 0xff, then zeros: `LENGTH_MASKS[16 - len..]` keeps the first `len` bytes of 16.
#[cfg(any(all(target_arch = "x86_64", target_feature = "ssse3"), target_arch = "aarch64"))]
static LENGTH_MASKS: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
// A byte is in the alphabet when its two nibbles agree on a class:
// bit 0: 0-9 (3x, x <= 9)     bit 1: A-O (4x, x >= 1)   bit 2: P-Z (5x, x <= 10)
// bit 3: _   (5f)             bit 4: a-o (6x, x >= 1)   bit 5: p-z (7x, x <= 10)
#[cfg(any(all(target_arch = "x86_64", target_feature = "ssse3"), target_arch = "aarch64"))]
static CLASS_OF_HIGH: [u8; 16] = [0, 0, 0, 1, 2, 4 | 8, 16, 32, 0, 0, 0, 0, 0, 0, 0, 0];
#[cfg(any(all(target_arch = "x86_64", target_feature = "ssse3"), target_arch = "aarch64"))]
static CLASS_OF_LOW: [u8; 16] = [
    1 | 4 | 32, 1 | 2 | 4 | 16 | 32, 1 | 2 | 4 | 16 | 32, 1 | 2 | 4 | 16 | 32, 1 | 2 | 4 | 16 | 32,
    1 | 2 | 4 | 16 | 32, 1 | 2 | 4 | 16 | 32, 1 | 2 | 4 | 16 | 32, 1 | 2 | 4 | 16 | 32, 1 | 2 | 4 | 16 | 32,
    2 | 4 | 16 | 32, 2 | 16, 2 | 16, 2 | 16, 2 | 16, 2 | 8 | 16,
];
/// What to add to a byte to get its code, by high nibble; `_` needs 4 less than the rest of its row.
#[cfg(any(all(target_arch = "x86_64", target_feature = "ssse3"), target_arch = "aarch64"))]
static CODE_OFFSET: [u8; 16] = [
    0, 0, 0, 0u8.wrapping_sub(47), 0u8.wrapping_sub(54), 0u8.wrapping_sub(54), 0u8.wrapping_sub(59),
    0u8.wrapping_sub(59), 0, 0, 0, 0, 0, 0, 0, 0,
];

/// `pack` for an ID in the reader's buffer: the 16 bytes at `id` must be readable,
/// whatever `len` is. Same result as `pack`, 16 bytes at a time.
#[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
#[inline(always)]
pub unsafe fn pack_padded(id: *const u8, len: usize) -> Key {
    use std::arch::x86_64::*;
    if len == 0 || len > 10 {
        return 0;
    }
    let bytes = _mm_loadu_si128(id as *const __m128i);
    let nibble = _mm_set1_epi8(0x0f);
    let high = _mm_and_si128(_mm_srli_epi16(bytes, 4), nibble);
    let low = _mm_and_si128(bytes, nibble);
    let table = |t: &[u8; 16]| _mm_loadu_si128(t.as_ptr() as *const __m128i);
    let class = _mm_and_si128(_mm_shuffle_epi8(table(&CLASS_OF_HIGH), high), _mm_shuffle_epi8(table(&CLASS_OF_LOW), low));
    let keep = _mm_loadu_si128(LENGTH_MASKS.as_ptr().add(16 - len) as *const __m128i);
    let outside = _mm_and_si128(_mm_cmpeq_epi8(class, _mm_setzero_si128()), keep);
    if _mm_movemask_epi8(outside) != 0 {
        return 0;
    }
    let underscore = _mm_and_si128(_mm_cmpeq_epi8(bytes, _mm_set1_epi8(b'_' as i8)), _mm_set1_epi8(-4));
    let codes = _mm_add_epi8(_mm_add_epi8(bytes, _mm_shuffle_epi8(table(&CODE_OFFSET), high)), underscore);
    let codes = _mm_and_si128(codes, keep);
    let first = _mm_cvtsi128_si64(codes) as u64;
    let last = _mm_extract_epi16(codes, 4) as u64;
    (squeeze(first) << 16) | ((last & 0xff) << 10) | ((last >> 8) << 4)
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub unsafe fn pack_padded(id: *const u8, len: usize) -> Key {
    use std::arch::aarch64::*;
    if len == 0 || len > 10 {
        return 0;
    }
    let bytes = vld1q_u8(id);
    let high = vshrq_n_u8(bytes, 4);
    let low = vandq_u8(bytes, vdupq_n_u8(0x0f));
    let class = vandq_u8(vqtbl1q_u8(vld1q_u8(CLASS_OF_HIGH.as_ptr()), high), vqtbl1q_u8(vld1q_u8(CLASS_OF_LOW.as_ptr()), low));
    let keep = vld1q_u8(LENGTH_MASKS.as_ptr().add(16 - len));
    let outside = vandq_u8(vceqzq_u8(class), keep);
    if vmaxvq_u8(outside) != 0 {
        return 0;
    }
    let underscore = vandq_u8(vceqq_u8(bytes, vdupq_n_u8(b'_')), vdupq_n_u8(0u8.wrapping_sub(4)));
    let codes = vaddq_u8(vaddq_u8(bytes, vqtbl1q_u8(vld1q_u8(CODE_OFFSET.as_ptr()), high)), underscore);
    let codes = vandq_u8(codes, keep);
    let first = vgetq_lane_u64(vreinterpretq_u64_u8(codes), 0);
    let last = vgetq_lane_u64(vreinterpretq_u64_u8(codes), 1);
    (squeeze(first) << 16) | ((last & 0xff) << 10) | (((last >> 8) & 0xff) << 4)
}

#[cfg(not(any(all(target_arch = "x86_64", target_feature = "ssse3"), target_arch = "aarch64")))]
#[inline(always)]
pub unsafe fn pack_padded(id: *const u8, len: usize) -> Key {
    pack(std::slice::from_raw_parts(id, len))
}

/// `key_of` for a non-empty ID in the reader's buffer (16 readable bytes at `id`).
#[inline(always)]
pub unsafe fn key_of_padded(id: *const u8, len: usize) -> Key {
    let key = pack_padded(id, len);
    if key != 0 {
        key
    } else {
        intern(std::slice::from_raw_parts(id, len))
    }
}

#[cold]
#[inline(never)]
fn intern(id: &[u8]) -> Key {
    let mut exotic = EXOTIC.lock().unwrap();
    let at = match exotic.index.get(id) {
        Some(&at) => at,
        None => {
            let at = exotic.ids.len() as u32;
            exotic.index.insert(id.to_vec(), at);
            exotic.ids.push(id.to_vec());
            at
        }
    };
    (u64::from(at) << 4) | 1
}

/// The key of a non-empty ID. Equal IDs get equal keys and different IDs different keys.
#[cfg(test)]
pub fn key_of(id: &[u8]) -> Key {
    let key = pack(id);
    if key != 0 {
        key
    } else {
        intern(id)
    }
}

#[inline(always)]
fn is_packed(key: Key) -> bool {
    key & 15 == 0
}

/// Appends the ID a key stands for.
#[inline]
pub fn write_id(key: Key, out: &mut Vec<u8>) {
    if is_packed(key) {
        let mut rest = key;
        while rest != 0 {
            out.push(DECODE[(rest >> 58) as usize]);
            rest <<= 6;
        }
    } else {
        out.extend_from_slice(&EXOTIC.lock().unwrap().ids[(key >> 4) as usize]);
    }
}

#[cold]
#[inline(never)]
fn compare_slow(a: Key, b: Key) -> Ordering {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    write_id(a, &mut x);
    write_id(b, &mut y);
    x.cmp(&y)
}

/// Byte-wise order of the two IDs.
#[inline(always)]
pub fn compare(a: Key, b: Key) -> Ordering {
    if is_packed(a | b) {
        a.cmp(&b)
    } else {
        compare_slow(a, b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_order_is_byte_order() {
        let ids: [&[u8]; 14] = [
            b"0", b"9", b"A", b"U1", b"Z", b"_", b"a", b"u", b"u1", b"u10", b"u1a", b"u2", b"uzzzzzzzzz", b"z",
        ];
        for a in ids {
            for b in ids {
                assert_eq!(compare(key_of(a), key_of(b)), a.cmp(b), "{a:?} vs {b:?}");
            }
        }
    }

    #[test]
    fn padded_pack_is_pack() {
        let mut state = 99u64;
        let mut next = move || {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        let alphabet = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
        let mut packed = 0;
        for round in 0..300_000 {
            let mut buffer = [0u8; 32];
            for byte in &mut buffer {
                let r = next();
                // Mostly the alphabet; sometimes its neighbours or anything at all.
                *byte = match r % 40 {
                    0 => b"/:@[^`{\x7f -\"\r\t,\n\0"[(r >> 8) as usize % 16],
                    1 => (r >> 8) as u8,
                    _ => alphabet[(r >> 8) as usize % alphabet.len()],
                };
            }
            let len = (next() % 14) as usize;
            let expected = pack(&buffer[..len]);
            assert_eq!(unsafe { pack_padded(buffer.as_ptr(), len) }, expected, "round {round}: {:?}", &buffer[..len]);
            packed += usize::from(expected != 0);
        }
        // Every single byte value, alone and after a valid character.
        for byte in 0..=255u8 {
            let mut buffer = [b'u'; 32];
            buffer[0] = byte;
            assert_eq!(unsafe { pack_padded(buffer.as_ptr(), 1) }, pack(&buffer[..1]), "byte {byte}");
            buffer[0] = b'u';
            buffer[9] = byte;
            assert_eq!(unsafe { pack_padded(buffer.as_ptr(), 10) }, pack(&buffer[..10]), "byte {byte} last");
        }
        assert!(packed > 100_000, "{packed} packed");
    }

    #[test]
    fn exotic_ids_round_trip_and_order() {
        let ids: [&[u8]; 9] = [b" u1", b"u1 ", b"u1", b"A\tB", b"user_id_a", b"e3\r", b"u1234567890", b"\xd9\xa2", b"u"];
        for a in ids {
            let mut back = Vec::new();
            write_id(key_of(a), &mut back);
            assert_eq!(back, a);
            assert_eq!(key_of(a), key_of(a));
            for b in ids {
                assert_eq!(compare(key_of(a), key_of(b)), a.cmp(b), "{a:?} vs {b:?}");
                assert_eq!(key_of(a) == key_of(b), a == b);
            }
        }
    }
}
