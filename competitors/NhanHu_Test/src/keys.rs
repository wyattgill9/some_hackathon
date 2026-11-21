//! String identities as u64 keys.
//!
//! A string of 1 to 9 bytes, each in 0x01..=0x7F, is packed exactly into the low
//! 63 bits: seven bits per byte, left aligned, zero padded. Integer order of two
//! packed keys equals byte-wise order of the strings (padding sorts before every
//! byte). Every other non-empty string is interned and gets `LONG_BIT | index`;
//! comparisons involving such a key fall back to the bytes. Key 0 is the empty
//! string and is never stored.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock, RwLock};

pub const LONG_BIT: u64 = 1 << 63;
const MAX_COMPACT: usize = 9;

/// Packs the string of length `n` at `p`; at least 16 readable bytes must start at `p`.
#[inline(always)]
pub unsafe fn compact_at(p: *const u8, n: usize) -> Option<u64> {
    if n > MAX_COMPACT {
        return None;
    }
    let w = (p as *const u64).read_unaligned();
    let mask = if n >= 8 { !0u64 } else { (1u64 << (8 * n)) - 1 };
    let v = w & mask;
    let b9 = if n == 9 { *p.add(8) as u64 } else { 0 };
    let vv = v | !mask;
    let zero = vv.wrapping_sub(0x0101_0101_0101_0101) & !vv & 0x8080_8080_8080_8080;
    let high = (v & 0x8080_8080_8080_8080) | (b9 & 0x80);
    if zero | high != 0 || (n == 9 && b9 == 0) {
        return None;
    }
    #[cfg(target_feature = "bmi2")]
    let packed = std::arch::x86_64::_pext_u64(v.swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F);
    #[cfg(not(target_feature = "bmi2"))]
    let packed = {
        let b = v.swap_bytes();
        let mut r = 0u64;
        for i in (0..8).rev() {
            r = (r << 7) | ((b >> (8 * i)) & 0x7F);
        }
        r
    };
    Some((packed << 7) | b9)
}

#[inline(always)]
pub fn compact(s: &[u8]) -> Option<u64> {
    if s.len() > MAX_COMPACT {
        return None;
    }
    let mut buf = [0u8; 16];
    buf[..s.len()].copy_from_slice(s);
    unsafe { compact_at(buf.as_ptr(), s.len()) }
}

struct Interner {
    map: Mutex<HashMap<Vec<u8>, u64>>,
    strings: RwLock<Vec<Vec<u8>>>,
}

fn interner() -> &'static Interner {
    static I: OnceLock<Interner> = OnceLock::new();
    I.get_or_init(|| Interner { map: Mutex::new(HashMap::new()), strings: RwLock::new(Vec::new()) })
}

#[cold]
fn intern(s: &[u8]) -> u64 {
    let i = interner();
    let mut map = i.map.lock().unwrap();
    if let Some(&k) = map.get(s) {
        return k;
    }
    let mut strings = i.strings.write().unwrap();
    let k = LONG_BIT | strings.len() as u64;
    strings.push(s.to_vec());
    map.insert(s.to_vec(), k);
    k
}

/// Key of a non-empty string.
#[inline(always)]
pub fn key_of(s: &[u8]) -> u64 {
    match compact(s) {
        Some(k) => k,
        None => intern(s),
    }
}

/// Key of a non-empty string that is followed by at least 16 readable bytes.
#[inline(always)]
pub unsafe fn key_of_padded(s: &[u8]) -> u64 {
    match compact_at(s.as_ptr(), s.len()) {
        Some(k) => k,
        None => intern(s),
    }
}

/// `key_lookup` for a string followed by at least 16 readable bytes.
#[inline(always)]
pub unsafe fn key_lookup_padded(s: &[u8]) -> Option<u64> {
    match compact_at(s.as_ptr(), s.len()) {
        Some(k) => Some(k),
        None => key_lookup(s),
    }
}

/// Key of a string if it already has one (never interns). Used for lookups
/// in fixed tables (products, campaigns), where an unseen long string cannot match.
#[inline(always)]
pub fn key_lookup(s: &[u8]) -> Option<u64> {
    match compact(s) {
        Some(k) => Some(k),
        None => {
            let i = interner();
            let map = i.map.lock().unwrap();
            map.get(s).copied()
        }
    }
}

pub fn key_bytes_into(k: u64, out: &mut Vec<u8>) {
    if k & LONG_BIT != 0 {
        let i = interner();
        let strings = i.strings.read().unwrap();
        out.extend_from_slice(&strings[(k & !LONG_BIT) as usize]);
    } else {
        for j in 0..MAX_COMPACT {
            let c = ((k >> (56 - 7 * j)) & 0x7F) as u8;
            if c == 0 {
                break;
            }
            out.push(c);
        }
    }
}

/// Appends the string of `k` as a PLAIN Parquet BYTE_ARRAY (length, bytes).
#[inline(always)]
pub fn push_plain(k: u64, out: &mut Vec<u8>) {
    #[cfg(target_feature = "bmi2")]
    if k & LONG_BIT == 0 {
        let b = unsafe { std::arch::x86_64::_pdep_u64(k >> 7, 0x7F7F_7F7F_7F7F_7F7F) }.swap_bytes();
        let c9 = (k & 0x7F) as u8;
        let z = b.wrapping_sub(0x0101_0101_0101_0101) & !b & 0x8080_8080_8080_8080;
        let n8 = if z == 0 { 8 } else { (z.trailing_zeros() / 8) as usize };
        let n = n8 + (n8 == 8 && c9 != 0) as usize;
        out.extend_from_slice(&(n as u32).to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes()[..n8]);
        if n == 9 {
            out.push(c9);
        }
        return;
    }
    let v = key_bytes(k);
    out.extend_from_slice(&(v.len() as u32).to_le_bytes());
    out.extend_from_slice(&v);
}

pub fn key_bytes(k: u64) -> Vec<u8> {
    let mut v = Vec::new();
    key_bytes_into(k, &mut v);
    v
}

#[inline]
pub fn key_cmp(a: u64, b: u64) -> Ordering {
    if (a | b) & LONG_BIT == 0 {
        a.cmp(&b)
    } else {
        key_bytes(a).cmp(&key_bytes(b))
    }
}

/// Fibonacci hashing: the top `bits` bits of k times an odd constant.
#[inline(always)]
pub fn fib(k: u64, bits: u32) -> usize {
    (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - bits)) as usize
}

/// A second, independent Fibonacci hash.
#[inline(always)]
pub fn fib2(k: u64, bits: u32) -> usize {
    (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> (64 - bits)) as usize
}

#[allow(dead_code)]
#[inline(always)]
pub fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn order() {
        let words: Vec<&[u8]> = vec![
            b"a", b"a-", b"a--", b"a0", b"A", b"Z", b"_", b"z", b"u1", b"U1", b"u10", b"u2", b"-", b"0", b"zzzzzzzzz",
            b"aaaaaaaaa", b"u1 ", b" u1", b"\tB", b"abcdefghij", b"e3\r", b"a\0b", b"\x7f", b"\x80", b"zzzzzzzz\x01",
            b"\x01", b"user_id_a", b"\xc3\xa9", b"ab\0",
        ];
        for &x in &words {
            assert_eq!(key_bytes(key_of(x)), x.to_vec());
            let mut p = Vec::new();
            push_plain(key_of(x), &mut p);
            assert_eq!(&p[4..], x, "{:?}", x);
            assert_eq!(u32::from_le_bytes(p[..4].try_into().unwrap()) as usize, x.len());
            for &y in &words {
                assert_eq!(key_cmp(key_of(x), key_of(y)), x.cmp(y), "{:?} {:?}", x, y);
            }
        }
    }
}
