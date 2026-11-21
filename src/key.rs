//! IDs as order-preserving `u64` keys. Up to 10 bytes of `[0-9A-Z_a-z]` pack at 6 bits each, first byte in the
//! top bits and the low 4 bits zero, so integer order is byte-wise order. Every ID the generator writes packs.
//! Anything else (a space, a tab, a `\r`, 11+ bytes) is interned exactly, with tag bit 0 set. `0` is the empty ID.
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

pub type Key = u64;

const fn code_table() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        t[i] = match b {
            b'0'..=b'9' => b - b'0' + 1,
            b'A'..=b'Z' => b - b'A' + 11,
            b'_' => 37,
            b'a'..=b'z' => b - b'a' + 38,
            _ => 0,
        };
        i += 1;
    }
    t
}
const CODE: [u8; 256] = code_table();

/// pshufb tables for `pack16`: class bits per high nibble, class bits per low nibble, code offset per high nibble.
/// Class 1: high 0x3 with low 0..=9. Class 2: high 0x4/0x6 with low 1..=15. Class 4: high 0x5/0x7 with low
/// 0..=10. Class 8: high 0x5 with low 15 ('_').
const NIBBLE: ([u8; 16], [u8; 16], [u8; 16]) = {
    let mut hi = [0u8; 16];
    hi[3] = 1;
    hi[4] = 2;
    hi[5] = 4 | 8;
    hi[6] = 2;
    hi[7] = 4;
    let mut lo = [0u8; 16];
    let mut i = 0;
    while i < 16 {
        lo[i] = (if i <= 9 { 1 } else { 0 }) | (if i >= 1 { 2 } else { 0 }) | (if i <= 10 { 4 } else { 0 }) | (if i == 15 { 8 } else { 0 });
        i += 1;
    }
    let mut off = [0u8; 16];
    off[3] = b'0' - 1;
    off[4] = b'A' - 11;
    off[5] = b'A' - 11;
    off[6] = b'a' - 38;
    off[7] = b'a' - 38;
    (hi, lo, off)
};
const DECODE: &[u8; 64] = b"\x000123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

#[inline]
pub fn pack(id: &[u8]) -> Key {
    if id.is_empty() {
        return 0;
    }
    if id.len() <= 10 {
        let (mut k, mut ok) = (0u64, true);
        for &b in id {
            let c = CODE[b as usize];
            ok &= c != 0;
            k = k << 6 | c as u64;
        }
        if ok {
            return k << (64 - 6 * id.len());
        }
    }
    intern(id)
}

/// `pack` for a field inside a buffer with at least 16 readable bytes from `p`: one 16-byte load classifies
/// every byte at once; anything irregular takes the scalar path.
///
/// # Safety
/// `p[..16]` must be readable.
#[cfg(all(target_arch = "x86_64", target_feature = "sse4.1", target_feature = "bmi2"))]
#[inline]
pub unsafe fn pack16(p: *const u8, len: usize) -> Key {
    use std::arch::x86_64::*;
    unsafe {
        if len == 0 || len > 10 {
            return pack(std::slice::from_raw_parts(p, len));
        }
        let v = _mm_loadu_si128(p as *const __m128i);
        // Each byte's class from its two nibbles: a byte is in the alphabet iff some class accepts both its
        // high nibble (0x3 digits; 0x4/0x6 letters 1..15; 0x5/0x7 letters 0..10; 0x5 '_' at 15) and its low.
        let lo = _mm_and_si128(v, _mm_set1_epi8(0x0f));
        let hi = _mm_and_si128(_mm_srli_epi16::<4>(v), _mm_set1_epi8(0x0f));
        let hi_class = _mm_shuffle_epi8(_mm_loadu_si128(NIBBLE.0.as_ptr() as *const __m128i), hi);
        let lo_class = _mm_shuffle_epi8(_mm_loadu_si128(NIBBLE.1.as_ptr() as *const __m128i), lo);
        let bad = _mm_cmpeq_epi8(_mm_and_si128(hi_class, lo_class), _mm_setzero_si128());
        let lanes = (1u32 << len) - 1;
        if _mm_movemask_epi8(bad) as u32 & lanes != 0 || (_mm_movemask_epi8(v) as u32 & lanes) != 0 {
            return pack(std::slice::from_raw_parts(p, len));
        }
        // code = byte - offset(high nibble); '_' (0x5f) is the one exception to its row's offset.
        let off = _mm_shuffle_epi8(_mm_loadu_si128(NIBBLE.2.as_ptr() as *const __m128i), hi);
        let under = _mm_cmpeq_epi8(v, _mm_set1_epi8(b'_' as i8));
        let codes = _mm_blendv_epi8(_mm_sub_epi8(v, off), _mm_set1_epi8(37), under);
        // Lanes past the ID are zeroed; the first 8 codes pack with one pext, codes 9 and 10 by hand.
        let keep = _mm_cmpgt_epi8(_mm_set1_epi8(len as i8), _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
        let codes = _mm_and_si128(codes, keep);
        let lo = _mm_cvtsi128_si64(codes) as u64;
        let hi = _mm_extract_epi16::<4>(codes) as u32;
        let head = _pext_u64(lo.swap_bytes(), 0x3f3f_3f3f_3f3f_3f3f);
        head << 16 | ((hi & 0xff) as u64) << 10 | ((hi >> 8) as u64) << 4
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.1", target_feature = "bmi2")))]
#[inline]
pub unsafe fn pack16(p: *const u8, len: usize) -> Key {
    pack(unsafe { std::slice::from_raw_parts(p, len) })
}

#[inline]
pub fn is_packed(k: Key) -> bool {
    k & 1 == 0
}

/// Appends the ID's bytes.
pub fn decode(k: Key, out: &mut Vec<u8>) {
    if is_packed(k) {
        let mut k = k;
        while k != 0 {
            out.push(DECODE[(k >> 58) as usize]);
            k <<= 6;
        }
    } else {
        out.extend_from_slice(&INTERN.lock().unwrap().list[(k >> 4) as usize]);
    }
}

/// Byte-wise order of the IDs, an integer compare when both are packed.
pub fn cmp(a: Key, b: Key) -> Ordering {
    if is_packed(a) && is_packed(b) {
        return a.cmp(&b);
    }
    let g = INTERN.lock().unwrap();
    let (mut ba, mut bb) = ([0u8; 10], [0u8; 10]);
    bytes(a, &g, &mut ba).cmp(bytes(b, &g, &mut bb))
}

fn bytes<'a>(k: Key, g: &'a Interner, buf: &'a mut [u8; 10]) -> &'a [u8] {
    if !is_packed(k) {
        return &g.list[(k >> 4) as usize];
    }
    let (mut k, mut n) = (k, 0);
    while k != 0 {
        buf[n] = DECODE[(k >> 58) as usize];
        n += 1;
        k <<= 6;
    }
    &buf[..n]
}

struct Interner {
    map: HashMap<Vec<u8>, u32>,
    list: Vec<Vec<u8>>,
}

static INTERN: LazyLock<Mutex<Interner>> =
    LazyLock::new(|| Mutex::new(Interner { map: HashMap::new(), list: Vec::new() }));

#[cold]
fn intern(id: &[u8]) -> Key {
    let mut g = INTERN.lock().unwrap();
    let i = match g.map.get(id) {
        Some(&i) => i,
        None => {
            let i = g.list.len() as u32;
            g.list.push(id.to_vec());
            g.map.insert(id.to_vec(), i);
            i
        }
    };
    (i as u64) << 4 | 1
}

/// A 64-bit mixer for partitioning and hashing keys (use the top bits).
#[inline]
pub fn mix(k: u64) -> u64 {
    let x = (k ^ (k >> 32)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(id: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        decode(pack(id), &mut out);
        out
    }

    #[test]
    fn packing() {
        for id in [&b"u1"[..], b"u10", b"U1", b"e42huhoug", b"c_1190", b"abcdefghij", b"_", b"0", b"Zz9_"] {
            assert!(is_packed(pack(id)), "{:?}", String::from_utf8_lossy(id));
            assert_eq!(round_trip(id), id);
        }
        for id in [&b" u1"[..], b"u1 ", b"u1\r", b"A\tB", b"abcdefghijk", "é".as_bytes(), b"a,b"] {
            assert!(!is_packed(pack(id)), "{:?}", String::from_utf8_lossy(id));
            assert_eq!(round_trip(id), id);
            assert_eq!(pack(id), pack(id));
        }
        assert_eq!(pack(b""), 0);
        assert_ne!(pack(b"u1"), pack(b"U1"));
    }

    #[test]
    fn simd_matches_scalar() {
        let mut rng = 0x1357_9bdf_2468_ace0u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut buf = vec![b'z'; 64];
        for _ in 0..20000 {
            let n = (next() % 13) as usize;
            for b in &mut buf[..n] {
                *b = if next() % 4 == 0 { (next() % 256) as u8 } else { DECODE[1 + (next() % 63) as usize] };
            }
            buf[n] = b','; // what follows a field in a line
            let id = &buf[..n];
            assert_eq!(unsafe { pack16(buf.as_ptr(), n) }, pack(id), "{:?}", String::from_utf8_lossy(id));
        }
    }

    #[test]
    fn order() {
        let mut rng = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut ids: Vec<Vec<u8>> = vec![
            b"u1".to_vec(), b"u10".to_vec(), b"u1 ".to_vec(), b" u1".to_vec(), b"U1".to_vec(), b"u".to_vec(),
            b"user_id_a".to_vec(), b"A\tB".to_vec(), b"abcdefghij".to_vec(), b"abcdefghijk".to_vec(), b"_".to_vec(),
        ];
        for _ in 0..3000 {
            let n = 1 + (next() % 10) as usize;
            ids.push((0..n).map(|_| DECODE[1 + (next() % 63) as usize]).collect());
        }
        for _ in 0..500 {
            let n = 1 + (next() % 12) as usize;
            ids.push((0..n).map(|_| (next() % 256) as u8).collect());
        }
        let keys: Vec<Key> = ids.iter().map(|i| pack(i)).collect();
        for (i, a) in ids.iter().enumerate() {
            for (j, b) in ids.iter().enumerate().skip(i) {
                assert_eq!(cmp(keys[i], keys[j]), a.cmp(b), "{:?} vs {:?}", a, b);
                assert_eq!(keys[i] == keys[j], a == b);
            }
        }
    }
}
