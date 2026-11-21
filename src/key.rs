//! IDs as order-preserving `u64` keys. Up to 9 bytes of ASCII (1..=127, so a space, a tab or a `\r` is fine)
//! pack at 7 bits each, first byte in the top bits, zero filled; bit 63 is clear. A byte is never zero, so
//! integer order is byte-wise order. Every ID the generator writes packs. Anything else (a NUL, a byte over
//! 127, 10+ bytes) is interned exactly, with bit 63 set. `0` is the empty ID.
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

pub type Key = u64;

const INTERNED: u64 = 1 << 63;

#[inline]
pub fn pack(id: &[u8]) -> Key {
    if id.is_empty() {
        return 0;
    }
    if id.len() <= 9 && id.iter().all(|&b| b.wrapping_sub(1) < 127) {
        let mut k = 0u64;
        for &b in id {
            k = k << 7 | b as u64;
        }
        return k << (7 * (9 - id.len()));
    }
    intern(id)
}

/// `pack` for a field inside a buffer with at least 9 readable bytes from `p`: one 8-byte load, a SWAR test
/// for a NUL or a byte over 127, one `pext`; the ninth byte by hand.
///
/// # Safety
/// `p[..9]` must be readable.
#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
pub unsafe fn pack16(p: *const u8, len: usize) -> Key {
    use std::arch::x86_64::{_bzhi_u64, _pext_u64};
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGH: u64 = ONES << 7;
    unsafe {
        if len.wrapping_sub(1) >= 9 {
            return pack(std::slice::from_raw_parts(p, len));
        }
        let n8 = (8 * len) as u32; // BZHI keeps everything past 64
        let w = _bzhi_u64((p as *const u64).read_unaligned(), n8);
        // The high bit of every zero byte of w (exact for the lowest one, which is all the test needs), only
        // within the ID; no byte of the ID may have its own high bit set.
        let zero = _bzhi_u64(w.wrapping_sub(ONES) & !w, n8);
        let head = _pext_u64(w.swap_bytes(), ONES * 0x7f) << 7;
        // The ninth byte when there is one, else a 1 that passes the test and adds nothing: ID lengths are a
        // coin toss per field, so no branch on it.
        let nine = (len == 9) as u64;
        let b8 = (*p.add(8) as u64 & nine.wrapping_neg()) | (nine ^ 1);
        if ((zero | w) & HIGH) | ((b8.wrapping_sub(1) | b8) & 0x80) != 0 {
            return pack(std::slice::from_raw_parts(p, len));
        }
        head | (b8 & nine.wrapping_neg())
    }
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
#[inline]
pub unsafe fn pack16(p: *const u8, len: usize) -> Key {
    pack(unsafe { std::slice::from_raw_parts(p, len) })
}

#[inline]
pub fn is_packed(k: Key) -> bool {
    k & INTERNED == 0
}

/// Appends the ID's bytes.
pub fn decode(k: Key, out: &mut Vec<u8>) {
    if is_packed(k) {
        let mut k = k;
        while k != 0 {
            out.push((k >> 56) as u8 & 0x7f);
            k = k << 7 & !INTERNED;
        }
    } else {
        out.extend_from_slice(&INTERN.lock().unwrap().list[(k & !INTERNED) as usize]);
    }
}

/// Byte-wise order of the IDs, an integer compare when both are packed.
pub fn cmp(a: Key, b: Key) -> Ordering {
    if is_packed(a) && is_packed(b) {
        return a.cmp(&b);
    }
    let g = INTERN.lock().unwrap();
    let (mut ba, mut bb) = ([0u8; 9], [0u8; 9]);
    bytes(a, &g, &mut ba).cmp(bytes(b, &g, &mut bb))
}

fn bytes<'a>(k: Key, g: &'a Interner, buf: &'a mut [u8; 9]) -> &'a [u8] {
    if !is_packed(k) {
        return &g.list[(k & !INTERNED) as usize];
    }
    let (mut k, mut n) = (k, 0);
    while k != 0 {
        buf[n] = (k >> 56) as u8 & 0x7f;
        n += 1;
        k = k << 7 & !INTERNED;
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
    INTERNED | i as u64
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
        for id in [&b"u1"[..], b"u10", b"U1", b"e42huhoug", b"c_1190", b"abcdefghi", b"_", b"0", b"Zz9_", b" u1", b"u1 ", b"u1\r", b"A\tB", b"a,b"] {
            assert!(is_packed(pack(id)), "{:?}", String::from_utf8_lossy(id));
            assert_eq!(round_trip(id), id);
        }
        for id in [&b"abcdefghij"[..], "é".as_bytes(), b"a\0b", b"\x80"] {
            assert!(!is_packed(pack(id)), "{:?}", String::from_utf8_lossy(id));
            assert_eq!(round_trip(id), id);
            assert_eq!(pack(id), pack(id));
        }
        assert_eq!(pack(b""), 0);
        assert_ne!(pack(b"u1"), pack(b"U1"));
        assert_ne!(pack(b"u1"), pack(b"u1 "));
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
        for _ in 0..50000 {
            let n = (next() % 12) as usize;
            for b in &mut buf[..n] {
                *b = match next() % 16 {
                    0 => (next() % 256) as u8,
                    1 => 0,
                    2 => 127,
                    3 => 128,
                    _ => 1 + (next() % 127) as u8,
                };
            }
            buf[n] = b"\n,\0\x80"[(next() % 4) as usize]; // what follows a field must not matter
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
            b"user_id_a".to_vec(), b"A\tB".to_vec(), b"abcdefghi".to_vec(), b"abcdefghij".to_vec(), b"_".to_vec(),
            b"\x7f".to_vec(), b"\x01".to_vec(), b"a\0".to_vec(),
        ];
        for _ in 0..3000 {
            let n = 1 + (next() % 9) as usize;
            ids.push((0..n).map(|_| 1 + (next() % 127) as u8).collect());
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
