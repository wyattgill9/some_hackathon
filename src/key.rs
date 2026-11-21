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
