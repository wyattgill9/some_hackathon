//! Timestamp fast paths. F1 goes through a per-thread date cache keyed on its 11 date bytes plus a SWAR check of
//! `HH:MM:SS`; F3 of up to 16 digits is parsed 8 digits at a time. Everything else (F2, longer F3, every bad
//! form) takes the exact slow path in `parse::ts`, so the result is identical by construction.
use crate::parse;

const SLOTS: usize = 32;

pub struct TsCache {
    keys: [u64; SLOTS], // "YYYY-MM-" as a little-endian u64
    tails: [u32; SLOTS], // "DDT"
    days: [i64; SLOTS], // that date's midnight, in epoch ms
}

impl Default for TsCache {
    fn default() -> Self {
        TsCache { keys: [0; SLOTS], tails: [0; SLOTS], days: [0; SLOTS] }
    }
}

impl TsCache {
    /// `parse_raw` on a copy; for callers without a padded buffer (tests).
    #[cfg(test)]
    pub fn parse(&mut self, b: &[u8]) -> Option<i64> {
        if b.len() > 32 {
            return parse::ts(b);
        }
        let mut tmp = [0u8; 48];
        tmp[..b.len()].copy_from_slice(b);
        unsafe { self.parse_raw(tmp.as_ptr(), b.len()) }
    }

    /// The timestamp at `p[..len]`, where at least 16 bytes from `p` are readable (the reader pads its buffer).
    ///
    /// # Safety
    /// `p[..max(len, 16)]` must be readable.
    #[inline(always)]
    pub unsafe fn parse_raw(&mut self, p: *const u8, len: usize) -> Option<i64> {
        let b = unsafe { std::slice::from_raw_parts(p, len) };
        if len == 20 && b[19] == b'Z' {
            return Some(self.date(b)? + time_of_day(&b[11..19])?);
        }
        if (1..=16).contains(&len) {
            if let Some(v) = unsafe { digits16(p, len) } {
                return Some(v);
            }
        }
        if b.len() == 25 && matches!(b[19], b'+' | b'-') && b[22] == b':' {
            let local = self.date(b)? + time_of_day(&b[11..19])?;
            let (h, m) = (two(b[20], b[21])?, two(b[23], b[24])?);
            if h > 23 || m > 59 {
                return None;
            }
            let off = (h * 60 + m) * 60_000;
            return Some(if b[19] == b'+' { local - off } else { local + off });
        }
        parse::ts(b)
    }

    /// The midnight of a valid `YYYY-MM-DDT` prefix, from the cache or the calendar.
    #[inline(always)]
    fn date(&mut self, b: &[u8]) -> Option<i64> {
        let key = u64::from_le_bytes(b[0..8].try_into().unwrap());
        let tail = u32::from_le_bytes([b[8], b[9], b[10], 0]);
        let slot = ((b[8] & 15) as usize * 10 + (b[9] & 15) as usize) & (SLOTS - 1);
        if self.keys[slot] == key && self.tails[slot] == tail { Some(self.days[slot]) } else { self.learn(slot, key, tail, b) }
    }

    #[cold]
    fn learn(&mut self, slot: usize, key: u64, tail: u32, b: &[u8]) -> Option<i64> {
        let mut midnight = [0u8; 20];
        midnight[..11].copy_from_slice(&b[..11]);
        midnight[11..].copy_from_slice(b"00:00:00Z");
        let day = parse::ts(&midnight)?;
        (self.keys[slot], self.tails[slot], self.days[slot]) = (key, tail, day);
        Some(day)
    }
}

#[inline(always)]
fn two(a: u8, b: u8) -> Option<i64> {
    let (x, y) = (a.wrapping_sub(b'0'), b.wrapping_sub(b'0'));
    (x < 10 && y < 10).then_some((x * 10 + y) as i64)
}

/// `HH:MM:SS` to ms, or None. Digit lanes must be 0..=9 after xor with '0', separator lanes must be 0.
#[inline(always)]
fn time_of_day(t: &[u8]) -> Option<i64> {
    let x = u64::from_le_bytes(t.try_into().unwrap()) ^ 0x3030_3a30_303a_3030;
    if (x.wrapping_add(0x7676_7f76_767f_7676) | x) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let d = |i: u32| (x >> (8 * i)) & 0xff;
    let (h, m, s) = (d(0) * 10 + d(1), d(3) * 10 + d(4), d(6) * 10 + d(7));
    if h > 23 || m > 59 || s > 59 {
        return None;
    }
    Some(((h * 60 + m) * 60 + s) as i64 * 1000)
}

/// Right-aligns `len` bytes into 16 lanes with `pshufb` and fills the lanes before them with '0'.
const fn align_tables() -> ([[u8; 16]; 17], [[u8; 16]; 17]) {
    let (mut shuf, mut fill) = ([[0x80u8; 16]; 17], [[0u8; 16]; 17]);
    let mut l = 0;
    while l <= 16 {
        let mut i = 0;
        while i < 16 {
            if i >= 16 - l {
                shuf[l][i] = (i - (16 - l)) as u8;
            } else {
                fill[l][i] = b'0';
            }
            i += 1;
        }
        l += 1;
    }
    (shuf, fill)
}
static ALIGN: ([[u8; 16]; 17], [[u8; 16]; 17]) = align_tables();

/// 1 to 16 ASCII digits at `p` as a number, 8 at a time, from one 16-byte load (no stack copy: a copy
/// followed by wider loads stalls on store forwarding).
#[inline(always)]
unsafe fn digits16(p: *const u8, len: usize) -> Option<i64> {
    #[cfg(target_feature = "ssse3")]
    let (hi, lo) = unsafe {
        use std::arch::x86_64::*;
        let v = _mm_loadu_si128(p as *const __m128i);
        let aligned = _mm_shuffle_epi8(v, _mm_loadu_si128(ALIGN.0[len].as_ptr() as *const __m128i));
        let d = _mm_or_si128(aligned, _mm_loadu_si128(ALIGN.1[len].as_ptr() as *const __m128i));
        (_mm_cvtsi128_si64(d) as u64, _mm_extract_epi64::<1>(d) as u64)
    };
    #[cfg(not(target_feature = "ssse3"))]
    let (hi, lo) = {
        let mut t = [b'0'; 16];
        t[16 - len..].copy_from_slice(unsafe { std::slice::from_raw_parts(p, len) });
        (u64::from_le_bytes(t[..8].try_into().unwrap()), u64::from_le_bytes(t[8..].try_into().unwrap()))
    };
    Some((eight(hi)? * 100_000_000 + eight(lo)?) as i64)
}

#[inline(always)]
fn eight(w: u64) -> Option<u64> {
    let x = w ^ 0x3030_3030_3030_3030;
    if (x.wrapping_add(0x7676_7676_7676_7676) | x) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    let x = (x * 10 + (x >> 8)) & 0x00ff_00ff_00ff_00ff;
    let x = (x * 100 + (x >> 16)) & 0x0000_ffff_0000_ffff;
    Some((x * 10000 + (x >> 32)) & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(s: &[u8]) {
        let mut c = TsCache::default();
        assert_eq!(c.parse(s), parse::ts(s), "{:?}", String::from_utf8_lossy(s));
        assert_eq!(c.parse(s), parse::ts(s), "{:?} (cached)", String::from_utf8_lossy(s));
    }

    #[test]
    fn fast_equals_slow() {
        let fixed: &[&[u8]] = &[
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000", b"0001791071890000",
            b"2026-10-04T01:30:00+02:00", b"2026-10-03T23:58:10-00:00", b"0", b"86400000", b"123456789012345678901",
            b"2028-02-29T00:00:00Z", b"2027-02-29T00:00:00Z", b"2026-10-03T24:00:00Z", b"2026-10-03T23:59:60Z",
            b"2026-10-03T23:58:10z", b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10", b"2026-10-03T23:58:10.000Z",
            b"2026-10-03T23:58:10Z\r", b"1791071890000.0", b"1.79e12", b"-5", "٢٥٠".as_bytes(), b"", b"1",
            b"00000000000000001791071890000", b"9999999999999999", b"99999999999999999", b"2026-13-01T00:00:00Z",
            b"2026-02-30T00:00:00Z", b"2026-10-3T23:58:10Z", b"2026-10-03T23:58:1Z", b"2026-10-03T2:58:10Z",
            "２０26-10-03T23:58:10Z".as_bytes(), "2026-10-03T２3:58:10Z".as_bytes(),
        ];
        for s in fixed {
            check(s);
        }
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for _ in 0..20000 {
            let t = 1_600_000_000_000i64 + (next() % 300_000_000_000) as i64;
            let s = t / 1000;
            let (days, rem) = (s.div_euclid(86400), s.rem_euclid(86400));
            let (y, m, d) = civil(days);
            let f1 = format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem / 60 % 60, rem % 60);
            check(f1.as_bytes());
            check(format!("{t}").as_bytes());
            let (oh, om) = (next() % 26, next() % 62); // offsets up to +25:61: some valid, some not
            let sign = if next() % 2 == 0 { '+' } else { '-' };
            let f2 = format!("{}{sign}{oh:02}:{om:02}", &f1[..19]);
            check(f2.as_bytes());
            let mut bytes = f2.into_bytes();
            let i = (next() % 25) as usize;
            bytes[i] = (next() % 128) as u8;
            check(&bytes);
            check(format!("{:013}", t % 10_000_000_000).as_bytes());
            let mut bytes = f1.into_bytes();
            let i = (next() % 20) as usize;
            bytes[i] = (next() % 128) as u8;
            check(&bytes);
            bytes.truncate((next() % 21) as usize);
            check(&bytes);
        }
    }

    fn civil(z: i64) -> (i64, i64, i64) {
        let z = z + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        (yoe + era * 400 + (m <= 2) as i64, m, d)
    }
}
