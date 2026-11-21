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
    #[inline]
    pub fn parse(&mut self, b: &[u8]) -> Option<i64> {
        if b.len() == 20 && b[19] == b'Z' {
            let key = u64::from_le_bytes(b[0..8].try_into().unwrap());
            let tail = u32::from_le_bytes([b[8], b[9], b[10], 0]);
            let slot = ((b[8] & 15) as usize * 10 + (b[9] & 15) as usize) & (SLOTS - 1);
            let day = if self.keys[slot] == key && self.tails[slot] == tail {
                self.days[slot]
            } else {
                self.learn(slot, key, tail, b)?
            };
            return Some(day + time_of_day(&b[11..19])?);
        }
        if b.len() <= 16 {
            if let Some(v) = digits(b) {
                return Some(v);
            }
        }
        parse::ts(b)
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

/// `HH:MM:SS` to ms, or None. Digit lanes must be 0..=9 after xor with '0', separator lanes must be 0.
#[inline]
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

/// 1 to 16 ASCII digits as a number, 8 at a time.
#[inline]
fn digits(b: &[u8]) -> Option<i64> {
    if b.is_empty() {
        return None;
    }
    let mut p = [b'0'; 16];
    p[16 - b.len()..].copy_from_slice(b);
    let hi = eight(u64::from_le_bytes(p[..8].try_into().unwrap()))?;
    let lo = eight(u64::from_le_bytes(p[8..].try_into().unwrap()))?;
    Some((hi * 100_000_000 + lo) as i64)
}

#[inline]
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
