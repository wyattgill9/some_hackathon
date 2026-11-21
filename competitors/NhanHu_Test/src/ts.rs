//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 digits (epoch ms).

pub const SECOND: i64 = 1_000;
pub const DAY: i64 = 86_400_000;

#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline(always)]
fn dig(c: u8) -> u32 {
    c.wrapping_sub(b'0') as u32
}

#[inline(always)]
fn two(s: &[u8], i: usize) -> Option<i64> {
    let a = dig(s[i]);
    let b = dig(s[i + 1]);
    if a > 9 || b > 9 {
        return None;
    }
    Some((a * 10 + b) as i64)
}

#[inline(always)]
fn parse_iso(s: &[u8]) -> Option<i64> {
    // s.len() is 20 or 25
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = two(s, 0)? * 100 + two(s, 2)?;
    let mo = two(s, 5)?;
    let d = two(s, 8)?;
    let h = two(s, 11)?;
    let mi = two(s, 14)?;
    let se = two(s, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let dim = match mo {
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let mut ms = (((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60 + se) * SECOND;
    if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = s[19];
        if (sign != b'+' && sign != b'-') || s[22] != b':' {
            return None;
        }
        let oh = two(s, 20)?;
        let om = two(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60 * SECOND;
        if sign == b'+' {
            ms -= off;
        } else {
            ms += off;
        }
    }
    Some(ms)
}

#[inline(always)]
fn parse_digits(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = dig(c);
        if d > 9 {
            return None;
        }
        v = v.saturating_mul(10).saturating_add(d as u64);
    }
    Some(v.min(i64::MAX as u64) as i64)
}

#[inline(always)]
fn eight(x: u64) -> u64 {
    // x holds 8 digit values, the most significant in the lowest byte
    let x = (x.wrapping_mul(2561)) >> 8;
    let x = ((x & 0x00FF_00FF_00FF_00FF).wrapping_mul(6_553_601)) >> 16;
    ((x & 0x0000_FFFF_0000_FFFF).wrapping_mul(42_949_672_960_001)) >> 32
}

/// 1 to 16 ASCII digits at p (16 readable bytes) as a number.
#[inline(always)]
pub unsafe fn digits16(p: *const u8, n: usize) -> Option<i64> {
    let lo = (p as *const u64).read_unaligned() as u128;
    let hi = (p.add(8) as *const u64).read_unaligned() as u128;
    let mask: u128 = if n >= 16 { !0 } else { (1u128 << (8 * n)) - 1 };
    let d = ((lo | (hi << 64)) ^ 0x3030_3030_3030_3030_3030_3030_3030_3030) & mask;
    if (d | d.wrapping_add(0x7676_7676_7676_7676_7676_7676_7676_7676 & mask)) & 0x8080_8080_8080_8080_8080_8080_8080_8080 != 0 {
        return None;
    }
    let d = d << (8 * (16 - n));
    Some((eight(d as u64) * 100_000_000 + eight((d >> 64) as u64)) as i64)
}

/// Milliseconds since the epoch, or None for a bad timestamp. Out-of-range F3
/// values saturate (they are then outside every window).
#[inline(always)]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_iso(s);
    }
    parse_digits(s)
}

/// A faster parser with a per-thread cache of calendar dates (the data hold few distinct dates).
pub struct TsParser {
    keys: Box<[u64; 1024]>,
    days: Box<[i32; 1024]>,
}

const NO_DAY: i32 = i32::MIN;

impl TsParser {
    pub fn new() -> Self {
        TsParser { keys: Box::new([0; 1024]), days: Box::new([0; 1024]) }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn parse(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if (n == 20 || n == 25) && s[4] == b'-' {
            return self.iso(s);
        }
        parse_digits(s)
    }

    /// `parse` for a string followed by at least 16 readable bytes.
    #[inline(always)]
    pub unsafe fn parse_padded(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if (n == 20 || n == 25) && s[4] == b'-' {
            return self.iso(s);
        }
        if n >= 1 && n <= 16 {
            return digits16(s.as_ptr(), n);
        }
        parse_digits(s)
    }

    #[cold]
    #[inline(never)]
    fn date_miss(&mut self, x: u64, d1: u32, d2: u32, key: u64, idx: usize) -> i32 {
        let b = |i: u32| ((x >> (8 * i)) & 0xFF) as i64;
        let year = b(0) * 1000 + b(1) * 100 + b(2) * 10 + b(3);
        let mo = b(5) * 10 + b(6);
        let d = (d1 * 10 + d2) as i64;
        let dim = match mo {
            2 => {
                if is_leap(year) {
                    29
                } else {
                    28
                }
            }
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        let v = if (1..=12).contains(&mo) && d >= 1 && d <= dim { days_from_civil(year, mo, d) as i32 } else { NO_DAY };
        self.keys[idx] = key;
        self.days[idx] = v;
        v
    }

    /// Parses F1 format (`YYYY-MM-DDTHH:MM:SSZ`) directly, returning (milliseconds, seconds).
    #[inline(always)]
    pub fn parse_f1_seconds(&mut self, s: &[u8]) -> Option<(i64, i64)> {
        if s.len() != 20 || unsafe { *s.get_unchecked(19) } != b'Z' {
            return None;
        }
        const HI: u64 = 0x8080_8080_8080_8080;
        const ADD: u64 = 0x7676_7676_7676_7676;
        let lo = u64::from_le_bytes(s[0..8].try_into().unwrap());
        let x = lo ^ 0x2D30_302D_3030_3030;
        let t = u64::from_le_bytes(s[11..19].try_into().unwrap());
        let y = t ^ 0x3030_3A30_303A_3030;
        let d1 = dig(s[8]);
        let d2 = dig(s[9]);
        if ((x | x.wrapping_add(ADD) | y | y.wrapping_add(ADD)) & HI) != 0
            || (x & 0xFF00_00FF_0000_0000) != 0
            || (y & 0x0000_FF00_00FF_0000) != 0
            || d1 > 9
            || d2 > 9
            || s[10] != b'T'
        {
            return None;
        }
        let key = x | ((d1 as u64) << 32) | ((d2 as u64) << 56);
        let idx = (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 54) as usize;
        let days = if self.keys[idx] == key {
            self.days[idx]
        } else {
            self.date_miss(x, d1, d2, key, idx)
        };
        if days == NO_DAY {
            return None;
        }
        let t2 = y.wrapping_mul(10).wrapping_add(y >> 8);
        let h = (t2 & 0xFF) as i64;
        let mi = ((t2 >> 24) & 0xFF) as i64;
        let se = ((t2 >> 48) & 0xFF) as i64;
        if h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let secs = days as i64 * 86400 + (h * 3600 + mi * 60 + se);
        Some((secs * SECOND, secs))
    }

    #[inline(always)]
    fn iso(&mut self, s: &[u8]) -> Option<i64> {
        const HI: u64 = 0x8080_8080_8080_8080;
        const ADD: u64 = 0x7676_7676_7676_7676;
        let lo = u64::from_le_bytes(s[0..8].try_into().unwrap());
        let x = lo ^ 0x2D30_302D_3030_3030;
        let t = u64::from_le_bytes(s[11..19].try_into().unwrap());
        let y = t ^ 0x3030_3A30_303A_3030;
        let d1 = dig(s[8]);
        let d2 = dig(s[9]);
        if ((x | x.wrapping_add(ADD) | y | y.wrapping_add(ADD)) & HI) != 0
            || (x & 0xFF00_00FF_0000_0000) != 0
            || (y & 0x0000_FF00_00FF_0000) != 0
            || d1 > 9
            || d2 > 9
            || s[10] != b'T'
        {
            return None;
        }
        // validated digits sit in bytes 0-3, 5, 6 of x; bytes 4 and 7 are zero, so the
        // day digits fit there and the result identifies the date exactly
        let key = x | ((d1 as u64) << 32) | ((d2 as u64) << 56);
        let idx = (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 54) as usize;
        let days = if self.keys[idx] == key {
            self.days[idx]
        } else {
            self.date_miss(x, d1, d2, key, idx)
        };
        if days == NO_DAY {
            return None;
        }
        // pairs of digits: byte 0 = hour, byte 3 = minute, byte 6 = second
        let t2 = y.wrapping_mul(10).wrapping_add(y >> 8);
        let h = (t2 & 0xFF) as i64;
        let mi = ((t2 >> 24) & 0xFF) as i64;
        let se = ((t2 >> 48) & 0xFF) as i64;
        if h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let mut ms = days as i64 * DAY + (h * 3600 + mi * 60 + se) * SECOND;
        if s.len() == 20 {
            if s[19] != b'Z' {
                return None;
            }
        } else {
            let sign = s[19];
            if (sign != b'+' && sign != b'-') || s[22] != b':' {
                return None;
            }
            let oh = two(s, 20)?;
            let om = two(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60 * SECOND;
            if sign == b'+' {
                ms -= off;
            } else {
                ms += off;
            }
        }
        Some(ms)
    }
}

/// Days since the epoch to (y, m, d).
#[allow(dead_code)]
pub fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms() {
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(f1));
        assert_eq!(parse_ts(b"0001791071890000"), Some(f1));
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), Some(f1));
        for bad in [
            &b""[..], b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z", b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10",
            b"1791071890000.0", b"1.79e12", b"-5", b"2026-13-03T23:58:10Z", b"2026-02-30T23:58:10Z",
            b"2027-02-29T23:58:10Z", b"2026-10-03T24:58:10Z", b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200", b"2026-10-03T23:58:10.123Z",
        ] {
            assert_eq!(parse_ts(bad), None, "{:?}", std::str::from_utf8(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"86400000"), Some(86_400_000));
        assert_eq!(parse_ts(b"179102160000000000000"), Some(i64::MAX));
        assert_eq!(civil_from_days(days_from_civil(2028, 2, 29)), (2028, 2, 29));
    }
    #[test]
    fn digits() {
        let mut x: u64 = 5;
        for _ in 0..300_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let n = 1 + (x >> 60) as usize % 16;
            let mut b: Vec<u8> = (0..n).map(|i| b'0' + ((x >> (i * 3)) % 10) as u8).collect();
            if (x >> 20) % 5 == 0 {
                let i = ((x >> 23) as usize) % n;
                b[i] = [b'a', b'/', b':', b' ', b'.', 0xD9, b'-'][((x >> 26) % 7) as usize];
            }
            let mut buf = b.clone();
            buf.resize(n + 16, b'7');
            assert_eq!(unsafe { digits16(buf.as_ptr(), n) }, parse_ts(&b), "{:?}", std::str::from_utf8(&b));
        }
    }
    #[test]
    fn fast_matches_slow() {
        let mut p = TsParser::new();
        let mut x: u64 = 99;
        let base = days_from_civil(1999, 1, 1);
        let cases: Vec<&[u8]> = vec![b"2026-10-03T23:58:10Z", b"2028-02-29T00:00:00+05:30", b"2027-02-29T00:00:00Z", b"2026-13-01T00:00:00Z", b"2026-00-01T00:00:00Z", b"2026-10-00T00:00:00Z", b"2026-10-3 T00:00:00Z", b"2026-10-03T00:00:00z", b"2026-1a-03T00:00:00Z", b"2026/10-03T00:00:00Z", b"2026-10-03T00;00:00Z", b"2026-10-03T00:00:00-23:59", b"2026-10-03T00:00:00+23:60"];
        for c in &cases {
            assert_eq!(p.parse(c), parse_ts(c), "{:?}", std::str::from_utf8(c));
        }
        for _ in 0..200_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let day = base + ((x >> 20) % 12_000) as i64;
            let (y, m, d) = civil_from_days(day);
            let r = x >> 40;
            let mut s = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}", y, m, (d + (r % 3) as i64 - 1).max(0), r % 25, (r >> 5) % 61, (r >> 11) % 61);
            if r % 2 == 0 { s.push('Z'); } else { s.push_str(&format!("{}{:02}:{:02}", if r % 4 == 1 { '+' } else { '-' }, (r >> 17) % 25, (r >> 22) % 61)); }
            let mut b = s.into_bytes();
            if (r >> 30) % 7 == 0 { let i = ((r >> 33) % b.len() as u64) as usize; b[i] = b"0123456789-:TZ+x"[((r >> 37) % 16) as usize]; }
            assert_eq!(p.parse(&b), parse_ts(&b), "{:?}", std::str::from_utf8(&b));
        }
    }
}
