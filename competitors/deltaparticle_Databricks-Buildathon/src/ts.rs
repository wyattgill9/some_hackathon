//! Timestamps (rules.md "Timestamps"): F1 `YYYY-MM-DDTHH:MM:SSZ`,
//! F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`, F3 ASCII digits (epoch ms). Anything else is bad.

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const DAY: i64 = 86_400_000;

/// An F3 value too long for i64: valid syntax, outside every window.
pub const HUGE: i64 = i64::MAX;

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some(a as i64 * 10 + b as i64)
    } else {
        None
    }
}

pub const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Days since the epoch of the first of each month, 1900-01 to 2199-12.
const TABLE_FIRST_YEAR: i64 = 1900;
const TABLE_YEARS: usize = 300;
static MONTH_START: [i32; TABLE_YEARS * 12] = {
    let mut t = [0i32; TABLE_YEARS * 12];
    let mut i = 0;
    while i < TABLE_YEARS * 12 {
        t[i] = days_from_civil(TABLE_FIRST_YEAR + (i / 12) as i64, (i % 12) as i64 + 1, 1) as i32;
        i += 1;
    }
    t
};

#[inline(always)]
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 => {
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

#[inline(always)]
fn le8(s: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(s[at..at + 8].try_into().unwrap())
}

const fn lanes(b: [u8; 8]) -> u64 {
    u64::from_le_bytes(b)
}

/// `w ^ pattern` must give 0..=9 in every lane of `digit_lanes` and 0 in the
/// other lanes (the separators). Returns the xored word.
#[inline(always)]
fn swar_check(w: u64, pattern: u64, digit_lanes: u64) -> Option<u64> {
    let x = w ^ pattern;
    // Every lane must be below 16, digit lanes below 10, others exactly 0.
    let bad = (x & 0xF0F0_F0F0_F0F0_F0F0)
        | ((x.wrapping_add(0x7676_7676_7676_7676)) & 0x8080_8080_8080_8080 & digit_lanes)
        | (x & !digit_lanes);
    if bad != 0 {
        None
    } else {
        Some(x)
    }
}

/// Two-digit numbers of adjacent lanes: lane i of the result is
/// 10 * lane i + lane i+1 (lanes hold 0..=9).
#[inline(always)]
fn pairs(x: u64) -> u64 {
    x.wrapping_mul(10).wrapping_add(x >> 8)
}

/// `YYYY-MM-DDTHH:MM:SS` in the first 19 bytes, as local epoch ms (SWAR).
#[inline(always)]
fn civil19(s: &[u8]) -> Option<i64> {
    // "YYYY-MM-" at 0..8 and "HH:MM:SS" at 11..19; "DD" and 'T' at 8..11.
    const DATE: u64 = lanes([b'0', b'0', b'0', b'0', b'-', b'0', b'0', b'-']);
    const DATE_DIGITS: u64 = lanes([0xFF, 0xFF, 0xFF, 0xFF, 0, 0xFF, 0xFF, 0]);
    const TIME: u64 = lanes([b'0', b'0', b':', b'0', b'0', b':', b'0', b'0']);
    const TIME_DIGITS: u64 = lanes([0xFF, 0xFF, 0, 0xFF, 0xFF, 0, 0xFF, 0xFF]);
    let d = swar_check(le8(s, 0), DATE, DATE_DIGITS)?;
    let t = swar_check(le8(s, 11), TIME, TIME_DIGITS)?;
    if s[10] != b'T' {
        return None;
    }
    let d1 = s[8].wrapping_sub(b'0') as i64;
    let d2 = s[9].wrapping_sub(b'0') as i64;
    if d1 > 9 || d2 > 9 {
        return None;
    }
    let dp = pairs(d);
    let tp = pairs(t);
    let year = (dp & 0xFF) as i64 * 100 + ((dp >> 16) & 0xFF) as i64;
    let month = ((dp >> 40) & 0xFF) as i64;
    let day = d1 * 10 + d2;
    let hour = (tp & 0xFF) as i64;
    let minute = ((tp >> 24) & 0xFF) as i64;
    let second = ((tp >> 48) & 0xFF) as i64;
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let y = year - TABLE_FIRST_YEAR;
    let days = if (0..TABLE_YEARS as i64).contains(&y) {
        MONTH_START[(y * 12 + month - 1) as usize] as i64 + day - 1
    } else {
        days_from_civil(year, month, day)
    };
    Some(days * DAY + ((hour * 60 + minute) * 60 + second) * SECOND)
}

/// Day numbers of recently seen `YYYY-MM-DD` dates. A dataset's valid
/// timestamps fall on a few dozen dates, so the date half of most ISO
/// timestamps is a lookup; only dates that validated go in.
pub struct DateCache {
    key: [u64; 64],
    key2: [u16; 64],
    days: [i64; 64],
}

impl DateCache {
    pub fn new() -> Self {
        Self { key: [0; 64], key2: [0; 64], days: [0; 64] }
    }
}

impl Default for DateCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Validated `YYYY-MM-DD` at 0..10 as days since the epoch.
#[inline(always)]
fn date_days(s: &[u8]) -> Option<i64> {
    const DATE: u64 = lanes([b'0', b'0', b'0', b'0', b'-', b'0', b'0', b'-']);
    const DATE_DIGITS: u64 = lanes([0xFF, 0xFF, 0xFF, 0xFF, 0, 0xFF, 0xFF, 0]);
    let d = swar_check(le8(s, 0), DATE, DATE_DIGITS)?;
    let d1 = s[8].wrapping_sub(b'0') as i64;
    let d2 = s[9].wrapping_sub(b'0') as i64;
    if d1 > 9 || d2 > 9 {
        return None;
    }
    let dp = pairs(d);
    let year = (dp & 0xFF) as i64 * 100 + ((dp >> 16) & 0xFF) as i64;
    let month = ((dp >> 40) & 0xFF) as i64;
    let day = d1 * 10 + d2;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    let y = year - TABLE_FIRST_YEAR;
    Some(if (0..TABLE_YEARS as i64).contains(&y) {
        MONTH_START[(y * 12 + month - 1) as usize] as i64 + day - 1
    } else {
        days_from_civil(year, month, day)
    })
}

/// `civil19` with the date half looked up in `cache`.
#[inline(always)]
fn civil19_c(s: &[u8], cache: &mut DateCache) -> Option<i64> {
    const TIME: u64 = lanes([b'0', b'0', b':', b'0', b'0', b':', b'0', b'0']);
    const TIME_DIGITS: u64 = lanes([0xFF, 0xFF, 0, 0xFF, 0xFF, 0, 0xFF, 0xFF]);
    let t = swar_check(le8(s, 11), TIME, TIME_DIGITS)?;
    if s[10] != b'T' {
        return None;
    }
    let tp = pairs(t);
    let hour = (tp & 0xFF) as i64;
    let minute = ((tp >> 24) & 0xFF) as i64;
    let second = ((tp >> 48) & 0xFF) as i64;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let k = le8(s, 0);
    let k2 = u16::from_le_bytes([s[8], s[9]]);
    let i = ((k ^ (k2 as u64) << 3).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 58) as usize;
    let days = if cache.key[i] == k && cache.key2[i] == k2 {
        cache.days[i]
    } else {
        let d = date_days(s)?;
        cache.key[i] = k;
        cache.key2[i] = k2;
        cache.days[i] = d;
        d
    };
    Some(days * DAY + ((hour * 60 + minute) * 60 + second) * SECOND)
}

/// `parse` with a date cache (same results).
#[inline(always)]
pub fn parse_c(s: &[u8], cache: &mut DateCache) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return civil19_c(s, cache);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') {
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let offset = (oh * 60 + om) * MINUTE;
        let local = civil19_c(s, cache)?;
        return Some(if s[19] == b'+' { local - offset } else { local + offset });
    }
    parse_f3(s)
}

/// The straightforward version, kept as the reference for tests.
#[cfg(test)]
fn civil19_ref(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = d2(s, 0)? * 100 + d2(s, 2)?;
    let month = d2(s, 5)?;
    let day = d2(s, 8)?;
    let hour = d2(s, 11)?;
    let minute = d2(s, 14)?;
    let second = d2(s, 17)?;
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let y = year - TABLE_FIRST_YEAR;
    let days = if (0..TABLE_YEARS as i64).contains(&y) {
        MONTH_START[(y * 12 + month - 1) as usize] as i64 + day - 1
    } else {
        days_from_civil(year, month, day)
    };
    Some(days * DAY + ((hour * 60 + minute) * 60 + second) * SECOND)
}

/// Epoch milliseconds, `HUGE` for an F3 value beyond i64, or None if bad.
#[inline(always)]
pub fn parse(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return civil19(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') {
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let offset = (oh * 60 + om) * MINUTE;
        let local = civil19(s)?;
        return Some(if s[19] == b'+' { local - offset } else { local + offset });
    }
    parse_f3(s)
}

/// Eight ASCII digits (first is most significant) to their value, or None.
#[inline(always)]
fn digits8(w: u64) -> Option<u64> {
    if (w & 0xF0F0_F0F0_F0F0_F0F0) | ((w.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) >> 4
        != 0x3030_3030_3030_3030 | 0x0303_0303_0303_0303
    {
        return None;
    }
    let mut v = w & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v.wrapping_mul(10 << 8 | 1)) >> 8;
    v = ((v & 0x00FF_00FF_00FF_00FF).wrapping_mul(100 << 16 | 1)) >> 16;
    v = ((v & 0x0000_FFFF_0000_FFFF).wrapping_mul(10000 << 32 | 1)) >> 32;
    Some(v)
}

#[inline(always)]
fn parse_f3(s: &[u8]) -> Option<i64> {
    if s.len() == 13 {
        if let Some(hi) = digits8(le8(s, 0)) {
            let mut lo = 0u64;
            let mut ok = true;
            for &b in &s[8..13] {
                let d = b.wrapping_sub(b'0');
                ok &= d < 10;
                lo = lo * 10 + d as u64;
            }
            if ok {
                return Some((hi * 100_000 + lo) as i64);
            }
            return None;
        }
        return None;
    }
    parse_f3_slow(s)
}

#[inline(never)]
fn parse_f3_slow(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut value: u64 = 0;
    let mut significant = 0usize;
    for &b in s {
        let d = b.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        if significant > 0 || d != 0 {
            significant += 1;
            if significant <= 18 {
                value = value * 10 + d as u64;
            }
        }
    }
    if significant > 18 {
        // Could still fit in i64 at 19 digits, but any such value is far
        // outside every window (year > 2286), so it is simply out of window.
        return Some(HUGE);
    }
    Some(value as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The SWAR parsers must agree with the reference on mutated inputs.
    #[test]
    fn swar_matches_reference() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let seeds: [&[u8]; 6] = [
            b"2026-10-03T23:58:10Z", b"2028-02-29T00:00:00Z", b"1999-12-31T23:59:59Z",
            b"2027-02-28T12:30:45Z", b"2025-01-01T09:05:07Z", b"2100-02-28T23:59:59Z",
        ];
        let alphabet = b"0123456789:-TtZz +/9";
        let mut cache = DateCache::new();
        for i in 0..400_000 {
            let mut v = seeds[i % seeds.len()].to_vec();
            for _ in 0..(next() % 3) {
                let at = (next() % 19) as usize;
                v[at] = alphabet[(next() % alphabet.len() as u64) as usize];
            }
            assert_eq!(civil19(&v), civil19_ref(&v), "{:?}", String::from_utf8_lossy(&v));
            assert_eq!(civil19_c(&v, &mut cache), civil19_ref(&v), "cached {:?}", String::from_utf8_lossy(&v));
        }
        for i in 0..400_000u64 {
            let mut v = format!("{:013}", next() % 10_000_000_000_000).into_bytes();
            if i % 3 == 0 {
                let at = (next() % 13) as usize;
                v[at] = alphabet[(next() % alphabet.len() as u64) as usize];
            }
            assert_eq!(parse_f3(&v), parse_f3_slow(&v), "{:?}", String::from_utf8_lossy(&v));
        }
    }
    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        for bad in [
            &b""[..], b"2026-10-03 10:00:00Z", b"2026-10-03T10:00:00z", b"2026-10-03t10:00:00Z",
            b"2026-13-03T10:00:00Z", b"2026-02-30T10:00:00Z", b"2027-02-29T10:00:00Z",
            b"2026-10-03T24:00:00Z", b"2026-10-03T10:00:60Z", b"2026-10-03T10:00:00+24:00",
            b"2026-10-03T10:00:00+05:60", b"2026-10-03T10:00:00+0200", b"1791071890000.0",
            b"1.79e12", b"-5", b"2026-10-03T10:00:00", b"2026-10-03T10:00:00.123Z",
        ] {
            assert_eq!(parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        assert!(parse(b"2028-02-29T10:00:00Z").is_some());
        for y in [1899i64, 1900, 1970, 2000, 2025, 2028, 2100, 2199, 2200] {
            for m in 1..=12 {
                let txt = format!("{y:04}-{m:02}-15T12:34:56Z");
                assert_eq!(parse(txt.as_bytes()), Some(days_from_civil(y, m, 15) * DAY + 45_296_000), "{txt}");
            }
        }
        assert_eq!(parse(b"123456789012345678901"), Some(HUGE));
        assert_eq!(parse(b"2026-10-03T10:00:00-00:00"), parse(b"2026-10-03T10:00:00Z"));
    }
}
