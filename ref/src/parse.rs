//! The rules.md CSV dialect and value parsers. Byte-exact: no trimming, ASCII digits only.

/// Split one line into exactly `N` fields, unquoting each; `None` if the line is malformed.
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out = [&line[..0]; N];
    let mut n = 0;
    for f in line.split(|&b| b == b',') {
        if n == N {
            return None;
        }
        out[n] = unquote(f)?;
        n += 1;
    }
    (n == N).then_some(out)
}

fn unquote(f: &[u8]) -> Option<&[u8]> {
    if !f.contains(&b'"') {
        return Some(f);
    }
    let inner = f.strip_prefix(b"\"")?.strip_suffix(b"\"")?;
    (!inner.contains(&b'"')).then_some(inner)
}

/// Digits only, saturating at u64::MAX. `None` for empty or any non-ASCII-digit byte.
fn digits(b: &[u8]) -> Option<u64> {
    if b.is_empty() || !b.iter().all(u8::is_ascii_digit) {
        return None;
    }
    Some(b.iter().fold(0u64, |v, &c| v.saturating_mul(10).saturating_add((c - b'0') as u64)))
}

/// A purchase price: digits only, 1 to 10,000,000.
pub fn price(b: &[u8]) -> Option<u32> {
    digits(b).filter(|v| (1..=10_000_000).contains(v)).map(|v| v as u32)
}

/// F1, F2 or F3 to epoch milliseconds UTC. An F3 too big for i64 saturates (it is out of any window).
pub fn ts(b: &[u8]) -> Option<i64> {
    if let Some(v) = digits(b) {
        return Some(v.min(i64::MAX as u64) as i64);
    }
    match b.len() {
        20 if b[19] == b'Z' => datetime(b),
        25 if matches!(b[19], b'+' | b'-') && b[22] == b':' => {
            let (h, m) = (d2(b, 20)?, d2(b, 23)?);
            if h > 23 || m > 59 {
                return None;
            }
            let off = (h * 60 + m) * 60_000;
            Some(if b[19] == b'+' { datetime(b)? - off } else { datetime(b)? + off })
        }
        _ => None,
    }
}

fn d2(b: &[u8], i: usize) -> Option<i64> {
    let (x, y) = (b[i].wrapping_sub(b'0'), b[i + 1].wrapping_sub(b'0'));
    (x < 10 && y < 10).then_some((x * 10 + y) as i64)
}

/// `YYYY-MM-DDTHH:MM:SS` in b[..19], calendar-checked.
fn datetime(b: &[u8]) -> Option<i64> {
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let (mo, d, h, mi, s) = (d2(b, 5)?, d2(b, 8)?, d2(b, 11)?, d2(b, 14)?, d2(b, 17)?);
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let dim = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if d < 1 || d > dim || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + s * 1000)
}

/// Howard Hinnant's days_from_civil.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect() {
        assert_eq!(fields::<2>(b"\"u4\",u1"), Some([&b"u4"[..], b"u1"]));
        assert_eq!(fields::<2>(b"\"\",u1"), Some([&b""[..], b"u1"]));
        assert_eq!(fields::<2>(b" u1,u1 \r"), Some([&b" u1"[..], b"u1 \r"]));
        assert_eq!(fields::<2>(b"u1,u5\""), None);
        assert_eq!(fields::<2>(b"u1\"x\",u5"), None);
        assert_eq!(fields::<2>(b"\""), None);
        assert_eq!(fields::<2>(b"u1,u2,u3"), None);
        assert_eq!(fields::<2>(b"u1"), None);
        assert_eq!(fields::<2>(b""), None);
    }

    #[test]
    fn timestamps() {
        let f1 = ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(ts(b"1791071890000"), Some(f1));
        assert_eq!(ts(b"0001791071890000"), Some(f1));
        assert_eq!(ts(b"2026-10-04T01:30:00+02:00"), ts(b"2026-10-03T23:30:00Z"));
        assert_eq!(ts(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(ts(b"2026-10-03T23:58:10+23:59"), Some(f1 - (23 * 60 + 59) * 60_000));
        assert_eq!(ts(b"0"), Some(0));
        assert_eq!(ts(b"123456789012345678901"), Some(i64::MAX));
        assert_eq!(ts(b"2028-02-29T00:00:00Z"), Some(1_835_395_200_000));
        assert_eq!(ts(b"2000-02-29T00:00:00Z"), Some(951_782_400_000));
        for bad in [
            &b""[..], b"2027-02-29T00:00:00Z", b"2100-02-29T00:00:00Z", b"2026-13-01T00:00:00Z",
            b"2026-02-30T00:00:00Z", b"2026-10-03T24:00:00Z", b"2026-10-03T23:59:60Z",
            b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z", b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10", b"2026-10-03T23:58:10.000Z", b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200", b"2026-10-03T23:58:10Z\r",
            b"1791071890000.0", b"1.79e12", b"-5", "٢٥٠".as_bytes(), "２０26-10-03T23:58:10Z".as_bytes(),
            b"2026-00-03T23:58:10Z", b"2026-10-00T23:58:10Z",
        ] {
            assert_eq!(ts(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn prices() {
        assert_eq!(price(b"0250"), Some(250));
        assert_eq!(price(b"10000000"), Some(10_000_000));
        for bad in [&b""[..], b"0", b"10000001", b"19.99", b"-5", b"+5", b"1000\r", "２５０".as_bytes(), "٢٥٠".as_bytes()] {
            assert_eq!(price(bad), None);
        }
        assert_eq!(price(b"99999999999999999999999"), None);
    }
}
