//! How values are written: timestamps in the three valid forms and the bad
//! ones, IDs, and CSV lines with random quoting.

use std::io::Write;

use crate::random::Random;

pub const SECOND: i64 = 1_000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

/// Real-world offsets from -10:00 to +12:00, in minutes.
const OFFSETS: [i64; 31] = [
    -600, -570, -540, -480, -420, -360, -300, -240, -210, -180, -120, -60, 0, 60, 120, 180, 210,
    240, 270, 300, 330, 345, 360, 390, 420, 480, 525, 540, 570, 600, 720,
];

pub struct Civil {
    pub year: i64,
    pub month: i64,
    pub day: i64,
    pub hour: i64,
    pub minute: i64,
    pub second: i64,
}

/// The calendar fields of `milliseconds` since the epoch (Howard Hinnant's
/// `civil_from_days`); the milliseconds themselves are dropped.
pub fn civil(milliseconds: i64) -> Civil {
    let days = milliseconds.div_euclid(DAY);
    let seconds_of_day = milliseconds.rem_euclid(DAY) / SECOND;
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    Civil {
        year: year_of_era + era * 400 + i64::from(month <= 2),
        month,
        day: day_of_year - (153 * month_index + 2) / 5 + 1,
        hour: seconds_of_day / 3_600,
        minute: seconds_of_day / 60 % 60,
        second: seconds_of_day % 60,
    }
}

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn write_civil(out: &mut Vec<u8>, at: &Civil, separator: char) {
    let Civil {
        year,
        month,
        day,
        hour,
        minute,
        second,
    } = at;
    write!(
        out,
        "{year:04}-{month:02}-{day:02}{separator}{hour:02}:{minute:02}:{second:02}"
    )
    .unwrap();
}

/// Form F1, `2026-10-03T23:58:10Z`. The milliseconds must be whole seconds.
pub fn write_utc(out: &mut Vec<u8>, milliseconds: i64) {
    write_civil(out, &civil(milliseconds), 'T');
    out.push(b'Z');
}

/// Form F2, `2026-10-04T01:28:10+01:30`: the local time at that offset.
fn write_with_offset(out: &mut Vec<u8>, milliseconds: i64, offset_minutes: i64) {
    write_civil(out, &civil(milliseconds + offset_minutes * MINUTE), 'T');
    let sign = if offset_minutes < 0 { '-' } else { '+' };
    let offset = offset_minutes.abs();
    write!(out, "{sign}{:02}:{:02}", offset / 60, offset % 60).unwrap();
}

/// A valid `ts`: F1 60%, F2 10%, F3 30%. Only F3 carries milliseconds, so a
/// timestamp that is not a whole second is always F3.
pub fn valid_timestamp(out: &mut Vec<u8>, random: &mut Random, milliseconds: i64) {
    let draw = random.below(10);
    if milliseconds % SECOND != 0 || draw >= 7 {
        write!(out, "{milliseconds}").unwrap();
    } else if draw == 6 {
        write_with_offset(out, milliseconds, *random.pick(&OFFSETS));
    } else {
        write_utc(out, milliseconds);
    }
}

/// One of every bad form in the README's "Timestamps", built from the true time so it looks
/// like the rows around it. `outside` is a time outside the row's window.
pub fn bad_timestamp(out: &mut Vec<u8>, random: &mut Random, milliseconds: i64, outside: i64) {
    let seconds = milliseconds - milliseconds.rem_euclid(SECOND);
    let at = civil(seconds);
    match random.below(15) {
        0 => {}
        1 => {
            write_civil(out, &at, 'T');
            write!(out, ".{:03}Z", milliseconds.rem_euclid(SECOND)).unwrap();
        }
        2 => {
            write_civil(out, &at, 'T');
            out.push(b'z');
        }
        3 => {
            write_civil(out, &at, ' ');
            out.push(b'Z');
        }
        4 => write_civil(out, &at, 'T'),
        5 => write!(out, "{milliseconds}.0").unwrap(),
        6 => write!(
            out,
            "{}.{:02}e12",
            milliseconds / 1_000_000_000_000,
            milliseconds / 10_000_000_000 % 100
        )
        .unwrap(),
        7 => write!(out, "-{}", random.between(1, 9)).unwrap(),
        8 => {
            write_civil(out, &Civil { month: 13, ..at }, 'T');
            out.push(b'Z');
        }
        9 => {
            write_civil(
                out,
                &Civil {
                    month: 2,
                    day: 30,
                    ..at
                },
                'T',
            );
            out.push(b'Z');
        }
        10 => {
            write_civil(out, &Civil { hour: 24, ..at }, 'T');
            out.push(b'Z');
        }
        11 => {
            write_civil(out, &Civil { second: 60, ..at }, 'T');
            out.push(b'Z');
        }
        12 => {
            write_civil(out, &at, 't');
            out.push(b'Z');
        }
        13 => {
            // The row's own year may be a leap year (some windows hold 2028), so
            // the year before it is used then: never two leap years in a row.
            let year = if is_leap_year(at.year) {
                at.year - 1
            } else {
                at.year
            };
            write_civil(
                out,
                &Civil {
                    year,
                    month: 2,
                    day: 29,
                    ..at
                },
                'T',
            );
            out.push(b'Z');
        }
        _ => valid_timestamp(out, random, outside),
    }
}

/// Lowercase base 36, so an ID has letters a case variant can change.
pub fn base36(out: &mut Vec<u8>, value: u64) {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buffer = [0u8; 13];
    let mut at = buffer.len();
    let mut rest = value;
    loop {
        at -= 1;
        buffer[at] = DIGITS[(rest % 36) as usize];
        rest /= 36;
        if rest == 0 {
            break;
        }
    }
    out.extend_from_slice(&buffer[at..]);
}

/// A bijection on `0..2^40`, so IDs look random and never collide. Multiplying
/// by an odd number and xor-shifting right are each invertible modulo 2^40, so
/// any odd multipliers would do; these two are fixed so IDs never change.
pub fn scramble(value: u64, key: u64) -> u64 {
    const MASK: u64 = (1 << 40) - 1;
    let mut mixed = value & MASK;
    mixed = (mixed.wrapping_mul(0x9E37_79B9_7F4B) ^ key) & MASK;
    mixed ^= mixed >> 17;
    mixed.wrapping_mul(0xD6E8_FEB8_6659) & MASK
}

#[cfg(test)]
mod tests {
    use shopping_sessions_reference::timestamp;

    use super::*;
    use crate::random::Stream;

    /// The reference rejects every bad timestamp, and the forms checked below all come out.
    #[test]
    fn bad_timestamps_are_rejected_and_include_these_forms() {
        let mut random = Random::new(7, Stream::Dataset, 0);
        let true_time = timestamp::parse("2028-02-27T10:11:12Z").unwrap();
        let outside = timestamp::parse("2028-03-30T00:00:00Z").unwrap();
        let mut seen = Vec::new();
        for _ in 0..2_000 {
            let mut out = Vec::new();
            bad_timestamp(&mut out, &mut random, true_time, outside);
            let text = String::from_utf8(out).unwrap();
            let in_window = timestamp::parse(&text).filter(|parsed| *parsed != outside);
            assert_eq!(in_window, None, "{text:?} should be bad");
            seen.push(text);
        }
        for (form, found) in [
            ("lowercase t", seen.iter().any(|text| text.contains('t'))),
            ("lowercase z", seen.iter().any(|text| text.ends_with('z'))),
            (
                "February 29 outside a leap year",
                seen.iter().any(|text| text.starts_with("2027-02-29")),
            ),
            (
                "February 30",
                seen.iter().any(|text| text.contains("-02-30")),
            ),
            ("empty", seen.iter().any(String::is_empty)),
        ] {
            assert!(found, "no {form} among the bad timestamps");
        }
    }
}
