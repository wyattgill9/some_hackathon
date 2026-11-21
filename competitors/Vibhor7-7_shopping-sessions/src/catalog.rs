//! The small inputs: manifest.json, products.csv and campaigns.csv.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use crate::key::{self, Key};
use crate::sys;
use crate::ts;

pub const NONE: u32 = u32::MAX;

/// A fixed open-addressing map from packed IDs to small numbers.
struct KeyMap {
    slots: Vec<(Key, u32)>,
    mask: usize,
    shift: u32,
}

impl KeyMap {
    fn new(entries: usize) -> Self {
        let capacity = (entries * 2).max(16).next_power_of_two();
        Self { slots: vec![(0, NONE); capacity], mask: capacity - 1, shift: 64 - capacity.trailing_zeros() }
    }

    fn insert(&mut self, id: Key, value: u32) {
        let mut at = (sys::mix(id) >> self.shift) as usize;
        while self.slots[at].0 != 0 {
            if self.slots[at].0 == id {
                return;
            }
            at = (at + 1) & self.mask;
        }
        self.slots[at] = (id, value);
    }

    #[inline(always)]
    fn get(&self, id: Key) -> u32 {
        let mut at = (sys::mix(id) >> self.shift) as usize;
        loop {
            let slot = unsafe { *self.slots.get_unchecked(at) };
            if slot.0 == id || slot.0 == 0 {
                return slot.1;
            }
            at = (at + 1) & self.mask;
        }
    }
}

/// IDs made of a short prefix and a number, as the two files' own IDs are (`p123`,
/// `c_45`), found by the number in a small array instead of by a hash in a table
/// several times the size: the campaigns' fits the first cache level.
///
/// It only ever answers for text of exactly that shape (the prefix, then 1 to 7
/// digits, the first not `0`) and a number inside the array; anything else is
/// left to the map, which holds every ID. So it is exact for any file: in the
/// worst case, one whose IDs have no such shape, it never answers.
struct Numbered {
    /// The prefix as it reads in memory, the bits of it that count, and its length.
    prefix: u64,
    prefix_mask: u64,
    prefix_len: usize,
    /// Value + 1 for each number; 0 where no ID has that number.
    by_number: Vec<u16>,
}

/// The value of a `Numbered` lookup that has to be made in the map instead.
const ASK_THE_MAP: u32 = u32::MAX - 1;

impl Numbered {
    const MOST_DIGITS: usize = 7;
    const MOST_NUMBERS: usize = 1 << 20;

    /// The number of `id` if it is `prefix_len` bytes and then 1 to 7 digits not starting with `0`.
    fn number_of(id: &[u8], prefix_len: usize) -> Option<usize> {
        let digits = id.get(prefix_len..)?;
        if digits.is_empty() || digits.len() > Self::MOST_DIGITS || digits[0] == b'0' || !digits.iter().all(u8::is_ascii_digit) {
            return None;
        }
        Some(digits.iter().fold(0, |number, &digit| number * 10 + usize::from(digit - b'0')))
    }

    fn new<'a>(entries: impl Iterator<Item = (&'a [u8], u32)> + Clone) -> Self {
        let none = Self { prefix: 0, prefix_mask: 0, prefix_len: 0, by_number: Vec::new() };
        let Some((first, _)) = entries.clone().next() else { return none };
        let prefix_len = first.iter().position(u8::is_ascii_digit).unwrap_or(0);
        if prefix_len == 0 || prefix_len > 4 {
            return none;
        }
        let prefix_bytes = &first[..prefix_len];
        let shaped = |id: &[u8]| if id.starts_with(prefix_bytes) { Self::number_of(id, prefix_len) } else { None };
        let numbers = entries.clone().filter_map(|(id, _)| shaped(id)).filter(|&n| n < Self::MOST_NUMBERS).max().map_or(0, |n| n + 1);
        let mut by_number = vec![0u16; numbers];
        for (id, value) in entries {
            if let (Some(number), Ok(stored)) = (shaped(id), u16::try_from(u64::from(value) + 1)) {
                // As in the map, the first line to name an ID is the one that counts.
                if number < numbers && by_number[number] == 0 {
                    by_number[number] = stored;
                }
            } else if shaped(id).is_some_and(|number| number < numbers) {
                return none; // a value too large for the array: let the map answer everything
            }
        }
        let mut prefix = [0u8; 8];
        prefix[..prefix_len].copy_from_slice(prefix_bytes);
        Self { prefix: u64::from_le_bytes(prefix), prefix_mask: (1u64 << (8 * prefix_len)) - 1, prefix_len, by_number }
    }

    /// The value for the ID at `id` (16 readable bytes), NONE if it has this
    /// shape and no ID has its number, or ASK_THE_MAP.
    #[inline(always)]
    unsafe fn get(&self, id: *const u8, len: usize) -> u32 {
        let digits = len.wrapping_sub(self.prefix_len);
        if digits.wrapping_sub(1) >= Self::MOST_DIGITS || (id as *const u64).read_unaligned() & self.prefix_mask != self.prefix {
            return ASK_THE_MAP;
        }
        // The digits moved to the end of eight bytes, with `0`s in front of them.
        let shift = 64 - 8 * digits as u32;
        let text = (id.add(self.prefix_len) as *const u64).read_unaligned() << shift | (0x3030_3030_3030_3030 & ((1u64 << shift) - 1));
        let values = text ^ 0x3030_3030_3030_3030;
        let not_digits = (values | values.wrapping_add(0x7676_7676_7676_7676)) & 0x8080_8080_8080_8080;
        // The first digit is the byte at `shift`: a number written with a leading 0 is another ID.
        let leading_zero = (values >> shift) & 0xff == 0;
        let pairs = (values & 0x0f0f_0f0f_0f0f_0f0f).wrapping_mul(2_561) >> 8;
        let fours = (pairs & 0x00ff_00ff_00ff_00ff).wrapping_mul(6_553_601) >> 16;
        let number = ((fours & 0x0000_ffff_0000_ffff).wrapping_mul(42_949_672_960_001) >> 32) as usize;
        if not_digits != 0 || leading_zero || number >= self.by_number.len() {
            return ASK_THE_MAP;
        }
        match *self.by_number.get_unchecked(number) {
            0 => NONE,
            stored => u32::from(stored) - 1,
        }
    }
}

/// IDs that pack go in the fast map; any other spelling in the exact one.
struct IdMap {
    numbered: Numbered,
    packed: KeyMap,
    other: HashMap<Vec<u8>, u32>,
}

impl IdMap {
    fn new<'a>(entries: impl ExactSizeIterator<Item = (&'a [u8], u32)> + Clone) -> Self {
        let mut map = Self { numbered: Numbered::new(entries.clone()), packed: KeyMap::new(entries.len()), other: HashMap::new() };
        for (id, value) in entries {
            let packed = key::pack(id);
            if packed != 0 {
                map.packed.insert(packed, value);
            } else {
                map.other.entry(id.to_vec()).or_insert(value);
            }
        }
        map
    }

    /// Looks up an ID in the reader's buffer (16 readable bytes at `id`).
    #[inline(always)]
    unsafe fn get(&self, id: *const u8, len: usize) -> u32 {
        let found = self.numbered.get(id, len);
        if found != ASK_THE_MAP {
            return found;
        }
        self.get_mapped(id, len)
    }

    /// The lookup through the hash maps, which hold every ID.
    #[inline(never)]
    unsafe fn get_mapped(&self, id: *const u8, len: usize) -> u32 {
        let packed = key::pack_padded(id, len);
        if packed != 0 {
            self.packed.get(packed)
        } else {
            self.get_other(std::slice::from_raw_parts(id, len))
        }
    }

    #[cold]
    #[inline(never)]
    fn get_other(&self, id: &[u8]) -> u32 {
        self.other.get(id).copied().unwrap_or(NONE)
    }
}

pub struct Catalog {
    pub window_start: i64,
    pub window_end: i64,
    products: IdMap,
    /// Category names in byte order, `unknown` among them: a category's number is its rank.
    pub categories: Vec<Vec<u8>>,
    unknown: u32,
    campaigns: IdMap,
    pub campaign_ids: Vec<Vec<u8>>,
    pub channels: Vec<Vec<u8>>,
}

impl Catalog {
    /// The category number of a product; `unknown`'s if the product is not in the file.
    #[inline(always)]
    pub unsafe fn category(&self, product: *const u8, len: usize) -> u32 {
        let found = self.products.get(product, len);
        if found == NONE {
            self.unknown
        } else {
            found
        }
    }

    /// The campaign's number, or NONE if it is not in campaigns.csv.
    #[inline(always)]
    pub unsafe fn campaign(&self, id: *const u8, len: usize) -> u32 {
        self.campaigns.get(id, len)
    }
}

/// The two fields of each data line of a two-column file, by the CSV dialect.
fn two_columns(content: &[u8]) -> Vec<(&[u8], &[u8])> {
    let mut rows = Vec::with_capacity(content.len() / 12);
    let mut lines = content.split(|&b| b == b'\n');
    lines.next(); // header
    for line in lines {
        let mut parts = line.split(|&b| b == b',');
        let (Some(a), Some(b), None) = (parts.next(), parts.next(), parts.next()) else { continue };
        let (Some(a), Some(b)) = (unquoted(a), unquoted(b)) else { continue };
        rows.push((a, b));
    }
    rows
}

fn unquoted(field: &[u8]) -> Option<&[u8]> {
    match field.iter().filter(|&&b| b == b'"').count() {
        0 => Some(field),
        2 if field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' => Some(&field[1..field.len() - 1]),
        _ => None,
    }
}

fn manifest_time(text: &[u8], name: &str) -> i64 {
    let needle = format!("\"{name}\"");
    let found = text.windows(needle.len()).position(|w| w == needle.as_bytes());
    let value = found.and_then(|at| {
        let rest = &text[at + needle.len()..];
        let open = rest.iter().position(|&b| b == b'"')? + 1;
        let close = open + rest[open..].iter().position(|&b| b == b'"')?;
        Some(&rest[open..close])
    });
    let parsed = value.map_or(ts::BAD, |v| unsafe { ts::parse_plain(v.as_ptr(), v.len()) });
    if parsed == ts::BAD {
        eprintln!("pipeline: manifest.json has no valid {name}");
        std::process::exit(2);
    }
    parsed
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| {
        eprintln!("pipeline: cannot read {}: {e}", path.display());
        std::process::exit(2);
    })
}

pub fn load(data: &Path) -> Catalog {
    let manifest = read(&data.join("manifest.json"));
    let window_start = manifest_time(&manifest, "window_start");
    let window_end = manifest_time(&manifest, "window_end");

    let product_file = read(&data.join("products.csv"));
    let products = two_columns(&product_file);
    // Few categories, many products: number each name as it first appears, then rank the names.
    let mut seen: HashMap<&[u8], u32> = HashMap::new();
    let mut names: Vec<&[u8]> = Vec::new();
    let mut last: (&[u8], u32) = (b"", u32::MAX);
    let first_seen: Vec<u32> = products
        .iter()
        .map(|&(_, category)| {
            if last.1 == u32::MAX || last.0 != category {
                let number = *seen.entry(category).or_insert_with(|| {
                    names.push(category);
                    names.len() as u32 - 1
                });
                last = (category, number);
            }
            last.1
        })
        .collect();
    let mut sorted: BTreeSet<&[u8]> = names.iter().copied().collect();
    sorted.insert(b"unknown");
    let categories: Vec<Vec<u8>> = sorted.iter().map(|n| n.to_vec()).collect();
    let rank_of = |name: &[u8]| sorted.iter().position(|n| *n == name).unwrap() as u32;
    let rank: Vec<u32> = names.iter().map(|n| rank_of(n)).collect();
    let unknown = rank_of(b"unknown");
    let product_map = IdMap::new(products.iter().zip(&first_seen).map(|(&(id, _), &number)| (id, rank[number as usize])));

    let campaign_file = read(&data.join("campaigns.csv"));
    let campaigns = two_columns(&campaign_file);
    let campaign_map = IdMap::new(campaigns.iter().enumerate().map(|(i, &(id, _))| (id, i as u32)));

    Catalog {
        window_start,
        window_end,
        products: product_map,
        categories,
        unknown,
        campaigns: campaign_map,
        campaign_ids: campaigns.iter().map(|(id, _)| id.to_vec()).collect(),
        channels: campaigns.iter().map(|(_, channel)| channel.to_vec()).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a lookup must return: the value of the first entry spelled exactly like `id`.
    fn expected(entries: &[(Vec<u8>, u32)], id: &[u8]) -> u32 {
        entries.iter().find(|(known, _)| known == id).map_or(NONE, |&(_, value)| value)
    }

    /// `numbers`: how long the array by number must have come out (0: the map answers everything).
    fn check(entries: &[(Vec<u8>, u32)], probes: &[Vec<u8>], numbers: usize) {
        let map = IdMap::new(entries.iter().map(|(id, value)| (id.as_slice(), *value)));
        assert_eq!(map.numbered.by_number.len(), numbers);
        for probe in probes {
            // As in the reader's buffer: other bytes after the field, 16 of them readable.
            for tail in [&b",2026-10-03T23:58:10Z\n"[..], b"99999999999999999999", b"\"\n\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"] {
                let mut text = probe.clone();
                text.extend_from_slice(tail);
                let found = unsafe { map.get(text.as_ptr(), probe.len()) };
                assert_eq!(found, expected(entries, probe), "{:?} before {:?}", String::from_utf8_lossy(probe), String::from_utf8_lossy(tail));
            }
        }
    }

    #[test]
    fn numbered_ids_are_found_exactly_as_the_map_finds_them() {
        let mut entries: Vec<(Vec<u8>, u32)> = (1..=2_000u32).map(|n| (format!("c_{n}").into_bytes(), n * 7 % 2_000)).collect();
        // IDs of other shapes beside them, a repeated ID (the first one counts), and numbers far apart.
        for (id, value) in [("c_007", 3), ("c_0", 4), ("c_", 5), ("x9", 6), ("c_12", 1_999), ("c_3000", 8), ("C_5", 9), ("c_5x", 10), ("c_999999", 11), ("c_1234567", 12), ("c_12345678", 13), ("summer sale", 14), ("5", 15)] {
            entries.push((id.as_bytes().to_vec(), value));
        }
        let mut probes: Vec<Vec<u8>> = entries.iter().map(|(id, _)| id.clone()).collect();
        for n in (0..=3_100u32).chain([9_999, 10_000, 99_999, 999_998, 999_999, 1_000_000, 1_048_575, 1_048_576, 1_234_567, 9_999_999, 12_345_678, 99_999_999]) {
            for form in [format!("c_{n}"), format!("c_0{n}"), format!("C_{n}"), format!("c{n}"), format!("c_{n} "), format!("c_{n}a"), format!("d_{n}"), format!("{n}")] {
                probes.push(form.into_bytes());
            }
        }
        for odd in ["", "c", "c_", "c_-1", "c_+1", "c_1.0", "c_1e3", "c_\u{661}", "c_1\r", "_c1", "c_0000001", "c_00000001", "c_10000000"] {
            probes.push(odd.as_bytes().to_vec());
        }
        check(&entries, &probes, 1_000_000);

        // A file whose IDs have no such shape: every answer comes from the map.
        let plain: Vec<(Vec<u8>, u32)> = ["spring", "summer", "c_1", "9lives", "c_"].iter().enumerate().map(|(i, id)| (id.as_bytes().to_vec(), i as u32)).collect();
        check(&plain, &probes, 0);
        // Values too large for the array: the map answers.
        let wide: Vec<(Vec<u8>, u32)> = (1..=300u32).map(|n| (format!("p{n}").into_bytes(), n * 1_000)).collect();
        let wide_probes: Vec<Vec<u8>> = (0..=400u32).map(|n| format!("p{n}").into_bytes()).collect();
        check(&wide, &wide_probes, 0);
        // A long prefix, and products as the generator names them.
        let long: Vec<(Vec<u8>, u32)> = (1..=50u32).map(|n| (format!("prod{n}").into_bytes(), n)).collect();
        let long_probes: Vec<Vec<u8>> = (0..=60u32).flat_map(|n| [format!("prod{n}"), format!("pro{n}"), format!("prod0{n}"), format!("prodx{n}")]).map(String::into_bytes).collect();
        check(&long, &long_probes, 51);
    }
}
