//! Dedup on event_id: keep the copy with the smallest ingest_ts.
//!
//! Only a few percent of events have more than one copy, so the work is finding
//! them. Each partition is scanned twice against a small table of two-bit
//! counters indexed by key hash: the first scan marks the slots hit more than
//! once, the second collects the copies in those slots. Only those few are
//! sorted, by (event_id, ingest_ts); in each run of one event_id the first copy
//! wins and the event records of the others are marked dropped.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::parse::{Copy_, Event, Parsed, DROPPED, NO_EVENT};
use crate::pool::{self, Pool};
use crate::sys;

/// Returns how many event records were dropped.
pub fn run(
    copies: &Pool<Copy_>,
    events: &Pool<Event>,
    parsed: &[Parsed],
    partitions: usize,
    dedup_shift: u32,
    threads: usize,
) -> u64 {
    let lists: Vec<&pool::Chunks> = parsed.iter().map(|p| &p.copies).collect();
    let next = AtomicUsize::new(0);
    let partition_bits = 64 - dedup_shift;
    let dropped = sys::parallel(threads, |_| {
        // Two bits a slot, 32 slots a word: seen once, seen again.
        let mut state: Vec<u64> = Vec::new();
        let mut suspects: Vec<Copy_> = Vec::new();
        let mut dropped = 0u64;
        loop {
            let partition = next.fetch_add(1, Ordering::Relaxed);
            if partition >= partitions {
                break;
            }
            let count: usize = lists.iter().map(|chunks| chunks[partition].iter().map(|c| c.1 as usize).sum::<usize>()).sum();
            if count < 2 {
                continue;
            }
            // 16 slots a copy keeps chance collisions near 6%.
            let slots = (count * 16).next_power_of_two().max(1024);
            let slot_shift = 64 - slots.trailing_zeros();
            state.clear();
            state.resize(slots / 32, 0);
            // The partition took the hash's top bits; the slot takes the ones below.
            let slot = |copy: &Copy_| ((sys::mix(copy.event) << partition_bits) >> slot_shift) as usize;
            for chunks in &lists {
                for &(start, len) in &chunks[partition] {
                    let records = unsafe { std::slice::from_raw_parts(copies.at(start), len as usize) };
                    for copy in records {
                        let at = slot(copy);
                        unsafe {
                            let word = state.get_unchecked_mut(at >> 5);
                            let shift = (at & 31) * 2;
                            let seen = (*word >> shift) & 1;
                            *word |= (1 | (seen << 1)) << shift;
                        }
                    }
                }
            }
            suspects.clear();
            for chunks in &lists {
                for &(start, len) in &chunks[partition] {
                    let records = unsafe { std::slice::from_raw_parts(copies.at(start), len as usize) };
                    for copy in records {
                        let at = slot(copy);
                        if unsafe { *state.get_unchecked(at >> 5) } >> ((at & 31) * 2 + 1) & 1 != 0 {
                            suspects.push(*copy);
                        }
                    }
                }
            }
            suspects.sort_unstable_by_key(|copy| (copy.event, copy.ingest));
            let mut i = 0;
            while i < suspects.len() {
                let mut j = i + 1;
                while j < suspects.len() && suspects[j].event == suspects[i].event {
                    // A later copy, or an identical one with the same ingest_ts: it loses.
                    if suspects[j].at != NO_EVENT {
                        unsafe { (*events.at(suspects[j].at)).ts = DROPPED };
                        dropped += 1;
                    }
                    j += 1;
                }
                i = j;
            }
        }
        dropped
    });
    dropped.iter().sum()
}
