//! Linked IDs: every ID that appears in `links/`, mapped to its person's name.
//!
//! The lines are parsed in parallel into edges, then joined with a lock-free
//! union-find laid out in one open-addressing table. A root always takes the
//! byte-wise smaller of two IDs as its parent's key, so when the joins are done
//! the root of every tree is the smallest ID of its person: the name rules.md
//! asks for, with no separate pass to find it.

use std::fs::File;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::key::{self, Key};
use crate::reader::{self, Input, Job, Lines, ReadBuf};
use crate::sys;

/// ID to person. IDs that no link line names are not in it: they are their own person.
pub struct Links {
    /// Cache lines of four slots: four ID keys, then the four people they belong to.
    /// An ID key of 0 is an empty slot.
    table: sys::Shared<u64>,
    mask: usize,
    shift: u32,
    pub ids: usize,
    /// Milliseconds spent parsing, joining and naming, for the log.
    pub timing: [f64; 3],
}

impl Links {
    /// Where the search for `id` starts: always the first of the four slots that
    /// share a cache line, so a lookup nearly always ends in the line it started
    /// in, the one line `prefetch` asked for.
    #[inline(always)]
    fn home(&self, id: Key) -> usize {
        home(id, self.shift)
    }

    /// Where the lookup of `id` starts, and a request to the CPU to fetch that
    /// line now: the lookup itself comes some lines later (`person_at`).
    #[inline(always)]
    pub fn prefetch(&self, id: Key) -> u32 {
        let home = self.home(id);
        sys::prefetch(unsafe { self.table.0.add(id_word(home)) });
        home as u32
    }

    /// The name of the person `id` belongs to; `home` is what `prefetch(id)` returned.
    ///
    /// Whether an ID is linked is a coin toss from line to line, so the four
    /// slots of its cache line are compared at once and the answer selected,
    /// rather than branched on. Only an ID pushed out of its line (rare)
    /// takes the loop.
    #[inline(always)]
    pub fn person_at(&self, id: Key, home: u32) -> Key {
        unsafe {
            let line = self.table.0.add(id_word(home as usize)) as *const u64; // four IDs, then their four people
            let (found, empty) = line_masks(line, id);
            let stop = found | empty;
            if stop != 0 {
                let slot = stop.trailing_zeros() as usize; // the first slot that is `id` or free
                let person = *line.add(4 + slot);
                let is_found = (found >> slot) & 1 != 0;
                return std::hint::select_unpredictable(is_found, person, id);
            }
            self.person_from(id, (home as usize + 4) & self.mask)
        }
    }

    #[cold]
    #[inline(never)]
    fn person_from(&self, id: Key, mut at: usize) -> Key {
        loop {
            let held = unsafe { *self.table.0.add(id_word(at)) };
            if held == id {
                return unsafe { *self.table.0.add(id_word(at) + 4) };
            }
            if held == 0 {
                return id;
            }
            at = (at + 1) & self.mask;
        }
    }
}

/// Where slot `at`'s ID is in the table; the person it belongs to (or, while the
/// table is built, its parent) is four words on. Slots 4k to 4k+3 share a cache line.
#[inline(always)]
fn id_word(at: usize) -> usize {
    (at >> 2) * 8 + (at & 3)
}

/// For the four IDs at `line`: which are `id`, and which slots are free, one bit a slot.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn line_masks(line: *const u64, id: Key) -> (u32, u32) {
    use std::arch::x86_64::*;
    let ids = _mm256_loadu_si256(line as *const __m256i);
    let bits = |v: __m256i| _mm256_movemask_pd(_mm256_castsi256_pd(v)) as u32;
    (bits(_mm256_cmpeq_epi64(ids, _mm256_set1_epi64x(id as i64))), bits(_mm256_cmpeq_epi64(ids, _mm256_setzero_si256())))
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
#[inline(always)]
unsafe fn line_masks(line: *const u64, id: Key) -> (u32, u32) {
    let (mut found, mut empty) = (0u32, 0u32);
    for slot in 0..4 {
        let held = *line.add(slot);
        found |= u32::from(held == id) << slot;
        empty |= u32::from(held == 0) << slot;
    }
    (found, empty)
}

#[inline(always)]
fn home(id: Key, shift: u32) -> usize {
    (sys::mix(id) >> shift) as usize & !3
}

struct LinkLines {
    edges: Vec<(Key, Key)>,
}

impl Lines for LinkLines {
    const COMMAS: usize = 1;

    #[inline(always)]
    unsafe fn line(&mut self, data: *const u8, delims: *const u32, quotes: u32) {
        let comma = *delims.add(1) as usize;
        let (a0, a1, w0) = reader::unquoted(data, (*delims).wrapping_add(1) as usize, comma);
        let (b0, b1, w1) = reader::unquoted(data, comma + 1, *delims.add(2) as usize);
        if quotes != 2 * (w0 + w1) || a0 == a1 || b0 == b1 {
            return; // malformed, or a side is empty
        }
        let a = key::key_of_padded(data.add(a0), a1 - a0);
        let b = key::key_of_padded(data.add(b0), b1 - b0);
        if a != b {
            self.edges.push((a, b)); // a self-link changes nothing
        }
    }
}

/// The union-find while it is being built: each slot an ID key and, in the word
/// that will hold its person, its parent slot + 1, or 0 for a root.
struct Forest<'a> {
    words: &'a [AtomicU64],
    mask: usize,
    shift: u32,
}

impl Forest<'_> {
    #[inline(always)]
    fn id(&self, at: usize) -> &AtomicU64 {
        unsafe { self.words.get_unchecked(id_word(at)) }
    }

    #[inline(always)]
    fn parent(&self, at: usize) -> &AtomicU64 {
        unsafe { self.words.get_unchecked(id_word(at) + 4) }
    }

    /// The slot of `id`, claiming an empty one if it is new.
    #[inline]
    fn slot(&self, id: Key) -> usize {
        let mut at = home(id, self.shift);
        loop {
            let seen = self.id(at).load(Ordering::Relaxed);
            if seen == id {
                return at;
            }
            if seen == 0 {
                match self.id(at).compare_exchange(0, id, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => return at,
                    Err(other) if other == id => return at,
                    Err(_) => {}
                }
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline]
    fn find(&self, mut at: usize) -> usize {
        loop {
            let parent = self.parent(at).load(Ordering::Acquire);
            if parent == 0 {
                return at;
            }
            let up = (parent - 1) as usize;
            let grand = self.parent(up).load(Ordering::Acquire);
            if grand == 0 {
                return up;
            }
            // Path halving. Losing the race only means someone else shortened it.
            let _ = self.parent(at).compare_exchange_weak(parent, grand, Ordering::AcqRel, Ordering::Relaxed);
            at = up;
        }
    }

    fn join(&self, a: usize, b: usize) {
        loop {
            let (a, b) = (self.find(a), self.find(b));
            if a == b {
                return;
            }
            let (ka, kb) = (self.id(a).load(Ordering::Relaxed), self.id(b).load(Ordering::Relaxed));
            // The root with the larger ID goes under the one with the smaller.
            let (small, large) = if key::compare(ka, kb).is_lt() { (a, b) } else { (b, a) };
            if self.parent(large).compare_exchange(0, small as u64 + 1, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return;
            }
        }
    }
}

pub fn build(inputs: &[Input], threads: usize) -> Links {
    let started = std::time::Instant::now();
    let since = |from: std::time::Instant| from.elapsed().as_secs_f64() * 1e3;
    // Parse.
    let total: u64 = inputs.iter().map(|i| i.len).sum();
    let target = reader::job_bytes().unwrap_or((total / (threads as u64 * 4)).clamp(1 << 16, 1 << 22));
    let jobs: Vec<Job> = reader::jobs(inputs, target);
    let parse_threads = if total < (1 << 20) { 1 } else { threads };
    let next = AtomicUsize::new(0);
    let edge_lists: Vec<Vec<(Key, Key)>> = sys::parallel(parse_threads, |_| {
        let mut buf = ReadBuf::new();
        let mut lines = LinkLines { edges: Vec::new() };
        while let Some(job) = jobs.get(next.fetch_add(1, Ordering::Relaxed)) {
            let file: &File = &inputs[job.file as usize].file;
            reader::walk(file, job.offset, job.length, &mut buf, &mut lines);
        }
        lines.edges
    });
    let edges: usize = edge_lists.iter().map(Vec::len).sum();

    // Every edge names at most two new IDs; keep the table under three quarters
    // full (in practice about a third, since most IDs are named by several lines).
    let capacity = (edges * 2 * 4 / 3).max(16).next_power_of_two();
    let table = sys::map::<u64>(capacity * 2);
    let (mask, shift) = (capacity - 1, 64 - capacity.trailing_zeros());
    let parsed = since(started);
    if edges == 0 {
        return Links { table: sys::Shared(table), mask, shift, ids: 0, timing: [parsed, 0.0, 0.0] };
    }
    let shared = sys::Shared(table);
    let join_threads = if edges < 20_000 { 1 } else { threads };
    /// The table is far larger than the cache and every step lands somewhere new,
    /// so each loop asks for the lines it will need this many steps ahead.
    const AHEAD: usize = 8;
    let joining = std::time::Instant::now();

    // Join. The table is all zeros: every slot empty, every node a root.
    sys::parallel(join_threads, |me| {
        let forest = Forest {
            words: unsafe { std::slice::from_raw_parts({ shared }.0 as *const AtomicU64, capacity * 2) },
            mask,
            shift,
        };
        for list in edge_lists.iter().skip(me).step_by(join_threads) {
            for (at, &(a, b)) in list.iter().enumerate() {
                if let Some(&(next_a, next_b)) = list.get(at + AHEAD) {
                    sys::prefetch(forest.id(home(next_a, shift)));
                    sys::prefetch(forest.id(home(next_b, shift)));
                }
                let (a, b) = (forest.slot(a), forest.slot(b));
                forest.join(a, b);
            }
        }
    });
    let joined = since(joining);
    let naming = std::time::Instant::now();

    // Point every ID straight at its root...
    let ranges = |me: usize| (capacity * me / join_threads, capacity * (me + 1) / join_threads);
    sys::parallel(join_threads, |me| {
        let forest = Forest {
            words: unsafe { std::slice::from_raw_parts({ shared }.0 as *const AtomicU64, capacity * 2) },
            mask,
            shift,
        };
        let (from, to) = ranges(me);
        for at in from..to {
            if at + AHEAD < to {
                let parent = forest.parent(at + AHEAD).load(Ordering::Relaxed);
                if parent != 0 {
                    sys::prefetch(forest.id((parent - 1) as usize));
                }
            }
            if forest.id(at).load(Ordering::Relaxed) != 0 {
                let root = forest.find(at);
                if root != at {
                    forest.parent(at).store(root as u64 + 1, Ordering::Release);
                }
            }
        }
    });
    // ...then replace the pointer with the root's ID, which is the person's name.
    // Each thread writes only the second word of its own slots and reads only
    // first words, which no longer change.
    let counts = sys::parallel(join_threads, |me| {
        let forest = Forest {
            words: unsafe { std::slice::from_raw_parts({ shared }.0 as *const AtomicU64, capacity * 2) },
            mask,
            shift,
        };
        let (from, to) = ranges(me);
        let mut ids = 0usize;
        for at in from..to {
            if at + AHEAD < to {
                let parent = forest.parent(at + AHEAD).load(Ordering::Relaxed);
                if parent != 0 {
                    sys::prefetch(forest.id((parent - 1) as usize));
                }
            }
            let id = forest.id(at).load(Ordering::Relaxed);
            if id != 0 {
                ids += 1;
                let parent = forest.parent(at).load(Ordering::Relaxed);
                let name = if parent == 0 { id } else { forest.id((parent - 1) as usize).load(Ordering::Relaxed) };
                forest.parent(at).store(name, Ordering::Relaxed);
            }
        }
        ids
    });
    Links { table: shared, mask, shift, ids: counts.iter().sum(), timing: [parsed, joined, since(naming)] }
}
