//! Partitioned record streams.
//!
//! While parsing, each thread appends records to one of P partitions. A
//! partition is a list of fixed-size chunks carved out of slabs; a record never
//! moves once it is in its chunk, and every record has a global 32-bit index,
//! so a later phase can point at one (the dedup phase does, to drop the losing
//! copies of an event).

use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::sys;

const SLAB_BITS: u32 = 18;
const SLAB_RECORDS: usize = 1 << SLAB_BITS;
const MAX_SLABS: usize = (1 << (32 - SLAB_BITS)) - 1;

/// Slabs of `T`, shared by all threads.
pub struct Pool<T> {
    slabs: Box<[AtomicPtr<T>]>,
    next: AtomicUsize,
}

impl<T> Pool<T> {
    pub fn new() -> Self {
        Self { slabs: (0..MAX_SLABS).map(|_| AtomicPtr::new(std::ptr::null_mut())).collect(), next: AtomicUsize::new(0) }
    }

    /// A fresh slab: its first record's global index, and where it is.
    fn grab(&self) -> (u32, *mut T) {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        if id >= MAX_SLABS {
            eprintln!("pipeline: more than 2^32 records in one stream");
            std::process::abort();
        }
        let slab = sys::map::<T>(SLAB_RECORDS);
        self.slabs[id].store(slab, Ordering::Release);
        ((id << SLAB_BITS) as u32, slab)
    }

    /// The record with global index `at`.
    #[inline(always)]
    pub fn at(&self, at: u32) -> *mut T {
        unsafe {
            let slab = self.slabs.get_unchecked((at >> SLAB_BITS) as usize).load(Ordering::Relaxed);
            slab.add(at as usize & (SLAB_RECORDS - 1))
        }
    }

    pub fn records(&self) -> usize {
        self.next.load(Ordering::Relaxed) * SLAB_RECORDS
    }
}

/// What a thread leaves behind: per partition, its chunks as (global index of the first record, records).
pub type Chunks = Vec<Vec<(u32, u32)>>;

/// Where a partition's buffered records will go.
struct Pending<T> {
    /// The buffer's place in the chunk; null before the partition's first record.
    line: *mut T,
    /// Global index of the next record pushed, and one past the chunk's last.
    index: u32,
    end_index: u32,
}

/// One thread's write side: for each partition, a write-combining buffer of `N` records.
///
/// A thread appends to a couple of thousand partitions in turn, each into
/// memory nothing has touched yet. Written directly, every new cache line of
/// every partition is first fetched from memory (only to be overwritten), and
/// with that many streams no prefetcher sees them coming. Here `N` records, a
/// whole number of cache lines, collect in a small buffer that stays in cache
/// and then go out in non-temporal stores, which fetch nothing.
pub struct Combine<'p, T, const N: usize> {
    pool: &'p Pool<T>,
    /// `N` buffered records per partition, each buffer starting on a cache line.
    buffers: *mut [T; N],
    states: Vec<Pending<T>>,
    chunks: Vec<Vec<(u32, u32)>>,
    chunk: u32,
    slab: *mut T,
    slab_index: u32,
    slab_left: u32,
}

/// Copies `lines` 64-byte lines (both ends 64-byte aligned) without reading the destination into cache first.
#[inline(always)]
unsafe fn stream_lines(from: *const u8, to: *mut u8, lines: usize) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    {
        use std::arch::x86_64::*;
        for half in 0..2 * lines {
            _mm256_stream_si256(to.add(32 * half) as *mut __m256i, _mm256_load_si256(from.add(32 * half) as *const __m256i));
        }
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx")))]
    {
        use std::arch::x86_64::*;
        for part in 0..4 * lines {
            _mm_stream_si128(to.add(16 * part) as *mut __m128i, _mm_load_si128(from.add(16 * part) as *const __m128i));
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    std::ptr::copy_nonoverlapping(from, to, 64 * lines);
}

impl<'p, T: Copy, const N: usize> Combine<'p, T, N> {
    /// Cache lines in a full buffer.
    const LINES: usize = N * std::mem::size_of::<T>() / 64;

    /// `chunk` records per chunk: a power of two, at least `N`, no larger than a slab.
    pub fn new(pool: &'p Pool<T>, partitions: usize, chunk: usize) -> Self {
        assert!(N.is_power_of_two() && (N * std::mem::size_of::<T>()) % 64 == 0 && std::mem::align_of::<T>() <= 64);
        assert!(chunk.is_power_of_two() && chunk >= N && chunk <= SLAB_RECORDS);
        Self {
            pool,
            buffers: sys::map::<[T; N]>(partitions.max(64)), // page-aligned, so every buffer starts on a cache line
            states: (0..partitions).map(|_| Pending { line: std::ptr::null_mut(), index: 0, end_index: 0 }).collect(),
            chunks: vec![Vec::new(); partitions],
            chunk: chunk as u32,
            slab: std::ptr::null_mut(),
            slab_index: 0,
            slab_left: 0,
        }
    }

    /// Asks the CPU for what the next `push` to `partition` will touch: its state
    /// and its buffer. A thread writes to its partitions in no order, and the
    /// states and buffers of a thousand partitions are far more than the first
    /// cache level holds, so a push whose partition was not announced a few lines
    /// earlier starts with a wait.
    #[inline(always)]
    pub fn prefetch(&self, partition: usize) {
        unsafe {
            sys::prefetch(self.states.as_ptr().add(partition));
            let buffer = self.buffers.add(partition) as *const u8;
            for line in 0..Self::LINES {
                sys::prefetch(buffer.add(64 * line));
            }
        }
    }

    /// Appends `record` to `partition` and returns its global index. The record
    /// is only in its final place once its buffer has filled, or after `finish`.
    #[inline(always)]
    pub fn push(&mut self, partition: usize, record: T) -> u32 {
        unsafe {
            if (*self.states.as_ptr().add(partition)).index == (*self.states.as_ptr().add(partition)).end_index {
                self.refill(partition);
            }
            let state = &mut *self.states.as_mut_ptr().add(partition);
            let index = state.index;
            let slot = index as usize & (N - 1);
            let buffer = self.buffers.add(partition) as *mut T;
            buffer.add(slot).write(record);
            state.index = index + 1;
            if slot == N - 1 {
                stream_lines(buffer as *const u8, state.line as *mut u8, Self::LINES);
                state.line = state.line.add(N);
            }
            index
        }
    }

    /// A new chunk for `partition`. Its last one is full, so its buffer is empty.
    #[cold]
    #[inline(never)]
    fn refill(&mut self, partition: usize) {
        if !self.states[partition].line.is_null() {
            self.chunks[partition].push((self.states[partition].end_index - self.chunk, self.chunk));
        }
        if self.slab_left == 0 {
            let (index, slab) = self.pool.grab();
            self.slab = slab;
            self.slab_index = index;
            self.slab_left = SLAB_RECORDS as u32;
        }
        let state = &mut self.states[partition];
        state.line = self.slab;
        state.index = self.slab_index;
        state.end_index = self.slab_index + self.chunk;
        self.slab = unsafe { self.slab.add(self.chunk as usize) };
        self.slab_index += self.chunk;
        self.slab_left -= self.chunk;
    }

    /// Puts the records still in buffers in their places and returns the chunks.
    pub fn finish(mut self) -> Chunks {
        for partition in 0..self.states.len() {
            let state = &self.states[partition];
            if state.line.is_null() {
                continue;
            }
            let buffered = state.index as usize & (N - 1);
            unsafe { std::ptr::copy_nonoverlapping(self.buffers.add(partition) as *const T, state.line, buffered) };
            let used = state.index - (state.end_index - self.chunk);
            if used > 0 {
                self.chunks[partition].push((state.end_index - self.chunk, used));
            }
        }
        // Non-temporal stores are only ordered with everything else once fenced.
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        self.chunks
    }
}
