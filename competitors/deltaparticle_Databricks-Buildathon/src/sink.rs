//! Partitioned output buffers ("sinks") for the hash-partitioning passes.
//!
//! Every partition writes into 16 KB blocks carved from one big region
//! reserved up front (on Linux: anonymous memory advised to use huge pages,
//! so page faults and page zeroing are ~500x rarer than with growing Vecs).
//! Each partition stages records in a 128-byte buffer that is copied out a
//! whole cache line pair at a time with non-temporal stores, which write
//! memory without first reading it and without evicting useful cache lines.

use std::sync::atomic::{AtomicUsize, Ordering};

pub const BLOCK: usize = 16 << 10;
const STAGE_BYTES: usize = 128;

/// A region handed out in BLOCK-sized pieces by an atomic bump pointer.
pub struct Region {
    #[cfg(unix)]
    _map: memmap2::MmapMut,
    #[cfg(not(unix))]
    _buf: Vec<u8>,
    base: *mut u8,
    size: usize,
    next: AtomicUsize,
}

unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    pub fn new(bytes: usize) -> Self {
        let size = bytes.div_ceil(BLOCK) * BLOCK + BLOCK;
        #[cfg(unix)]
        {
            let mut map = memmap2::MmapMut::map_anon(size).expect("reserve memory");
            #[cfg(target_os = "linux")]
            let _ = map.advise(memmap2::Advice::HugePage);
            let raw = map.as_mut_ptr();
            // Align the first block to BLOCK (mmap gives page alignment).
            let off = raw.align_offset(BLOCK).min(BLOCK);
            Region { base: unsafe { raw.add(off) }, size: size - off, next: AtomicUsize::new(0), _map: map }
        }
        #[cfg(not(unix))]
        {
            let mut buf: Vec<u8> = Vec::with_capacity(size + BLOCK);
            let raw = buf.as_mut_ptr();
            let off = raw.align_offset(BLOCK);
            Region { base: unsafe { raw.add(off) }, size, next: AtomicUsize::new(0), _buf: buf }
        }
    }

    #[inline]
    fn block(&self) -> *mut u8 {
        let off = self.next.fetch_add(BLOCK, Ordering::Relaxed);
        if off + BLOCK <= self.size {
            return unsafe { self.base.add(off) };
        }
        // Region exhausted (far more rows than estimated): fall back to the
        // heap. Leaked on purpose; the process exits soon after.
        let layout = std::alloc::Layout::from_size_align(BLOCK, BLOCK).unwrap();
        let p = unsafe { std::alloc::alloc(layout) };
        assert!(!p.is_null(), "out of memory");
        p
    }
}

/// Copy one 128-byte staging buffer to `dst` (32-byte aligned).
#[inline(always)]
unsafe fn stream128(dst: *mut u8, src: *const u8) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    {
        use std::arch::x86_64::*;
        for i in 0..4 {
            let v = _mm256_loadu_si256(src.add(32 * i) as *const __m256i);
            _mm256_stream_si256(dst.add(32 * i) as *mut __m256i, v);
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx")))]
    std::ptr::copy_nonoverlapping(src, dst, STAGE_BYTES);
}

#[inline(always)]
fn fence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence();
    }
}

/// One thread's partitioned output for records of type T.
/// `size_of::<T>()` must divide 128 or be a multiple of 8 dividing 384.
pub struct Sink<'r, T: Copy> {
    region: &'r Region,
    /// Records per staging buffer, and per block.
    k: usize,
    cap: usize,
    stage: Vec<T>,
    fill: Vec<u8>,
    cur: Vec<*mut T>,
    left: Vec<usize>,
    base: Vec<*mut T>,
    full: Vec<Vec<*mut T>>,
}

unsafe impl<'r, T: Copy + Send> Send for Sink<'r, T> {}
unsafe impl<'r, T: Copy + Sync> Sync for Sink<'r, T> {}

impl<'r, T: Copy + Default> Sink<'r, T> {
    pub fn new(region: &'r Region, parts: usize) -> Self {
        let size = std::mem::size_of::<T>();
        // Staging holds a whole number of records filling a multiple of 32 bytes.
        let k = if STAGE_BYTES % size == 0 { STAGE_BYTES / size } else { 96 / size };
        assert!(k > 0 && (k * size) % 32 == 0, "unsupported record size");
        let cap = (BLOCK / (k * size)) * k;
        Self {
            region,
            k,
            cap,
            stage: vec![T::default(); parts * k],
            fill: vec![0; parts],
            cur: vec![std::ptr::null_mut(); parts],
            left: vec![0; parts],
            base: vec![std::ptr::null_mut(); parts],
            full: (0..parts).map(|_| Vec::new()).collect(),
        }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, x: T) {
        let f = self.fill[p] as usize;
        unsafe { *self.stage.get_unchecked_mut(p * self.k + f) = x };
        if f + 1 < self.k {
            self.fill[p] = (f + 1) as u8;
            return;
        }
        self.fill[p] = 0;
        self.spill(p);
    }

    #[inline(never)]
    fn spill(&mut self, p: usize) {
        if self.left[p] < self.k {
            self.next_block(p);
        }
        let k = self.k;
        unsafe {
            let src = self.stage.as_ptr().add(p * k) as *const u8;
            let dst = self.cur[p] as *mut u8;
            let bytes = k * std::mem::size_of::<T>();
            if bytes == STAGE_BYTES {
                stream128(dst, src);
            } else {
                std::ptr::copy_nonoverlapping(src, dst, bytes);
            }
            self.cur[p] = self.cur[p].add(k);
        }
        self.left[p] -= k;
    }

    fn next_block(&mut self, p: usize) {
        if !self.base[p].is_null() {
            let b = self.base[p];
            self.full[p].push(b);
        }
        let b = self.region.block() as *mut T;
        self.base[p] = b;
        self.cur[p] = b;
        self.left[p] = self.cap;
    }

    /// Write out the staged records (normal stores) and order all stores
    /// before other threads read the blocks.
    pub fn finish(&mut self) {
        for p in 0..self.fill.len() {
            let f = self.fill[p] as usize;
            for i in 0..f {
                if self.left[p] == 0 {
                    self.next_block(p);
                }
                unsafe {
                    *self.cur[p] = self.stage[p * self.k + i];
                    self.cur[p] = self.cur[p].add(1);
                }
                self.left[p] -= 1;
            }
            self.fill[p] = 0;
        }
        fence();
    }

    /// Records of partition p (after `finish`).
    pub fn len(&self, p: usize) -> usize {
        let partial = if self.base[p].is_null() { 0 } else { self.cap - self.left[p] };
        self.full[p].len() * self.cap + partial
    }

    /// The records of partition p as slices, block by block (after `finish`).
    pub fn slices(&self, p: usize) -> impl Iterator<Item = &[T]> + '_ {
        let cap = self.cap;
        let full = self.full[p].iter().map(move |&b| unsafe { std::slice::from_raw_parts(b as *const T, cap) });
        let partial = if self.base[p].is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.base[p] as *const T, cap - self.left[p]) })
        };
        full.chain(partial)
    }

    /// Record j of partition p (after `finish`).
    #[inline]
    pub fn get(&self, p: usize, j: usize) -> &T {
        let (b, o) = (j / self.cap, j % self.cap);
        let base = if b < self.full[p].len() { self.full[p][b] } else { self.base[p] };
        unsafe { &*base.add(o) }
    }

    /// Bytes of region a sink may need beyond its records: one partial block
    /// per partition.
    pub fn slack(parts: usize) -> usize {
        parts * BLOCK
    }
}
