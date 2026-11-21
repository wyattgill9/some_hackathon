//! Partitioned record streams for one thread. Each partition is a list of 16 KiB blocks carved from 32 MiB
//! huge-page arenas and filled through a staging buffer of 8 records: a push writes into the staging line
//! (hot in cache), and every eighth push streams the whole buffer into the block with non-temporal stores, so
//! the thousands of open blocks never cost a read-for-ownership or a cache line each. Nothing is ever freed.
use std::mem::size_of;

const BLOCK: usize = 16 << 10;
const REGION: usize = 32 << 20;
const HUGE: usize = 2 << 20;
pub const STAGE: usize = 8;

pub struct Pool<T: Copy> {
    cap: usize, // records per block, a multiple of STAGE
    stage: Vec<[T; STAGE]>,
    fill: Vec<u8>,
    cur: Vec<*mut T>,
    used: Vec<u32>,
    done: Vec<Vec<(*mut T, u32)>>,
    region: *mut u8,
    region_used: usize,
}

// The blocks are plain memory owned by the pool; readers only see shared slices after `finish`.
unsafe impl<T: Copy> Send for Pool<T> {}
unsafe impl<T: Copy> Sync for Pool<T> {}

impl<T: Copy> Pool<T> {
    pub fn new(parts: usize) -> Self {
        assert!(size_of::<T>() % 4 == 0 && (STAGE * size_of::<T>()) % 32 == 0);
        Pool {
            cap: (BLOCK / size_of::<T>()) & !(STAGE - 1),
            stage: vec![unsafe { std::mem::zeroed() }; parts],
            fill: vec![0; parts],
            cur: vec![std::ptr::null_mut(); parts],
            used: vec![0; parts],
            done: (0..parts).map(|_| Vec::new()).collect(),
            region: std::ptr::null_mut(),
            region_used: REGION,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, v: T) {
        let f = self.fill[p] as usize;
        self.stage[p][f] = v;
        if f + 1 == STAGE {
            self.flush(p);
        } else {
            self.fill[p] = f as u8 + 1;
        }
    }

    #[inline(never)]
    fn flush(&mut self, p: usize) {
        if self.cur[p].is_null() || self.used[p] as usize + STAGE > self.cap {
            self.new_block(p);
        }
        unsafe {
            let dst = self.cur[p].add(self.used[p] as usize) as *mut u8;
            stream(self.stage[p].as_ptr() as *const u8, dst, STAGE * size_of::<T>());
        }
        self.used[p] += STAGE as u32;
        self.fill[p] = 0;
    }

    #[cold]
    fn new_block(&mut self, p: usize) {
        if !self.cur[p].is_null() {
            self.done[p].push((self.cur[p], self.used[p]));
        }
        if self.region_used + BLOCK > REGION {
            self.region = map_huge(REGION);
            self.region_used = 0;
        }
        self.cur[p] = unsafe { self.region.add(self.region_used) } as *mut T;
        self.region_used += BLOCK;
        self.used[p] = 0;
    }

    /// Moves every partly filled staging buffer into its block. Call once, before reading.
    pub fn finish(&mut self) {
        for p in 0..self.fill.len() {
            let f = self.fill[p] as usize;
            if f == 0 {
                continue;
            }
            if self.cur[p].is_null() || self.used[p] as usize + f > self.cap {
                self.new_block(p);
            }
            unsafe { std::ptr::copy_nonoverlapping(self.stage[p].as_ptr(), self.cur[p].add(self.used[p] as usize), f) };
            self.used[p] += f as u32;
            self.fill[p] = 0;
        }
        #[cfg(target_feature = "avx2")]
        unsafe {
            std::arch::x86_64::_mm_sfence()
        };
    }

    pub fn len(&self, p: usize) -> usize {
        self.done[p].iter().map(|b| b.1 as usize).sum::<usize>() + self.used[p] as usize
    }

    /// The partition's records, block by block.
    pub fn blocks(&self, p: usize) -> impl Iterator<Item = &[T]> {
        let cur = (!self.cur[p].is_null()).then(|| (self.cur[p], self.used[p]));
        self.done[p].iter().copied().chain(cur).map(|(b, n)| unsafe { std::slice::from_raw_parts(b, n as usize) })
    }
}

/// `bytes` from `src` to the 32-byte-aligned `dst` with non-temporal stores (a plain copy without AVX2).
#[inline]
unsafe fn stream(src: *const u8, dst: *mut u8, bytes: usize) {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_stream_si256};
        let mut i = 0;
        while i < bytes {
            _mm256_stream_si256(dst.add(i) as *mut __m256i, _mm256_loadu_si256(src.add(i) as *const __m256i));
            i += 32;
        }
    }
    #[cfg(not(target_feature = "avx2"))]
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, bytes)
    };
}

unsafe extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

/// `len` bytes of fresh, 2 MiB-aligned memory advised for transparent huge pages.
pub fn map_huge(len: usize) -> *mut u8 {
    const PROT_READ: i32 = 1;
    const PROT_WRITE: i32 = 2;
    const MAP_PRIVATE: i32 = 2;
    const MAP_ANONYMOUS: i32 = 0x20;
    const MAP_NORESERVE: i32 = 0x4000;
    const MADV_HUGEPAGE: i32 = 14;
    unsafe {
        let p = mmap(std::ptr::null_mut(), len + HUGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        assert!(p as isize != -1, "mmap failed");
        let aligned = p.add(p.align_offset(HUGE));
        madvise(aligned, len, MADV_HUGEPAGE); // advisory: ignored where THP is off
        aligned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let mut pool: Pool<[u64; 3]> = Pool::new(3);
        for i in 0..5000u64 {
            pool.push((i % 3) as usize, [i, i * 2, i * 3]);
        }
        pool.finish();
        for p in 0..3 {
            let got: Vec<[u64; 3]> = pool.blocks(p).flatten().copied().collect();
            let want: Vec<[u64; 3]> = (0..5000u64).filter(|i| (i % 3) as usize == p).map(|i| [i, i * 2, i * 3]).collect();
            assert_eq!(got, want);
            assert_eq!(pool.len(p), want.len());
        }
    }
}
