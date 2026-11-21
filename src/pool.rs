//! Partitioned record streams for one thread. Each partition is a list of blocks carved from 32 MiB huge-page
//! arenas (or from blocks another pool has finished with) and filled through a staging buffer of `S` bytes, one
//! or a few cache lines: a push writes into the staging buffer (hot in cache), and the push that completes it
//! streams whole lines into the block with non-temporal stores, so the thousands of open blocks never cost a
//! read-for-ownership, and the staging of all the partitions is `S` bytes each. Nothing is ever freed.
use std::mem::size_of;

pub const BLOCK: usize = 16 << 10;
const REGION: usize = 32 << 20;
const HUGE: usize = 2 << 20;
const LINE: usize = 64;

#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Stage<const S: usize>([u8; S]);

pub struct Pool<T: Copy, const S: usize = 64> {
    cap: usize, // records per block, a multiple of the records per stage
    stage: Vec<Stage<S>>,
    fill: Vec<u8>, // records staged, per partition: the one array every push touches is 1 byte a partition
    cur: Vec<*mut T>, // the open block, null before the first flush
    used: Vec<u32>, // records in the open block
    done: Vec<Vec<(*mut T, u32)>>,
    region: *mut u8,
    region_used: usize,
    region_len: usize,
    spare: Vec<(*mut u8, usize)>,
    regions: Vec<*mut u8>,
}

// The blocks are plain memory owned by the pool; readers only see shared slices after `finish`.
unsafe impl<T: Copy, const S: usize> Send for Pool<T, S> {}
unsafe impl<T: Copy, const S: usize> Sync for Pool<T, S> {}

impl<T: Copy, const S: usize> Pool<T, S> {
    /// Records per staging buffer.
    const PER: usize = S / size_of::<T>();

    pub fn new(parts: usize) -> Self {
        const { assert!(S % LINE == 0 && S % size_of::<T>() == 0, "the stage must be whole lines of whole records") };
        Pool {
            cap: (BLOCK / size_of::<T>()) / Self::PER * Self::PER,
            stage: vec![Stage([0; S]); parts],
            fill: vec![0; parts],
            cur: vec![std::ptr::null_mut(); parts],
            used: vec![0; parts],
            done: (0..parts).map(|_| Vec::new()).collect(),
            region: std::ptr::null_mut(),
            region_used: 0,
            region_len: 0,
            spare: Vec::new(),
            regions: Vec::new(),
        }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, v: T) {
        // In bounds: p comes from the partition bits, and fill stays below PER.
        unsafe {
            let f = *self.fill.get_unchecked(p) as usize;
            if f + 1 == Self::PER {
                self.flush(p, &v);
            } else {
                *self.fill.get_unchecked_mut(p) = f as u8 + 1;
                *(self.stage.get_unchecked_mut(p).0.as_mut_ptr().add(f * size_of::<T>()) as *mut T) = v;
            }
        }
    }

    /// Streams the staging line, completed by `last`, into the block. The last record never goes through the
    /// stage: a load of the line right after a narrower store into it would wait for the store to retire.
    #[inline(always)]
    fn flush(&mut self, p: usize, last: &T) {
        unsafe {
            let used = *self.used.get_unchecked(p) as usize;
            let cur = *self.cur.get_unchecked(p);
            if cur.is_null() || used + Self::PER > self.cap {
                self.new_block(p);
                return self.flush(p, last);
            }
            let dst = cur.add(used) as *mut u8;
            *self.used.get_unchecked_mut(p) = (used + Self::PER) as u32;
            *self.fill.get_unchecked_mut(p) = 0;
            stream::<T, S>(self.stage.get_unchecked(p).0.as_ptr(), last as *const T as *const u8, dst);
        }
    }

    #[cold]
    #[inline(never)]
    fn new_block(&mut self, p: usize) {
        if !self.cur[p].is_null() {
            self.done[p].push((self.cur[p], self.used[p]));
        }
        if self.region_used + BLOCK > self.region_len {
            match self.spare.pop() {
                Some((ptr, len)) => (self.region, self.region_len) = (ptr, len),
                None => {
                    self.region = map_huge(REGION);
                    self.region_len = REGION;
                    self.regions.push(self.region);
                }
            }
            self.region_used = 0;
        }
        self.cur[p] = unsafe { self.region.add(self.region_used) } as *mut T;
        self.used[p] = 0;
        self.region_used += BLOCK;
    }

    /// Hands the pool a block of another pool, which nothing reads any more, to carve a later block from.
    pub fn recycle(&mut self, block: *mut u8) {
        debug_assert!(block as usize % BLOCK == 0);
        self.spare.push((block, BLOCK));
    }

    /// Moves every partly filled staging line into its block. Call once, before reading.
    pub fn finish(&mut self) {
        for p in 0..self.fill.len() {
            let f = self.fill[p] as usize;
            if f == 0 {
                continue;
            }
            if self.cur[p].is_null() || self.used[p] as usize + f > self.cap {
                self.new_block(p);
            }
            unsafe { std::ptr::copy_nonoverlapping(self.stage[p].0.as_ptr() as *const T, self.cur[p].add(self.used[p] as usize), f) };
            self.used[p] += f as u32;
            self.fill[p] = 0;
        }
        #[cfg(target_feature = "avx2")]
        unsafe {
            std::arch::x86_64::_mm_sfence()
        };
    }

    /// Gives the pool's own arenas back now, in the calling thread, instead of at exit on one core.
    pub fn release(&self) {
        for &r in &self.regions {
            unsafe { madvise(r, REGION, 4) }; // MADV_DONTNEED
        }
    }

    pub fn len(&self, p: usize) -> usize {
        self.done[p].iter().map(|b| b.1 as usize).sum::<usize>() + self.used[p] as usize
    }

    /// The partition's records, block by block.
    pub fn blocks(&self, p: usize) -> impl Iterator<Item = &[T]> {
        let cur = (!self.cur[p].is_null()).then_some((self.cur[p], self.used[p]));
        self.done[p].iter().copied().chain(cur).map(|(b, n)| unsafe { std::slice::from_raw_parts(b, n as usize) })
    }
}

/// `S` bytes to the 64-byte-aligned `dst` with non-temporal stores (a plain copy without AVX2): the first
/// S - size_of::<T>() bytes from the 64-byte-aligned `stage`, the rest the record at `last`.
#[inline(always)]
unsafe fn stream<T, const S: usize>(stage: *const u8, last: *const u8, dst: *mut u8) {
    const { assert!(matches!(size_of::<T>(), 8 | 16 | 24 | 32), "unsupported record size") };
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let chunk = |i: usize| _mm256_load_si256(stage.add(32 * i) as *const __m256i);
        // The last 32 bytes: the stage's tail, then the record.
        // The record is read in 8-byte pieces: it was just written, usually field by field, and a wider load
        // would wait for those stores to retire instead of being forwarded.
        let q = |i: usize| (last as *const i64).add(i).read_unaligned();
        let tail = match size_of::<T>() {
            32 => _mm256_set_epi64x(q(3), q(2), q(1), q(0)),
            24 => _mm256_blend_epi32::<0b1111_1100>(chunk(S / 32 - 1), _mm256_set_epi64x(q(2), q(1), q(0), 0)),
            16 => _mm256_inserti128_si256::<1>(_mm256_castsi128_si256(_mm_load_si128(stage.add(S - 32) as *const __m128i)), _mm_set_epi64x(q(1), q(0))),
            _ => _mm256_blend_epi32::<0b1100_0000>(chunk(S / 32 - 1), _mm256_set1_epi64x(q(0))),
        };
        let mut i = 0;
        while i + 2 < S / 32 {
            nt64(dst.add(32 * i), chunk(i), chunk(i + 1));
            i += 2;
        }
        nt64(dst.add(S - 64), chunk(S / 32 - 2), tail);
    }
    #[cfg(not(target_feature = "avx2"))]
    unsafe {
        std::ptr::copy_nonoverlapping(stage, dst, S - size_of::<T>());
        std::ptr::copy_nonoverlapping(last, dst.add(S - size_of::<T>()), size_of::<T>());
    };
}

/// One line of non-temporal stores to the 64-byte-aligned `dst`.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn nt64(dst: *mut u8, lo: std::arch::x86_64::__m256i, hi: std::arch::x86_64::__m256i) {
    use std::arch::x86_64::*;
    unsafe {
        #[cfg(target_feature = "avx512f")]
        {
            _mm512_stream_si512(dst as *mut __m512i, _mm512_inserti64x4::<1>(_mm512_castsi256_si512(lo), hi));
        }
        #[cfg(not(target_feature = "avx512f"))]
        {
            _mm256_stream_si256(dst as *mut __m256i, lo);
            _mm256_stream_si256(dst.add(32) as *mut __m256i, hi);
        }
    }
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
        if std::env::var_os("SS_NOHUGE").is_none() {
            madvise(aligned, len, MADV_HUGEPAGE); // advisory: ignored where THP is off (SS_NOHUGE emulates that)
        }
        aligned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<const N: usize, const S: usize>() {
        let mut pool: Pool<[u64; N], S> = Pool::new(3);
        let rec = |i: u64| std::array::from_fn(|j| i * (j as u64 + 1));
        for i in 0..5000u64 {
            pool.push((i % 3) as usize, rec(i));
        }
        pool.finish();
        for p in 0..3 {
            let got: Vec<[u64; N]> = pool.blocks(p).flatten().copied().collect();
            let want: Vec<[u64; N]> = (0..5000u64).filter(|i| (i % 3) as usize == p).map(rec).collect();
            assert_eq!(got, want);
            assert_eq!(pool.len(p), want.len());
        }
    }

    #[test]
    fn round_trips() {
        round_trip::<1, 64>();
        round_trip::<2, 64>();
        round_trip::<2, 128>();
        round_trip::<3, 192>();
        round_trip::<4, 64>();
        round_trip::<4, 256>();
    }

    #[test]
    fn recycled_blocks() {
        let mut a: Pool<[u64; 2]> = Pool::new(1);
        for i in 0..40000u64 {
            a.push(0, [i, i]);
        }
        a.finish();
        let blocks: Vec<*mut u8> = a.blocks(0).map(|b| b.as_ptr() as *mut u8).collect();
        let mut b: Pool<[u64; 2]> = Pool::new(2);
        for &p in &blocks {
            b.recycle(p);
        }
        for i in 0..50000u64 {
            b.push((i & 1) as usize, [i, i * 3]);
        }
        b.finish();
        for p in 0..2 {
            let got: Vec<[u64; 2]> = b.blocks(p).flatten().copied().collect();
            let want: Vec<[u64; 2]> = (0..50000u64).filter(|i| (i & 1) as usize == p).map(|i| [i, i * 3]).collect();
            assert_eq!(got, want);
        }
        let reused = b.blocks(0).chain(b.blocks(1)).filter(|s| blocks.contains(&(s.as_ptr() as *mut u8))).count();
        assert!(reused > 0);
    }
}
