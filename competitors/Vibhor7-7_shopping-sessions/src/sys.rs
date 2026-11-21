//! The few things std does not give us: large lazily-committed allocations and
//! huge-page advice. Declared by hand so the crate needs nothing from crates.io.

use std::ffi::c_void;

extern "C" {
    fn mmap(addr: *mut c_void, len: usize, prot: i32, flags: i32, fd: i32, offset: i64) -> *mut c_void;
    #[cfg(target_os = "linux")]
    fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
}

const PROT_READ_WRITE: i32 = 1 | 2;
const MAP_PRIVATE: i32 = 2;
#[cfg(target_os = "linux")]
const MAP_ANONYMOUS: i32 = 0x20;
#[cfg(not(target_os = "linux"))]
const MAP_ANONYMOUS: i32 = 0x1000;
#[cfg(target_os = "linux")]
const MAP_NORESERVE: i32 = 0x4000;
#[cfg(not(target_os = "linux"))]
const MAP_NORESERVE: i32 = 0;
#[cfg(target_os = "linux")]
const MADV_HUGEPAGE: i32 = 14;

const HUGE: usize = 2 << 20;

extern "C" {
    #[cfg(target_os = "linux")]
    fn umask(mask: u32) -> u32;
    #[cfg(not(target_os = "linux"))]
    fn umask(mask: u16) -> u16;
}

/// Lets everything this process creates be read, written and removed by anyone.
/// The process runs as uid 1000 and the scorer that collects and clears the
/// output is someone else; nothing it has to do should depend on a chmod.
pub fn open_permissions() {
    unsafe {
        umask(0);
    }
}

/// `count` zeroed `T`s, never freed. The memory is committed page by page as it
/// is first written, so reserving far more than is used costs nothing. Large
/// blocks are aligned to 2 MB and advised for transparent huge pages, which
/// cuts both the page faults while filling them and the teardown at exit.
pub fn map<T>(count: usize) -> *mut T {
    let bytes = (count.max(1) * std::mem::size_of::<T>() + 4095) & !4095;
    let big = bytes >= HUGE;
    let len = if big { bytes + HUGE } else { bytes };
    let raw = unsafe {
        mmap(std::ptr::null_mut(), len, PROT_READ_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0)
    };
    if raw as isize == -1 || raw.is_null() {
        eprintln!("pipeline: mmap of {len} bytes failed");
        std::process::abort();
    }
    if !big {
        return raw as *mut T;
    }
    let aligned = (raw as usize + HUGE - 1) & !(HUGE - 1);
    #[cfg(target_os = "linux")]
    unsafe {
        madvise(aligned as *mut c_void, bytes, MADV_HUGEPAGE);
    }
    aligned as *mut T
}

/// The global allocator: the system's for small blocks; anything from 32 KB up
/// is put on memory advised for huge pages. A block of 2 MB or more is its own
/// mapping, aligned to 2 MB. One in between is carved out of a shared 8 MB
/// mapping and never given back: there are few of them (a thread's read buffer,
/// its hash tables and cursors) and they live as long as the run does.
///
/// These buffers are written densely and at random. On 4 KB pages each megabyte
/// of them is 256 page faults going in, 256 entries competing for the TLB (and
/// on a virtual machine a missed entry is a walk through two page tables), and
/// 256 pages for the kernel to free, one thread, at exit. Where the machine
/// only gives huge pages to memory that asks, this asks. Where it gives them to
/// everything, or to nothing, it changes nothing.
#[cfg(target_os = "linux")]
pub struct HugeBlocks;

#[cfg(target_os = "linux")]
mod huge_blocks {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};

    extern "C" {
        fn munmap(addr: *mut c_void, len: usize) -> i32;
        fn mremap(addr: *mut c_void, old_len: usize, new_len: usize, flags: i32, ...) -> *mut c_void;
    }
    const MREMAP_MAYMOVE: i32 = 1;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const MEDIUM: usize = 32 << 10;
    const ARENA: usize = 8 << 20;
    /// The shared mapping medium blocks are carved from: where the next goes, and where it ends.
    static CARVING: AtomicBool = AtomicBool::new(false);
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    static END: AtomicUsize = AtomicUsize::new(0);

    #[inline]
    fn large(layout: Layout) -> bool {
        layout.size() >= HUGE && layout.align() <= 4096
    }

    #[inline]
    fn medium(layout: Layout) -> bool {
        layout.size() >= MEDIUM && layout.size() < HUGE && layout.align() <= 4096
    }

    /// A medium block: zeroed (the mapping is fresh and nothing in it is ever reused), or null.
    unsafe fn carve(layout: Layout) -> *mut u8 {
        while CARVING.swap(true, Ordering::Acquire) {
            std::hint::spin_loop();
        }
        let align = layout.align().max(64);
        let mut start = (NEXT.load(Ordering::Relaxed) + align - 1) & !(align - 1);
        if start + layout.size() > END.load(Ordering::Relaxed) {
            start = block(ARENA) as usize;
            if start == 0 {
                CARVING.store(false, Ordering::Release);
                return std::ptr::null_mut();
            }
            END.store(start + ARENA, Ordering::Relaxed);
        }
        NEXT.store(start + layout.size(), Ordering::Relaxed);
        CARVING.store(false, Ordering::Release);
        start as *mut u8
    }

    #[inline]
    fn pages(size: usize) -> usize {
        (size + 4095) & !4095
    }

    /// A fresh, zeroed mapping of `size` bytes starting on a 2 MB boundary, or null.
    unsafe fn block(size: usize) -> *mut u8 {
        let len = pages(size);
        let raw = mmap(std::ptr::null_mut(), len + HUGE, PROT_READ_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if raw as isize == -1 || raw.is_null() {
            return std::ptr::null_mut();
        }
        let raw = raw as usize;
        let start = (raw + HUGE - 1) & !(HUGE - 1);
        // Give back what the alignment left over on either side, so the block is exactly its own mapping.
        if start > raw {
            munmap(raw as *mut c_void, start - raw);
        }
        let (end, raw_end) = (start + len, raw + len + HUGE);
        if raw_end > end {
            munmap(end as *mut c_void, raw_end - end);
        }
        madvise(start as *mut c_void, len, MADV_HUGEPAGE);
        start as *mut u8
    }

    unsafe impl GlobalAlloc for HugeBlocks {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            if large(layout) {
                block(layout.size())
            } else if medium(layout) {
                carve(layout)
            } else {
                System.alloc(layout)
            }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            if large(layout) || medium(layout) {
                self.alloc(layout) // fresh from the kernel: zeros already
            } else {
                System.alloc_zeroed(layout)
            }
        }

        unsafe fn dealloc(&self, at: *mut u8, layout: Layout) {
            if large(layout) {
                munmap(at as *mut c_void, pages(layout.size()));
            } else if !medium(layout) {
                System.dealloc(at, layout)
            }
        }

        unsafe fn realloc(&self, at: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let new_layout = Layout::from_size_align_unchecked(new_size, layout.align());
            if !large(layout) && !medium(layout) && !large(new_layout) && !medium(new_layout) {
                return System.realloc(at, layout, new_size);
            }
            if large(layout) && large(new_layout) {
                // The kernel moves the pages; nothing is copied.
                let moved = mremap(at as *mut c_void, pages(layout.size()), pages(new_size), MREMAP_MAYMOVE);
                return if moved as isize == -1 { std::ptr::null_mut() } else { moved as *mut u8 };
            }
            if medium(layout) && medium(new_layout) && new_size <= layout.size() {
                return at;
            }
            let new = self.alloc(new_layout);
            if !new.is_null() {
                std::ptr::copy_nonoverlapping(at, new, layout.size().min(new_size));
                self.dealloc(at, layout);
            }
            new
        }
    }
}

/// The most memory this process has had resident at once, in megabytes.
pub fn peak_memory_mb() -> u64 {
    extern "C" {
        fn getrusage(who: i32, usage: *mut [i64; 18]) -> i32;
    }
    // struct rusage: two timevals (four words), then ru_maxrss.
    let mut usage = [0i64; 18];
    unsafe { getrusage(0, &mut usage) };
    let unit = if cfg!(target_os = "linux") { 1 << 10 } else { 1 }; // kilobytes on Linux, bytes on macOS
    (usage[4] as u64 * unit) >> 20
}

/// A raw pointer that may cross threads. Every use is a phase in which each
/// thread touches its own records, with a join between phases.
#[derive(Clone, Copy)]
pub struct Shared<T>(pub *mut T);
unsafe impl<T> Send for Shared<T> {}
unsafe impl<T> Sync for Shared<T> {}

/// Runs `work(i)` on `count` threads and returns the results in thread order.
pub fn parallel<R: Send>(count: usize, work: impl Fn(usize) -> R + Sync) -> Vec<R> {
    if count <= 1 {
        return vec![work(0)];
    }
    std::thread::scope(|scope| {
        let work = &work;
        let handles: Vec<_> = (0..count).map(|i| scope.spawn(move || work(i))).collect();
        handles.into_iter().map(|h| h.join().expect("worker thread")).collect()
    })
}

/// Asks the CPU to start fetching the cache line at `at`. Never faults.
#[inline(always)]
pub fn prefetch<T>(at: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(at as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{at}]", at = in(reg) at, options(nostack, preserves_flags, readonly));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = at;
}

/// One multiply, all 64 bits mixed: the two halves of a 128-bit product folded together.
#[inline(always)]
pub fn mix(key: u64) -> u64 {
    let product = u128::from(key ^ 0x2d35_8dcc_aa6c_78a5) * 0x8bb8_4b93_962e_acc9_u128;
    (product as u64) ^ ((product >> 64) as u64)
}
