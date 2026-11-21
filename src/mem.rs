//! Big anonymous mappings. With transparent huge pages available (sysfs says `always` or `madvise`) a mapping
//! is advised for them and faults 2 MiB at a time; without them it is populated in bulk with
//! MADV_POPULATE_WRITE as it is first used, so a table costs one syscall per 2 MiB instead of 512 faults, and
//! no page is ever read before it is written (reading a fresh page maps the shared zero page, and the write
//! that follows then copies it and flushes the TLB of every thread).
use std::sync::OnceLock;

pub const HUGE: usize = 2 << 20;

unsafe extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

const MADV_DONTNEED: i32 = 4;
const MADV_HUGEPAGE: i32 = 14;
const MADV_POPULATE_WRITE: i32 = 23; // Linux 5.14+; EINVAL before, which is ignored

/// Whether the kernel will give MADV_HUGEPAGE mappings huge pages (SS_NOHUGE=1 pretends it will not).
pub fn thp() -> bool {
    static THP: OnceLock<bool> = OnceLock::new();
    *THP.get_or_init(|| {
        std::env::var_os("SS_NOHUGE").is_none()
            && std::fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled")
                .is_ok_and(|s| s.contains("[always]") || s.contains("[madvise]"))
    })
}

/// `len` bytes of fresh, 2 MiB-aligned memory, advised for huge pages where they exist.
pub fn map_huge(len: usize) -> *mut u8 {
    const PROT_READ: i32 = 1;
    const PROT_WRITE: i32 = 2;
    const MAP_PRIVATE: i32 = 2;
    const MAP_ANONYMOUS: i32 = 0x20;
    const MAP_NORESERVE: i32 = 0x4000;
    unsafe {
        let p = mmap(std::ptr::null_mut(), len + HUGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        assert!(p as isize != -1, "mmap failed");
        let aligned = p.add(p.align_offset(HUGE));
        if thp() {
            madvise(aligned, len, MADV_HUGEPAGE);
        }
        aligned
    }
}

/// Faults `len` bytes at `p` in, written, in one call, where huge pages are not available to do that
/// 2 MiB at a time anyway. Any page-aligned range of a mapping; a range already faulted is cheap.
pub fn populate(p: *mut u8, len: usize) {
    if thp() || len == 0 {
        return;
    }
    let start = p as usize & !4095;
    let end = (p as usize + len + 4095) & !4095;
    unsafe { madvise(start as *mut u8, end - start, MADV_POPULATE_WRITE) };
}

/// Gives the whole pages of a range back now, in the calling thread, instead of at exit on one core.
pub fn release(p: *mut u8, len: usize) {
    let start = (p as usize + 4095) & !4095;
    let end = (p as usize + len) & !4095;
    if end > start {
        unsafe { madvise(start as *mut u8, end - start, MADV_DONTNEED) };
    }
}

/// A zeroed Vec whose pages are written, not mapped to the zero page: for tables that are read before every
/// slot is written (`vec![0; n]` is a calloc that the allocator may leave to the kernel to zero).
pub fn zeroed<T: Copy + Default>(n: usize) -> Vec<T> {
    let mut v = Vec::with_capacity(n);
    v.resize(n, T::default());
    v
}
