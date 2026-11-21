//! Global allocator: large blocks come straight from mmap with transparent huge
//! pages requested (and grow with mremap); small ones go to the system malloc.

use std::alloc::{GlobalAlloc, Layout, System};

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn mremap(old: *mut u8, old_len: usize, new_len: usize, flags: i32, ...) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

const LARGE: usize = 1 << 21;
const PAGE: usize = 4096;

pub struct HugeAlloc;

#[inline]
fn round(n: usize) -> usize {
    (n + PAGE - 1) & !(PAGE - 1)
}

unsafe fn map(len: usize) -> *mut u8 {
    // PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE
    let p = mmap(std::ptr::null_mut(), len, 3, 0x2 | 0x20 | 0x4000, -1, 0);
    if p as isize == -1 {
        return std::ptr::null_mut();
    }
    madvise(p, len, 14); // MADV_HUGEPAGE
    p
}

unsafe impl GlobalAlloc for HugeAlloc {
    #[inline]
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() >= LARGE && l.align() <= PAGE {
            map(round(l.size()))
        } else {
            System.alloc(l)
        }
    }
    #[inline]
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() >= LARGE && l.align() <= PAGE {
            map(round(l.size()))
        } else {
            System.alloc_zeroed(l)
        }
    }
    #[inline]
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if l.size() >= LARGE && l.align() <= PAGE {
            munmap(p, round(l.size()));
        } else {
            System.dealloc(p, l)
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let old_large = l.size() >= LARGE && l.align() <= PAGE;
        let new_large = new_size >= LARGE && l.align() <= PAGE;
        if old_large && new_large {
            let q = mremap(p, round(l.size()), round(new_size), 1); // MREMAP_MAYMOVE
            if q as isize == -1 {
                return std::ptr::null_mut();
            }
            if new_size > l.size() {
                madvise(q, round(new_size), 14);
            }
            return q;
        }
        if !old_large && !new_large {
            return System.realloc(p, l, new_size);
        }
        let nl = Layout::from_size_align_unchecked(new_size, l.align());
        let q = self.alloc(nl);
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size));
            self.dealloc(p, l);
        }
        q
    }
}
