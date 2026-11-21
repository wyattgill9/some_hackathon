//! A fixed-capacity, lock-free, insert-only hash map from string keys to dense
//! user ids, shared by all parsing threads.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::keys::fib;

pub const BLOCK: u32 = 1024;

#[repr(C, align(16))]
struct Slot {
    /// 0 empty, 1 being written, otherwise id + 2
    state: AtomicU32,
    _pad: u32,
    key: u64,
}

pub struct UserTable {
    slots: *mut Slot,
    n_slots: usize,
    shift: u32,
    names: *mut u64,
    max_ids: usize,
    next_block: AtomicU32,
    max_blocks: u32,
    pub overflow: AtomicBool,
}

unsafe impl Sync for UserTable {}
unsafe impl Send for UserTable {}

pub struct IdAlloc {
    pub next: u32,
    end: u32,
}

impl IdAlloc {
    pub fn new() -> Self {
        IdAlloc { next: 0, end: 0 }
    }
}

extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

/// Lazily zeroed anonymous memory (2 MiB aligned, huge pages requested). Never freed.
pub fn zeroed_bytes(bytes: usize) -> *mut u8 {
    const HUGE: usize = 1 << 21;
    let len = bytes.max(1).div_ceil(HUGE) * HUGE;
    unsafe {
        // PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE
        let p = mmap(std::ptr::null_mut(), len + HUGE, 3, 0x2 | 0x20 | 0x4000, -1, 0);
        assert!(p as isize != -1, "mmap failed");
        let aligned = ((p as usize + HUGE - 1) & !(HUGE - 1)) as *mut u8;
        if huge_pages() {
            madvise(aligned, len, 14); // MADV_HUGEPAGE
        }
        aligned
    }
}

fn huge_pages() -> bool {
    static H: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *H.get_or_init(|| std::env::var_os("NOHUGE").is_none())
}

fn zeroed<T>(n: usize) -> *mut T {
    zeroed_bytes(n * std::mem::size_of::<T>()) as *mut T
}

impl UserTable {
    pub fn new(n_slots: usize, threads: usize) -> Self {
        let n_slots = n_slots.next_power_of_two().max(1 << 17);
        let max_blocks = ((n_slots as u64 * 7 / 10) / BLOCK as u64) as u32;
        let max_ids = (max_blocks as usize + threads + 2) * BLOCK as usize;
        UserTable {
            slots: zeroed::<Slot>(n_slots),
            n_slots,
            shift: 64 - n_slots.trailing_zeros(),
            names: zeroed::<u64>(max_ids),
            max_ids,
            next_block: AtomicU32::new(0),
            max_blocks,
            overflow: AtomicBool::new(false),
        }
    }

    #[inline(always)]
    pub fn hash(&self, key: u64) -> usize {
        fib(key, 64 - self.shift)
    }

    #[inline(always)]
    pub fn prefetch(&self, h: usize) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.add(h) as *const i8, _MM_HINT_T0);
        }
    }

    #[cold]
    fn new_block(&self, a: &mut IdAlloc) {
        let b = self.next_block.fetch_add(1, Ordering::Relaxed);
        if b >= self.max_blocks {
            self.overflow.store(true, Ordering::Relaxed);
            // keep going with ids from the spare area so nothing writes out of bounds
            let spare = (self.max_blocks + 1) * BLOCK;
            a.next = spare;
            a.end = spare + BLOCK;
            return;
        }
        a.next = b * BLOCK;
        a.end = a.next + BLOCK;
    }

    /// The id for `key` (non-zero), inserting it if new.
    #[inline(always)]
    pub fn get_or_insert(&self, key: u64, h: usize, a: &mut IdAlloc) -> u32 {
        let mask = self.n_slots - 1;
        let mut i = h;
        loop {
            let slot = unsafe { &*self.slots.add(i) };
            let st = slot.state.load(Ordering::Acquire);
            if st >= 2 {
                if unsafe { std::ptr::read_volatile(&slot.key) } == key {
                    return st - 2;
                }
                i = (i + 1) & mask;
                continue;
            }
            if st == 0 {
                if self.overflow.load(Ordering::Relaxed) {
                    return 0;
                }
                if slot.state.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire).is_err() {
                    continue;
                }
                if a.next == a.end {
                    self.new_block(a);
                }
                let id = a.next;
                a.next += 1;
                unsafe {
                    let sp = self.slots.add(i);
                    std::ptr::write_volatile(&mut (*sp).key, key);
                    if (id as usize) < self.max_ids {
                        *self.names.add(id as usize) = key;
                    }
                }
                slot.state.store(id + 2, Ordering::Release);
                return id;
            }
            std::hint::spin_loop();
        }
    }

    /// Number of id slots in use (allocated blocks), an upper bound on ids.
    pub fn id_limit(&self) -> usize {
        (self.next_block.load(Ordering::Relaxed).min(self.max_blocks) as usize) * BLOCK as usize
    }

    /// names[id] = key for allocated ids, 0 for holes.
    pub fn names(&self) -> &[u64] {
        unsafe { std::slice::from_raw_parts(self.names, self.id_limit()) }
    }

    /// Overwrites names[id]; only between parsing passes.
    pub unsafe fn set_name(&self, id: u32, key: u64) {
        *self.names.add(id as usize) = key;
    }

    /// Makes `key` (already present) map to `id`; only between parsing passes.
    pub fn redirect(&self, key: u64, id: u32) {
        let mask = self.n_slots - 1;
        let mut i = self.hash(key);
        loop {
            let slot = unsafe { &*self.slots.add(i) };
            let st = slot.state.load(Ordering::Acquire);
            if st >= 2 && unsafe { std::ptr::read_volatile(&slot.key) } == key {
                slot.state.store(id + 2, Ordering::Release);
                return;
            }
            if st == 0 {
                return;
            }
            i = (i + 1) & mask;
        }
    }
}

