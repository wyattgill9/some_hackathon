//! Delimiter masks of a window of bytes: bit i of each word is set where byte i is a comma, a quote, a newline.
//! The reader takes them at fixed places in a line (its first 64 bytes, then 32 or 64 more) with AVX2.

/// `[commas, quotes, newlines]` of the 64 bytes at `p`.
///
/// # Safety
/// `p[..64]` must be readable.
#[inline(always)]
pub unsafe fn masks64(p: *const u8) -> [u64; 3] {
    let [c0, q0, n0] = unsafe { masks32(p) };
    let [c1, q1, n1] = unsafe { masks32(p.add(32)) };
    [c0 | c1 << 32, q0 | q1 << 32, n0 | n1 << 32]
}

/// `[commas, quotes, newlines]` of the 32 bytes at `p`, in the low 32 bits.
///
/// # Safety
/// `p[..32]` must be readable.
#[inline(always)]
pub unsafe fn masks32(p: *const u8) -> [u64; 3] {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let v = _mm256_loadu_si256(p as *const __m256i);
        let m = |c: u8| _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, _mm256_set1_epi8(c as i8))) as u32 as u64;
        [m(b','), m(b'"'), m(b'\n')]
    }
    #[cfg(not(target_feature = "avx2"))]
    unsafe {
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for i in 0..32 {
            let b = *p.add(i);
            c |= ((b == b',') as u64) << i;
            q |= ((b == b'"') as u64) << i;
            n |= ((b == b'\n') as u64) << i;
        }
        [c, q, n]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks() {
        let data: Vec<u8> = b"a,b\n\"q\",,\nxx".repeat(8);
        let want = |c: u8, o: usize, w: usize| -> u64 { (0..w).filter(|&i| data[o + i] == c).map(|i| 1u64 << i).sum() };
        for o in 0..data.len() - 64 {
            let [c, q, n] = unsafe { masks64(data.as_ptr().add(o)) };
            assert_eq!([c, q, n], [want(b',', o, 64), want(b'"', o, 64), want(b'\n', o, 64)], "{o}");
            let [c, q, n] = unsafe { masks32(data.as_ptr().add(o)) };
            assert_eq!([c, q, n], [want(b',', o, 32), want(b'"', o, 32), want(b'\n', o, 32)], "{o}");
        }
    }
}
