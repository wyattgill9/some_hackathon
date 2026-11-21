# Ragnar Groot Koerkamp's 1BRC in Rust: what it does and what we can take

Snapshot of https://github.com/RagnarGrootKoerkamp/1brc at `master` (commit in `SOURCE.txt`, last change Feb 2025),
without `.git`. The full worklog, with timings and `perf stat` counters for every step, is the blog post at
https://curiouscoding.nl/posts/1brc/ (Jan 2024). Step numbers below refer to that post's headings in order.

Reference only. Read these before you touch it:
- **No license file.** The repo has none, and GitHub reports none, so copying code verbatim into our public entry is not
  clearly allowed. Take the ideas and write our own code.
- **It doesn't build with our toolchain.** It needs nightly (`portable_simd`, `slice_as_chunks`,
  `type_alias_impl_trait`) and a git dependency (`ptr_hash`). Nothing here is wired into our `Dockerfile`, which copies
  only `Cargo.toml`, `Cargo.lock` and `src`.
- **It is not 1BRC-compliant.** The `origin/compliant` branch (Jan 2024, not copied) adds a hash-collision check.

## Why this one

Rust 1BRC repos report times on different hardware, so compare cost per core:

| Repo | Reported time | Machine | Core-seconds |
|---|---|---|---|
| RagnarGrootKoerkamp/1brc | 0.90 s, 6 threads (4.93 s on 1 thread) | i7-10750H, 6 cores @ 4.6 GHz, HT off | ~5.4 |
| ymelois/1brc | 583.4 ms | Ryzen 9 5950X, 16 cores | ~9.3 |
| arthurlm/one-brc-rs | 705.8 ms (in-process, warm cache) | Ryzen 9 7950X, 16 cores, WSL2 | ~11.3 |

These are self-reported, so treat the ranking as rough. The 0.90 s comes from the blog's final table. It is measured inside the program and leaves out the `munmap` at exit.
The repo readme's own figures are 1.01 s wall on 12 threads and 5.72 s single-threaded. For comparison with other
languages, he points to the C++ entry https://github.com/lehuyduc/1brc-simd.

## The program (`src/main.rs`, 536 lines)

1. `mmap` the file and skip forward to a 32-byte-aligned offset.
2. Scan the first 4 MB to collect every city name. Each city goes in twice: `name` and `name;`, the second one used for
   negative values (step 23).
3. Build a PtrHash minimal perfect hash over those keys, with `keys * 5/2` slots of 32-byte `Record`s.
4. Split the file into `nthreads` equal chunks. In each thread, `iter_lines` runs **four independent cursors** at 0,
   1/4, 2/4 and 3/4 of the chunk, and the `step!` macro moves all four forward by one line per iteration.
5. Per line: one unaligned 32-byte load, `simd_eq` against `;` or `\n`, `to_bitmask`, `trailing_zeros`. That's the
   whole separator search, with no inner loop (step 17). The search for `;` keeps a fallback loop for names longer than
   32 bytes, wrapped in an `if` that is never taken, so it costs nothing (step 32).
6. Key: the first 8 and last 8 bytes of the name read as two `u64`s, shifted to drop the out-of-range bytes, then added
   (`hash_name`). **It is lossy.** Two names that share their first and last 8 bytes collide.
7. Value: one big-endian `u32` load, shifted to the field length. `pdep` then deposits the three digits into three
   21-bit fields of a `u64`, so summing values needs no multiplies (steps 28-29).
8. `Record::add`: count, sum, and *branchy* min/max on the raw bytes. On random data a city's min or max changes about
   ln(n) ≈ 15 times, so these branches almost never mispredict (step 30).
9. Each thread locks a global `Mutex<Phf>` and merges its table into it.

It assumes lines of at most 33 bytes, names unique by their first and last 8 bytes, and a city set fully present in
the first 4 MB. It does no validation. It also isn't exact: thread chunks aren't cut at line boundaries, the last 32
bytes of each chunk are skipped, and cursors 0-2 run as many lines as cursor 3 instead of stopping at the next quarter.
Our rules break every one of those assumptions, and for us any wrong row scores nothing.

## What moved the needle, with his numbers

| Step | Change | Before → after (single thread unless noted) |
|---|---|---|
| 7 | `memchr` crate instead of a byte loop | 42 s → 29 s |
| 10 | Key from two unaligned `u64` loads, no `memcpy` | 29 s → 23 s; branches and branch misses -30% |
| 11 | Perfect hash instead of hashbrown probing | 23 s → 17 s. Probing had been ~35% of runtime |
| 14 | `mmap` instead of `read` | 14 s → 12 s |
| 15 | 6 threads, per-thread tables | 12 s → 2.0 s |
| 16 | Branchless number parse | 2.0 s → 1.7 s; branch misses 440M → 140M |
| 17 | One 32-byte load per separator, no loops | 1.70 s → 1.67 s; branch misses 150M → 4M |
| 33 | 4 interleaved cursors per thread | 1.34 s → 1.23 s; IPC 2.20 → 2.66 |
| 35-37 | Reorder the 4 cursors stage by stage (all keys, then all indices, then all updates) | 1.23 s → 1.05 s; IPC 2.66 → 2.95 |
| 34 | (not fixed) `munmap` of the 13 GB mapping at exit | 180 ms on the main thread |

He measured every step with `perf stat` (instructions, IPC, branches, branch misses). Some changes looked like
no-ops on time but moved the counters: step 17 went 1.70 s → 1.67 s while branch misses fell from 150M to 4M.

## Mapping to our pipeline

The line references are to `src/main.rs` and `src/parse.rs` at the time of writing. The mapping comes from reading
the code, **not from a profile**: I haven't measured our phases for this write-up. Run `perf stat` on `~/data/ss60m`
before acting on any of it.

Our input isn't 1BRC input. On `~/data/ss20m`:
- The largest events file averages 77 bytes per line (4,151,634 lines), more than double his 33-byte limit.
- Quotes are the common case, not a rare one: 181,530 of 230,205 lines in `events/part-00000.csv` contain a `"`.
- Timestamp formats (F1, F2, F3, junk) are mixed line by line.
- The IDs we hash are short. In the samples, `event_id`, `user_id` and `click_id` are 8-9 bytes.

**1. Scan each byte once.** Today a byte is read up to four times:
- `lines()` (main.rs:499) splits on `\n` through `slice::split` with a closure, not `memchr`.
- `parse::fields` (parse.rs:4) splits each line on `,`.
- `unquote` (parse.rs:20) runs `contains(b'"')` over every field.
- `digits` / `d2` validate the digits.

His steps 7, 12 and 17 replace this with compare-to-bitmask. Our version: per 64-byte block, compare against `\n`, `,`
and `"` to get three `u64` masks. Fields are the set bits of the comma mask between two newline bits. A line whose
quote mask is zero takes the fast path, and a field touching a quote bit goes through `unquote`. Since 79% of lines
have a quote, the quote handling itself must be cheap: check that the quote bits sit exactly at the field's first and
last byte, rather than branching to a slow path. Use `std::arch` AVX2 intrinsics (our image builds with
`target-cpu=x86-64-v3`) instead of nightly `portable_simd`.

**2. Hash while the bytes are hot.** Dedup hashes `e.id` in a second pass over all events (main.rs:145), and
person resolution hashes `e.user` in a third (main.rs:166). Both walk back into the input buffer. Step 10's lesson
is to compute the key from register-sized loads during the parse. In our case the key must be lossless, because a
collision is a wrong row: pack an ID of up to 15 bytes, plus its length, into a `u128` and compare those directly.
Only longer IDs need the byte-slice path. His add-the-two-halves key is not acceptable here.

**3. Perfect hash for key sets we are given.** He builds his perfect hash from a 4 MB sample, which is a guess. We
know our key sets exactly: `products.csv` (50,000 products on ss20m) and `campaigns.csv` (2,000), both looked up
through foldhash `HashMap`s (main.rs:91, main.rs:97). Build a perfect hash from those files, then do **one** key
compare after the index lookup: unknown products have to map to `unknown`, and unknown campaigns make the click
invalid. That's one probe and one compare instead of a probe loop (step 11: 23 s → 17 s).

**4. Fixed-width fields with one load.** F1 timestamps are always 20 bytes and F2 always 25. `datetime`
(parse.rs:66) checks five separators and calls `d2` seven times, each with two digit checks and an early return. As in steps 28-29: load 16 bytes,
check all 14 digit positions in one masked compare, combine digit pairs with a multiply, then run the calendar check
on the 7 small integers. F3 epoch-ms fields of up to 16 bytes can go through the same trick with two 8-byte loads.

**5. Predictable branches stay; random ones go.** He made the number parse branchless because the digit count is
random per line (step 16), but kept min/max branchy because updates are rare (step 30). For us, dirty rows are rare,
so the validation branches can stay. The format dispatch in `parse::ts` (F1/F2/F3 by content) and the per-field
quote check are random per line, and those are the ones to make branchless. Branch misses from `perf stat` will
show whether this matters.

**6. Interleave independent lines (steps 33-37).** Run 2-4 cursors per rayon chunk and process them stage by stage.
This only paid off for him once the loop was branch-free, at IPC 2.20 → 2.95. Do it after items 1, 4 and 5.

**7. Teardown counts on our clock.** He measured 180 ms in `munmap` for a 13 GB mapping and never fixed it (step 34).
Our container's clock runs to process exit, so freeing the input buffers (5.4 GB on ss60m) and the event vectors is
billed too. Measure the gap between the final `write` lap and actual process exit before choosing between `mmap`,
`read`, and leaving memory unfreed.

**Already in place:** per-thread accumulators merged at the end (main.rs:237-256), and work split into chunks (rayon
over 8 MB chunks, main.rs:20). Our chunks balance load better than his `nthreads` equal slices.

## Files

- `src/main.rs`: the whole solution. Read `iter_lines` (the 4-cursor scan), `run` (the stage-ordered callback),
  `hash_name`, `parse_to_raw` and `raw_to_pdep`.
- `examples/generate.rs`: his input generator. `measurements-small.txt` and `result_ref.txt`: his small test input and
  expected output.
- `justfile`: his `perf stat` / flamegraph / CPU-frequency-pinning commands.
