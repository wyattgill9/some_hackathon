# What the fast entries do

Compiled from the five snapshots in this folder (see README.md). Techniques only; no code is copied.

## Part 1: the compiled technique list (what the fast entries do)

Legend: **D** = Dremix10/deltaparticle C++ (0.66 s), **V** = Vibhor (0.78 s, has per-change measurements), **C** = cherylaurelia (~0.4 s local), **N** = NhanHu (<0.55 s local). Measured effects are V's A/B numbers unless noted.

### 1. Process & build
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 1.1 | Static binary in `FROM scratch` (or 2 MB busybox) | D N V | Image size itself "worth nothing" (V exp 16), but static = no dynamic loader |
| 1.2 | `-C target-cpu=x86-64-v3` (AVX2+BMI2), **no AVX-512** by default | D V C | D: 512-bit ops downclock Skylake-X/Cascade Lake; N uses `cascadelake` |
| 1.3 | fat LTO, codegen-units=1, panic=abort | all | |
| 1.4 | JCC-erratum branch alignment (`-mbranches-within-32B-boundaries`) | D | GCC flag; Rust: `-C llvm-args=-x86-branches-within-32B-boundaries` (untested) |
| 1.5 | Zero dependencies (hand-declared `extern "C"` mmap/madvise) | V N | Build can't break on crates.io |
| 1.6 | Exit with `_exit(0)`/`process::exit` without freeing anything | all | D also `MADV_DONTNEED`s arenas in parallel first ("exit would tear down GBs on one core") |
| 1.7 | NCPU threads exactly (8); 2nd hyperthread worth +21%, >HT count costs 2% | V | |
| 1.8 | Persistent spin-then-sleep thread pool | D (post-0.66) | Low priority: 0.66 s was scored with fresh threads per phase |

### 2. Input I/O
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 2.1 | Cut every file into byte ranges (1–4 MiB), one global dynamic queue (atomic counter), largest files first; a range owns the lines that *start* in it; header skipped by position | all | Load-balances Zipf file sizes; no per-file threads |
| 2.2 | `pread` into a reused per-thread buffer, 256–512 KiB blocks (stays in L2), padded tail for SIMD over-reads | V C | **mmap measured +7.5–17% slower on VMs** (page-table walks on 4 KB file pages, V exp 10). Scorer is a GCP VM |
| 2.3 | mmap with a zero guard page after EOF (reserve anon `len+4096`, `MAP_FIXED` file over it), parse in place; `MADV_DONTNEED` input when done | D N | D uses this and is faster overall, but differs in many other ways. **A/B on the scorer** |
| 2.4 | Cold-cache `readahead(2)` threads, started only if `preadv2(RWF_NOWAIT)` probe misses | C | Only matters if the scorer's page cache is cold |
| 2.5 | Small tables (products/campaigns) parsed on the main thread while links run | D C | |

### 3. CSV scanning (the dialect)
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 3.1 | Stage 1: per 64 B block, AVX2 `cmpeq`+`movemask` for `\n` `,` `"` → three u64 bitmaps | all | simdjson style; 2 loads + 3 cmps + 3 movemasks per 64 B |
| 3.2 | Flatten bitmaps to u32 offset arrays with `tzcnt` + `m &= m-1`, branch-free fixed-N writes (N=8 delims, 2 newlines) | D V | V: one merged comma+newline stream with sentinels |
| 3.3 | Field-count check by delimiter index arithmetic (`delims[d+NF] == newline`) | V D | Wrong count / empty line → drop, resync to the newline |
| 3.4 | Quote validity: `quotes_in_line == 2 × wrapped_fields`, wrapped = len≥2 && first==`"` && last==`"` | V | Exact for the dialect; quotes counted via prefix popcount |
| 3.5 | Alt: bit-trick quote validity `M & !(M - X)` (N), or carry-add `FIELD_LOGIC` (D) | N D | Branch-free; more complex |
| 3.6 | Vector field split: the 4 bytes around each of 8 delimiters compared to `"` at once | V | exp 22: −4.4% A/B, −2.4% scorer |
| 3.7 | `event_type` checked with 4-byte (`view`,`cart`) / 8-byte (`purchase`) integer compares | D | |
| 3.8 | `\r` never special-cased: it fails length/digit checks on its own; lines ≥128 B or with NUL go to a scalar slow path | all | |
| 3.9 | Prefetch 1 KB ahead in stage 1 ("HW prefetcher restarts every 4 KB page") | D | |

### 4. Timestamps
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 4.1 | Times stored as **u32 ms relative to `window_start − 7d`**; window check = one unsigned compare | all | Limits window to <42 days (generator: 14); exit 2 otherwise |
| 4.2 | Per-thread **date cache** keyed on the `YYYY-MM-DD` bytes (32–1024 entries): hit = 2 integer compares; miss = full calendar validation | all | D uses a perfect-hash table built at startup for window±12 days |
| 4.3 | SWAR `HH:MM:SS` check: `x ^ "00:00:00"`, `(x + 0x7676_7F76_767F_7676) \| x` & `0x80…` = 0; digits via `x*10 + (x>>8)` or PEXT + tables | all | No division, no per-digit loop |
| 4.4 | F3 fast path for 13 digits (SWAR/SSE4.1 madd chain); 1–16 digits SWAR; longer → exact slow path (leading zeros: `000…1791021600000` is valid and in window; only a genuinely huge value is "out of window") | all | |
| 4.5 | Both F1 and F3 computed, result chosen with `hint::select_unpredictable`; one `black_box` fallback branch | V | Form is a coin toss per line → branch mispredicts; exp 6/11: −2%, mispredicts 733k→318k |
| 4.6 | `ingest_ts` on its own always-F1 predictable path; kept as u32 seconds (F1 is whole seconds) | V D | exp 23: −1.7%. D's "same-day skips the late check" shortcut is a trap under F2 (`…+02:00` local date ≠ UTC date): not adopted |
| 4.7 | Full exact parser kept as the cold path for everything irregular | all | We already have one (`src/parse.rs`) |

### 5. IDs & lookups
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 5.1 | **IDs packed into order-preserving u64**: 6 bits/char, ≤10 chars of `[0-9A-Z_a-z]`, first char in top bits, low 4 bits 0 | V C | Integer compare = byte-wise compare; every generator ID (≤9 chars) packs; exp 2 (SIMD pack): −8.6% |
| 5.2 | Alt packings: 7 bits × ≤9 ASCII bytes via one `pext` (N, zero headroom); raw "letter + ≤8 bytes" load&mask (D) | N D | |
| 5.3 | Anything else → **interned** via mutex `HashMap<Vec<u8>,u32>`, tag bit set; `cmp()` decodes when either side is interned | all | Exact for ` u1`, `u1\r`, `A\tB`, 11+ chars; slow but never wrong |
| 5.4 | SIMD packing: `pshufb` nibble class tables + `pext` (16 B at a time); both IDs of a line in one 256-bit op | V C | |
| 5.5 | Product/campaign IDs of shape `p123` / `c_45` looked up **by number in a small array** (≤7 digits, no leading zero); other spellings via exact map | D V | exp 25: −1%. Campaign array fits L1 |
| 5.6 | Categories ranked byte-wise (`unknown` included) so `top_category` tie-break = smallest index | all | |
| 5.7 | Output decodes keys 6 bits at a time (or 2 chars per lookup via a 4096-entry table) | V C | |

### 6. Links / persons
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 6.1 | **Links first, in parallel**: parse on all threads → lock-free union-find (CAS) → flatten so the table maps ID → person name; events/clicks then carry the person from parse time | V N C | V: links ≈4% of runtime |
| 6.2 | Union always hangs the root with the **larger** key under the smaller → the root *is* the byte-wise smallest name, no min pass | V C D | |
| 6.3 | Path halving with CAS; no rank/size | all | |
| 6.4 | Table layout: 4 keys + 4 persons per 64 B line, compared with one `cmpeq_epi64` | V | exp 14: −1.5%; exp 7 (2× larger table) was +1.3% → reverted |
| 6.5 | Lookups **prefetched 12–16 lines ahead** via a ring of pending records | V C N | exp 24; C: 3-stage software pipeline; N: batches of 16 + 512-entry direct-mapped cache |
| 6.6 | Alt (D): resolve persons in phase 3 once per *distinct* user ID per partition, forward linked IDs' data to the person's partition in a 2nd round | D | ~20M lookups → ~1M; more complex |
| 6.7 | Unlinked IDs are not in the table → person = the ID itself | V D | |

### 7. Records, partitioning, memory
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 7.1 | **Fixed-width records, nothing borrows the input**: event copy 16–32 B, survivor 12–16 B, click 16–24 B; `pay = price(24b) \| cat(8b)` | all | vs our 56 B + slices |
| 7.2 | **Hash-partition during parse** into 2^10 (20M) / 2^11 (60M) partitions by top bits of `key × 0x9E3779B97F4A7C15`: events by `event_id` (dedup), survivors & clicks by person | all | Every later phase works on an L2-sized partition with private tables; no locks, no global sort, no merge |
| 7.3 | Per-(thread, partition) chunk lists of fixed blocks (6–16 KiB) from per-thread bump arenas (32 MiB mmap regions); non-power-of-2 block size to avoid cache aliasing | all | N: 16 KiB+192 |
| 7.4 | **Write-combining buffers + non-temporal stores** (8 records = 2–3 cache lines staged, flushed with `_mm256_stream_si256`, `sfence` at end) | V C D | **V's biggest scorer win: 0.97 → 0.82 s** (exp 18–20; −12% on Ice Lake). Avoids read-for-ownership on ~3000 output streams |
| 7.5 | Partition count / chunk sizes derived from input bytes | V D C | |
| 7.6 | Transparent huge pages: `mmap(NORESERVE)` 2 MB-aligned + `MADV_HUGEPAGE` for every buffer ≥32 KB (global allocator or arenas); `mremap` for growth | all | exp 12+13: −4.4%; also pre-touch to avoid zero-page COW |
| 7.7 | Recycle consumed blocks (dedup input → person-phase output) ; reuse per-thread scratch across partitions; generation stamps instead of clearing | D N C | Kernel page zeroing was 4.8% of V's samples |
| 7.8 | Prefetch the partition's buffer/state 4 lines before the push | V | exp 24 |

### 8. Dedup
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 8.1 | Per event-id partition, private open-addressing table (u32 index, ≤50–67% load, hash on different bits than the partition), keep min `ingest`; validity decided at parse time so an invalid earliest copy still wins | D N | D: 4-byte slots (16-bit tag + block/idx) so the table stays in L2 |
| 8.2 | Two-bit counter filter finds the ~3% of ids seen twice; only those are sorted by (key, ingest) | V | dedup = 4.4% + 1.7% sort |
| 8.3 | Mark losers in place (`DROPPED` sentinel) instead of re-scattering survivors (one scatter pass instead of two) | V C | Needs stable record addresses/indices |
| 8.4 | Clicks are **never deduplicated** (copies identical → same credit, same assist set) | all | |

### 9. Sessions & attribution
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 9.1 | Per person partition: dense local person index (small open-addressing table), **counting sort** events/clicks by person (uninit `set_len`, no zeroing) | all | |
| 9.2 | **Sort-free sessions**: 30-min bins; events in a bin always share a session; split iff `min(next bin) − max(prev bin) > 30 min`; equal ts automatic | V C D | exp 8: people phase −10% (−2.7% total); alt (N): per-person `sort_unstable` on `ts<<32\|pay` u64 |
| 9.3 | Clicks per person sorted by (ts asc, click_id **desc**) so `clicks[hi−1]` is the credit (latest ts, smallest id) | V N | |
| 9.4 | Two-pointer 7-day window + per-campaign counts + O(1) swap-remove active list for assists; small persons use a quadratic/AVX2 path; whales radix-sort clicks | all | C: AVX2 window masks for ≤64 clicks; D: radix for ≥2048 |
| 9.5 | Dense per-thread `[campaign+1][day]` counters (u8 with overflow carry in D), merged in parallel at the end | all | |
| 9.6 | `top_category`: per-row category sums with a touched list / max scan over ≤255 categories | all | |
| 9.7 | Partitions processed largest-first (longest-job-first) | D C N | |

### 10. Output
| # | Technique | Who | Notes / effect |
|---|---|---|---|
| 10.1 | **Hand-rolled Parquet** (~230–310 lines): Thrift compact footer, one row group, one v1 DATA_PAGE per column, PLAIN, UNCOMPRESSED, no stats/dictionary; `day` = INT32 + DATE converted+logical type; optional `top_category` def-levels as one bit-packed run | all | No Arrow, no compression |
| 10.2 | **One `user_daily/part-N.parquet` per worker, written by the worker as soon as it runs out of partitions** (overlaps the people phase); `campaign_daily` one file on the main thread | all | |
| 10.3 | Build the file in one `Vec`, one `write_all` (or `writev` from block chains) — a write per page cost 10 ms/file with 8 writers | C D | |
| 10.4 | Counts as INT32 where they fit ("any integer width accepted") | V C D | |
| 10.5 | Dictionary + delta encodings (user_daily 78 MB → ~15 MB) | C | V estimates ~1% under Docker; only if `/output` is slow |
| 10.6 | `umask(0)`, files 0666 | V | |

### 11. Method (how they got there)
- A **slow literal reference implementation** diffed against the fast one on seeds with no published answer (V, N, D all do this); runs at 1–3M events; plus thread-count/partition-count independence checks and leap-day (seed 15) / year-end windows.
- A/B benchmarks interleaving two commits on one machine, min of 5; keep only measured wins (V reverted 3 of 25).
- Measured dead ends: mmap on VMs (+7.5%), bigger link table (+1.3%), 4-record WC buffers at 60M (+1.4%), busybox image alone (0%), block/partition size sweeps (±1%).
- V's profile after everything: parse 45% (48% of that is fields→values), people 19%, delimiter scan 9%, kernel 10% (read copy 5%, page zeroing 4.8%), dedup 6%, links 4%.
