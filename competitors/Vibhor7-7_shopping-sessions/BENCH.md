# BENCH.md: every experiment, kept or not

## How to read this

- **Scorer** is the only number that counts: the track page's time for a commit, on the hidden ~20M-event practice set.
- **x86 A/B** is what decides whether a change is kept: the `bench` workflow builds two commits with the Dockerfile and runs them interleaved on one GitHub x86-64 machine (2 cores / 4 threads, stand-in 20M-event dataset from `src/bin/synth.rs`). Times are the in-process total in ms, minimum of 5 runs, base → head. Noise between repeats is about 1%. The CPU differs from run to run, so only compare within a row.
- The workflow's 4-thread number has two threads to a core; its **one-thread** number has a core to itself. They can disagree: fewer instructions pay when threads share a core (experiment 22), a stall avoided pays when they do not (experiment 24). The track page says each run gets "8 pinned vCPUs (hyperthreads)", so the scorer is the first case, 4 cores with two threads each: **the 4-thread number is the one to believe.**
- **bench.sh 20M** is `scripts/bench.sh 20M` on the development Mac (ARM, 8 threads, the organizers' generator, seed 120): best / median wall ms of 5. The Mac is under memory pressure and its numbers wander by 10% or more; they are here because the rules ask for one, not because they decide anything.
- **Verified** means: 36 fixtures, `make check-dev`, `scripts/diff_vs_ref.sh` on seeds 2 to 5, on the native build and on an x86-64 build (`scripts/regress.sh`), then CI on real amd64.

## Scorer results

| When (EST) | Commit | Score | Note |
| --- | --- | --- | --- |
| Fri 2026-11-20, 8:49 PM | `d698c87` | 1.05 s | First submission. Debian runtime image, 115 MB. |
| Fri 9:35 PM | `8cb3e8e` | 0.99 s | Experiments 2 to 6. |
| Fri 9:59 PM | `6bbe143` | 0.98 s | Experiments 8, 9 and 11. |
| Fri 10:35 PM | `7f9bffe` | 0.96 s | Experiments 12 to 14. 18% less in-process time than `d698c87` on x86 A/B, 9% less on the scorer. |
| Fri 11:10 PM | `c24627f` | 0.97 s | The BusyBox image (experiment 16) on `7f9bffe`'s code: **the image alone is worth nothing.** |
| Fri 11:49 PM | `c6b4e3e` (code of `e03e2c3`) | 0.82 s | Write-combining buffers (18 to 20): 15% less than `c24627f`, where the x86 A/B gave 6% (Zen 4) to 12% (Ice Lake). |
| Sat 2026-11-21, 12:26 AM | `7a5ddaf` | 0.80 s | Experiment 22 alone: −2.4%, for −4.4% on x86 A/B. |
| Sat 12:53 AM | `fac219f` | **0.78 s** | Experiments 22 to 25: −4.9% from `c6b4e3e`, for −8.7% on x86 A/B. The entry for the final as of now. 10th of 163 teams; the board's best is 0.59 s. |

This table was read off the track page on 2026-11-21 (the first five rows' times too; three of them had not been logged here). `SUBMISSIONS.md` has the full hashes.

What the scores say:
- **The image is not it.** `c24627f` settled what experiment 16 left open.
- **The last two submissions moved the score by about half of what they moved the in-process time**: −2.4% for −4.4%, −4.9% for −8.7%. Either about 0.36 s of the 0.78 s is spent where our own timer does not look (before `main`, and after the process's last line: the kernel unmapping 1.2 GB), or the scorer's CPU gains less than GitHub's from fewer instructions. Experiments 18 to 20 went the other way (−15% for −6 to −12%), so it is not one fixed number. The probe's log is the way to tell.
- The page says practice times vary by up to about 20% between scoring machines: a difference of 0.02 s between two submissions is inside that.

## Experiments

Rows 1 to 15 were run before this log existed and are reconstructed from the benchmark runs; they have x86 A/B numbers but no `bench.sh` number.

| # | Change | Commit | x86 A/B total (ms) | bench.sh 20M | Verified | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Baseline: chunked reader, AVX2 delimiter scan, packed keys, union-find links, dedup stream, per-person sort | `d698c87` | 945 (Zen 4) | n/a | yes | kept |
| 2 | IDs packed 16 bytes at a time (SSE nibble tables + PEXT) | `20ceae3` | 1487 → 1359 (Zen 3), −8.6% | n/a | yes | kept |
| 3 | Sweep: read block 64K/128K/512K, person bits 7 to 10, dedup bits 7 to 10 | none | all within ±1% of defaults (Zen 3) | n/a | n/a | nothing changed |
| 4 | Link join prefetches its table lines; catalog loads without an allocation per row | `3d255f5` | 1378 → 1356 (Zen 3), −1.7% | n/a | yes | kept |
| 5 | Timestamps and link lookup written branch-free | `9304f81` | 1062 → 1070 (Granite Rapids), 0% | n/a | yes | no gain: the compiler rebuilt the branches (callgrind: instructions unchanged). Kept as the base for 6 |
| 6 | The same, forced with `hint::select_unpredictable`; field bounds in registers | `8cb3e8e` | 1147 → 1123 (Zen 4), −2%; parse −3.2% | n/a | yes | kept |
| 7 | Link table twice as large (fewer lookups spilling to a second cache line) | branch, deleted | 1031 → 1045 (Granite Rapids), +1.3% | n/a | yes | **reverted**: building it cost more than the lookups saved |
| 8 | Sessions without sorting: half-hour bins, two counting passes | `f761de9` | 1134 → 1103 (Zen 4), −2.7%; people −10% | n/a | yes | kept |
| 9 | Partition counts and chunk sizes follow the input size | `1c2d0ec` | 1087 → 1082 (Zen 4), noise | n/a | yes | kept, for the 60M final (2048 partitions instead of 512) |
| 10 | Input read in place through `mmap` instead of `read` | `d037d62`, reverted in `6bbe143` | 998 → 1073 (Granite Rapids), +7.5%; parse +11% | n/a | yes | **reverted**: page-table walks on 4 KB file pages cost more than the copy, on a VM |
| 11 | Timestamp fall-back kept a single branch with `black_box` | `6bbe143` | 1033 → 1032 (Granite Rapids), 0% | n/a | yes | kept: no speed change, but the code no longer depends on the compiler's mood (callgrind: 733k → 318k mispredictions) |
| 12 | Blocks of 2 MB and more on huge pages (own allocator); date table compared in two words | `0c6d2a0` | 885 → 852 (Zen 4), −3.7%; people −11.6% | n/a | yes | kept |
| 13 | Blocks of 32 KB to 2 MB on huge pages too | `ee6f925` | 975 → 968 (Granite Rapids), −0.7% | n/a | yes | kept |
| 14 | Link table as 4 IDs + 4 people per cache line; dedup with one two-bit array | `b58dc53` | 821 → 809 (Zen 4), −1.5% | n/a | yes | kept |
| 15 | Direct comparison of #1 with #11 | `d698c87` vs `6bbe143` | 945 → 825 (Zen 4), −12.7% | n/a | n/a | measurement only |
| — | Baseline for what follows | `7f9bffe` | — | 384 / 413, peak 1259 MB | yes | — |
| 16 | Runtime image BusyBox (2.3 MB) instead of Debian (115 MB); binary unchanged | `c5b2dfd` | 1261 → 1257 (Zen 3): in-process identical, as it must be. `docker run` wall on the runner is the same for both images (Docker mounts layers, it does not unpack them) | not rerun: same binary | yes | kept. Scored only together with 18 to 20, as `e03e2c3`: 0.96 → 0.82 s. What the image alone is worth is still unknown |
| 17 | Measurement: where a container's time goes outside the process (`overhead` workflow, 60M events, GitHub's Docker) | `c24627f` | per run: ~80 ms before `main`, ~2880 ms in the process, ~45 ms after it with output on tmpfs, ~80 ms with output on disk. The first run after an image is built: 6 s before `main`, once | n/a | n/a | measurement only. Under Docker, 0.13 to 0.16 s per run is outside the process, whatever the image and limits; writing 222 MB to disk adds ~35 ms after the process and nothing inside it. Says nothing about the scorer's `runc` sandboxes |
| — | Thread count on 2 cores / 4 hyperthreads (Ice Lake Xeon 8370C, the scorer's class of CPU) | `1cac78b` | 2 threads 1812, 3 threads 1643, **4 threads 1492**, 6 threads 1518, 8 threads 1523 | n/a | n/a | measurement only: the second hyperthread of a core is worth 21%; more threads than hyperthreads costs 2% |
| 18 | Event and copy records leave the parser through write-combining buffers and non-temporal stores (no cache line of fresh output is fetched just to be overwritten) | `9fcffee` | **1514 → 1334 (Ice Lake 8370C), −12%**; parse 1110 → 931 (−16%); one thread: parse 2688 → 2152 (−20%) | 374 / 401, peak 1269 MB (ARM has no such stores; no change expected there) | yes | **kept** |
| — | Sweep on top of 18: read block 16K/32K/64K/128K (default 256K); partition bits 9/9 and 11/10 (default 10/10) | `476a8a8` | defaults 791 (Zen 4); 16K 813, 32K 804, 64K 791, 128K 798; bits 9/9 797, 11/10 800 | n/a | n/a | nothing changed: the defaults are the best or tied |
| 19 | Click records through write-combining buffers too (8 records, three cache lines) | `daa71f5` | 1249 → 1240 (Zen 3), −0.7%; one thread: parse −2.5% | with 20 | yes | kept |
| 20 | Eight records to a buffer for the 16-byte streams: half the flush branches, twice the buffer | `35d159d` | 1239 → 1227 (Zen 3), −0.9%; one thread: parse −2.2% | 374 / 455, peak 1267 MB | yes | kept |
| — | 18 to 20 together against the last submitted commit, at 20M and at 60M | `c5b2dfd` vs `e03e2c3` | 20M: **756 → 708 (Zen 4), −6.4%**; parse −10%; one thread −15.5%. 60M: 3819 → 3759 (Zen 3), −1.3% | as 20 | yes | measurement only. The gain depends on the CPU: −12% on Ice Lake (18 alone), −6% on Zen 4, about −1% on Zen 3 |
| — | 60M sweep of partition bits (default 11/11): 10/10, 10/11, 11/10, 9/9 | `e03e2c3` | defaults 4028 (Ice Lake); 3989, 3999, 4017, 4027 | n/a | n/a | nothing changed: all within 1%, which is the noise. Fewer partitions make parse faster and people slower by the same amount |
| 21 | At 60M, four records to a buffer again for the 16-byte streams (0.5 MB of buffers a thread instead of 0.9 MB) | branch, deleted | 60M: 3738 → 3789 (Zen 3), +1.4% | n/a | fixtures and dev check | **reverted** (never merged): the larger buffers are better at 60M too |
| — | Measurement: instructions per line (`profile` workflow: callgrind, 1M events, one thread) | `1359a83` | 1.33 billion instructions for 1.30M event and click lines: about 1,000 a line over the whole run, about 680 of them in parse. 3.8M mispredicted branches, 3 a line: 325k on "is this write-combining buffer full" (one push in eight, at random), 113k on "is the event valid", the rest in the people phase | n/a | n/a | measurement only: parse is limited by how many instructions a line costs, not by memory |
| — | Measurement: the parse phase cut short at each stage (`SS_STOP`, branch `anatomy`, never merged) | `anatomy` on `1359a83` | 20M, Zen 4 (EPYC 9V74), 4 threads, cumulative parse ms: read 49, + delimiter scan 136, + line walk 155, + fields into values (unquote, keys, timestamps, validation) 503, + link lookup 570, + event records 670 (copy records instead: 643), everything 720. One thread: 105, 318, 361, 1152, 1313, 1647 (1512), 1758 | Mac, 8 threads: 38, 56, 61, 148, 177, 229 (248), 312 | n/a | measurement only: **48% of parse is turning fields into values**, 21% is writing the two record streams, 12% is the scan, 9% the link lookup, 7% `read()`. The knobs themselves cost 4% |
| — | Measurement: a container's time outside the process, at 20M (`overhead` workflow) | `1359a83` | per `docker run`: 115 to 129 ms before `main`, 1226 to 1260 ms in the process, 59 to 73 ms after it; the same with output on tmpfs, without limits, or pinned with a cpuset | n/a | n/a | measurement only: about 190 ms outside the process at 20M, as at 60M (experiment 17). Nothing we write changes it |
| 22 | One delimiter stream (commas and newlines in one array), and a line's fields split with vector instructions: the 4 bytes around each of a line's 8 delimiters are compared with a quote at once. The compiled loop spent about 145 of its 650 instructions a line on seven rounds of "is this field wrapped"; now about 60 | `7a5ddaf` | **999 → 955 (Zen 4, EPYC 9V74), −4.4%**; repeat 1003 → 959; parse 702 → 660 (−5.9%). One thread: 2510 → 2521, no change (fewer instructions only pay when two threads share a core) | 382 / 384, peak 1258 MB (ARM takes the scalar path) | yes | **kept** |
| 23 | `ingest_ts` checked with compare-and-branch (its form never varies, so the branches predict) instead of a dozen flag instructions, and its seconds taken straight from the parse, not by multiplying by 1000 and dividing again | `80df6c5` | 1250 → 1229 (Ice Lake 8370C), −1.7%; repeat 1248 → 1231, −1.3%; parse 850 → 830 (−2.4%). One thread: 3026 → 2993, −1.1% | 381 / 387, peak 1262 MB | yes | kept |
| — | Measurement: where the time is, not the instructions (`profile` workflow, new step: `perf` sampling on the CPU clock, 20M events, 4 threads, Zen 4) | `1359a83` | parse 52% of samples, people 16%, the delimiter scan 11%, dedup 4% + 1.7% sorting its suspects, kernel: 3.2% copying file data, 2.2% zeroing new pages. By line: **10% on the first touch of a push** (loading the partition's state, writing into its buffer), 2.2% on the campaign and product hash table's slot, 8% across the timestamp code, 3.5% in the hash function | n/a | n/a | measurement only. It named experiments 24 and 25 |
| 24 | The link table is read 12 lines after a line is parsed instead of 16; that names the person, so both partitions, whose state and buffer are prefetched then; the records are written 4 lines later | `86b8dce` | 20M: 1228 → 1221 (Ice Lake 8370C), −0.6%; repeat 1232 → 1229: nothing, with two threads to a core. **One thread: 3025 → 2935, −3.0%**; parse −4.6%. 60M (2^11 partitions): 2887 → 2825 (Granite Rapids 6975P-C), −2.2%; repeat −1.5%; one thread 7529 → 7311, −2.9% | 387 / 411, peak 1271 MB | yes | kept. A stall hurts less when a second thread can use the core meanwhile, and the scorer's 8 CPUs are hyperthreads (the track page says so), so expect the 4-thread numbers there: little at 20M, 2% at 60M |
| 25 | Campaign and product IDs of the files' own shape (a prefix and a number: `c_45`, `p123`) looked up by the number in a small array; any other text, and any number past the array, still goes through the map, which holds every ID | `ea498ca` | 1160 → 1150 (Zen 3), −0.9%; repeat 1162 → 1148, −1.2%; parse −1.7%. One thread: 3117 → 3076, −1.3% | 397 / 443, peak 1256 MB | yes (and a unit test against the map on 25,000 spellings) | kept |
| — | 22 to 25 together against the last scored commit, at 20M and at 60M | `e03e2c3` vs `f245e54` | 20M: **1244 → 1135 (Zen 3), −8.7%**; repeat 1242 → 1135; parse −12.4%; one thread 3134 → 2843, −9.3%. 60M: **3867 → 3483 (Zen 4, EPYC 9V74), −9.9%**; repeat 3828 → 3475, −9.2%; one thread 9720 → 8724, −10.2% | as 24 | yes | measurement only |
| — | Measurement: the profile again, after 22 to 25 (Sapphire Rapids 8573C, 4 threads) | `f245e54` | 1.26 billion instructions (was 1.33). Samples: parse 45%, people 19% + 1.4% sorting clicks, the delimiter scan 9%, **kernel 10%: 5.0% copying file data, 4.8% zeroing the 1.2 GB of new pages**, dedup 4.4% + 1.7% sorting suspects, links 4% | n/a | n/a | measurement only. The first touch of a push is down from 10% to about 5% of samples |

## The probe (branch `probe`)

Not for scoring. It runs the pipeline and then makes `user_daily` empty so that the dev check fails: a failed dev check is the only case in which the track page shows a submission its log. A rejected submission does not change the best score; it does use one of the two submissions an hour. Merge `main` into it before use; the commit to hand over is in the session's report, not here, because it changes with every merge.

What it logs, and what each answer decides (first in the log is first here, in case a long log is cut):
- **the parse phase of the dev set cut short at each stage, 8 threads** (read, + delimiter scan, + line walk, + fields into values, + link lookups, + event records, everything): where parse time goes on the scorer's CPU, to set against the same list from GitHub's machines (the `anatomy` measurement above);
- **the whole parse phase on 1, 2, 4, 6, 8, 12 and 16 threads**, and `read()` + scan alone: whether the 8 CPUs are 8 cores or 4 cores' hyperthreads, so whether the 4-thread or the one-thread column of this file is the one to believe, and whether memory bandwidth caps the scan;
- the CPU model and its AVX-512 subsets, cgroup CPU and memory limits, `cpu.stat` (was the run throttled), huge-page settings, every mount, the environment;
- when the sandbox's files and the process were created: how long a sandbox takes to reach `main` (experiment 16);
- the same spin loop on 1 to 16 threads: real CPUs, without memory in the picture;
- **256 MB touched and unmapped under each huge-page advice, on 1 and 8 threads**: what 4 KB pages cost here going in and at exit. The pipeline touches 1.2 GB at 20M events and 3.4 GB at 60M;
- `/output`, `/scratch` and `/tmp` write and read speed: whether a smaller output matters;
- `read()` against `mmap()` on the largest event file: whether experiment 10 should be retried;
- 256-bit against 512-bit scans, alone and alternating with scalar work: whether a 512-bit stage one pays.

## Before each submission

| Commit | Clean image build | Fixtures (image, scorer flags) | check-dev | diff seeds 2 to 5 | 60M: time | 60M: peak memory | Image |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `c5b2dfd` | yes, `--no-cache`, amd64 | 36/36 (locally under emulation and on CI's real amd64) | MATCH (native build, and the image's output) | 16/16 runs match | x86 CI, Docker `--cpus 4 --memory 12g` (the runner has no more): **4.06 to 4.34 s** per `docker run`, 3.07 s inside the process. Mac, native: 4.4 s best of 3, on a machine short of memory and disk | **3.4 GB** (x86, 4 threads); 3.1 to 3.4 GB on the Mac | 2.3 MB root filesystem, 3 layers |
| `e03e2c3` | yes, `--no-cache`, amd64 | 36/36 (locally under emulation and on CI's real amd64) | MATCH (native build, x86-64 build, and the image's output) | 16/16 runs match, on both builds | x86 CI, Docker `--cpus 4 --memory 12g`: **4.21 to 4.28 s** per `docker run` on Zen 3 (3.75 to 3.81 s inside the process), 4.47 to 4.58 s on Ice Lake (4.05 to 4.16 s inside). Not run on the Mac this time: 7 GB of free disk is not enough for the 5.4 GB dataset and its output without distorting the run | **3.5 GB** (x86, 4 threads) | 2.3 MB root filesystem (the binary is 0.8 MB), 3 layers |
| `f245e54` (code of `86b8dce`) | yes, `--no-cache`, amd64 | 36/36 (locally under emulation and on CI's real amd64) | MATCH (native build, x86-64 build, and the image's output) | 16/16 runs match, on both builds; leap-day and year-end seeds 8/8 | x86 CI, Docker `--cpus 4 --memory 12g`: **3.97 to 3.98 s** per `docker run` on Zen 4 (EPYC 9V74), 3.48 to 3.51 s inside the process; `e03e2c3` on the same machine: 3.83 to 3.87 s inside. Not run on the Mac (no room for the dataset) | **3.4 GB** (x86, 4 threads; 3,499 MB by `time`) | 2.44 MB root filesystem (the binary is 0.82 MB), 3 layers |

Notes on the 60M runs:
- No machine at hand has 8 CPUs and 24 GB for a container: the Mac's Docker VM has 8 GB, the CI runner 4 CPUs and 16 GB. The numbers above are under the largest limits each allows.
- The reference cannot run at 60M (it would need tens of GB), so correctness at that size rests on: two runs with different thread and partition counts (8 threads, 2^11/2^11 partitions; 3 threads, 2^8/2^12) giving identical tables (6,623,558 user rows; digests equal); `sessions` and `revenue_cents` summing to the same in both tables (10,012,569 and 19,153,547,521); and the diff against the reference at up to 3M events.
- On the CI runner, `docker run` at 60M takes 0.4 to 0.5 s longer than the process inside it; at 20M the difference is 0.4 s on some runners and 0.2 s on others. An empty container is 0.13 to 0.19 s of that (experiment 17). The first run after a build is far slower, once.

## To try, most promising first

1. **Read the scorer's machine**: the probe's log, if the user spends a submission on it (it has not been submitted; an unattended cycle never submits it, because it fails the dev check on purpose). The track page already answers one of its questions: the 8 CPUs are hyperthreads. Still open, and worth more now that half of each gain goes missing on the scorer: how long a sandbox takes to reach `main`, what 4 KB pages cost there going in and at exit, how fast `/output` is, and whether AVX-512 is there.
2. **`mmap` again, if the scorer is bare metal.** It lost on virtual machines because of page-table walks (experiment 10); on bare metal those are cheap, and the copy it saves is 5% of the samples.
3. **AVX-512 stage one**, if the probe shows it there and not slowing the scalar code around it. The scan is 9 to 11% of the run; with `vpcompressb` (Ice Lake and later) a block's delimiters become offsets in four instructions instead of thirty.
4. **Dedup without a record for every copy.** 97% of copy records only prove that an event has no second copy. They cost a push a line (about 10% of parse), 330 MB of new pages at 20M, and the dedup phase with its sort (6%). No design yet: every exact filter seen so far needs a table the size of the IDs, read at random.
5. **Fewer new pages.** Zeroing them is 4.8% of samples on the Intel runner. The copy records' memory is dead after dedup and could hold the output columns; the output could be written from its columns without first being copied into one buffer (78 MB at 20M).
6. **The people phase**, 19% and not yet read line by line in the sampled profile: three passes over each partition's events, a hash lookup an event, a sort of each person's clicks.
7. **Instruction cuts left in parse**, 1 to 2% each: both IDs of a line packed in one 256-bit operation; the `ts` flags; field bounds read from memory instead of extracted from a vector register.
8. **Smaller output.** `user_daily` is 78 MB of PLAIN Parquet at 20M events. Dictionary and run-length encodings would cut it to about a fifth. Worth 1% under Docker (experiment 17); more only if the probe shows the scorer's `/output` is slow.
9. **Dedup's sort of suspects**, 1.7%: a second filter to drop the chance collisions before sorting, or a small hash table instead of the sort.
