# Shopping Sessions

A hand-written pipeline for the Shopping Sessions track: dirty event, click and link CSVs in, two exact daily Parquet tables out. `rules.md` is the specification.

It is one static Rust binary with no dependencies. The CSV dialect in `rules.md` does not match any engine's reader (a `\r` is data, every comma splits, a quote is only legal around a whole field), so the parser is written to the rules instead of bending a general one to them.

## Run it

```sh
make build        # docker buildx build --platform linux/amd64 -t shopping-sessions:latest .
make fixtures     # the image on all 36 edge-case fixtures, with the scorer's flags
make run-dev      # the image on dev/, timed
make check-dev    # exact diff against dev-answer/
```

On an Apple-silicon Mac add `PLATFORM=linux/arm64` to run natively instead of under emulation.

Without Docker, with a Rust toolchain:

```sh
make native-fixtures   # cargo build --release, then the 36 fixtures
make check-dev         # the dev set, diffed against dev-answer/
make test              # unit tests
```

The test scripts need `uv` (they run `uv run --with polars`); set `PY=python3` if you have Python 3.10+ with polars installed.

## How it works

| Phase | What happens | File |
| --- | --- | --- |
| links | `links/` is parsed in parallel into edges and joined with a lock-free union-find. A root always adopts the byte-wise smaller ID, so each tree's root is the person's name. | `src/links.rs` |
| parse | Files are cut into ranges; each thread reads a range in 256 KB blocks, finds every newline, comma and quote 64 bytes at a time (AVX2), splits each line into its fields with a handful of vector instructions, then validates it. A surviving event is filed under its person; every parsed copy also leaves a small dedup record keyed by `event_id`. | `src/reader.rs`, `src/scan.rs`, `src/parse.rs`, `src/ts.rs`, `src/key.rs` |
| dedup | Per partition, a table of two-bit counters finds the few `event_id`s seen twice; only those copies are sorted, and every copy but the earliest has its event dropped. | `src/dedup.rs` |
| people | Per partition, events are put in half-hour-bin order by two counting passes (never sorted: events under 30 minutes apart always share a session) and walked once per person: sessions, the crediting click, the assisting campaigns. Each thread writes its own `user_daily` file. | `src/person.rs` |
| output | `campaign_daily`, and a minimal Parquet writer (PLAIN, uncompressed). | `src/main.rs`, `src/parquet.rs` |

Things that make it fast:

- **IDs are 64-bit keys.** An ID of up to 10 characters from `[0-9A-Z_a-z]` packs into a `u64` whose integer order is the ID's byte order, 16 bytes at a time (`src/key.rs`). Every ID the generator writes fits. Any other ID (a space, a tab, a `\r`, too long) is interned on a slow, exact path, so the rules hold for any input.
- **Dedup without a second pass over events.** A copy that would fail validation gets no event record, so if it is the earliest copy, dropping the other copies' records leaves nothing: "dedup comes before validation" for free.
- **Clicks need no dedup.** Copies of a click are identical, so they credit the same campaign and assist as the same campaign once.
- **No branch the CPU cannot predict on the hot path.** Which of the three forms a `ts` takes, and whether a user ID is linked, are coin tosses from line to line; both answers are computed and selected. The date part of a timestamp is a table hit.
- **Lookups are prefetched.** The link table is larger than the cache; each line asks for its cache line 12 lines before reading it, and the four slots of that line are compared at once. Reading it names the person, so the partitions the line's records go to: those are asked for then, 4 lines before the records are written.
- **Catalog IDs by number.** The files' own IDs are a prefix and a number (`c_45`, `p123`); text of exactly that shape is looked up in a small array by the number, anything else in a map that holds every ID.
- **Huge pages.** Every buffer of 32 KB or more is on memory advised for transparent huge pages (`src/sys.rs`): fewer page faults, fewer TLB misses, less for the kernel to free at exit.

Exactness limits, none of which the generator can reach: the window must be shorter than 42 days (timestamps are stored as 32-bit offsets), and `ingest_ts` is compared in whole seconds (it is always form F1).

## Testing

- `fixtures/`: the organizers' 36 hand-written cases.
- `dev-answer/`: the published answer for the 1M-event dev set (not committed; unzip `dev-answer.zip` here).
- `ref/reference.rs`: a second, deliberately slow implementation that reads `rules.md` literally and shares no code with the pipeline. `make reference` checks it against the fixtures and the dev answer.
- `scripts/diff_vs_ref.sh`: the pipeline against the reference on other seeds (leap-day and year-boundary windows included), with different thread and partition counts.
- `scripts/regress.sh`: all of the above, on the native build and on an x86-64 build.
- `scripts/fuzz.sh`: the same comparison on data dirtied far past what the generator does, read in tiny blocks and tiny file ranges so every boundary is hit.

`dev/` and the generator are not in this repository; `make dev` builds the dev set if `generate` is on your `PATH` or in `.tools/`.

## Measuring

A laptop cannot say what is fast on the scorer's Xeon, so two workflows run on GitHub's x86-64 machines, started by hand:

```sh
gh workflow run bench.yml -f base=<commit>     # this commit against another, interleaved runs, per-phase minimums
scripts/bench_result.sh                        # wait for it and print the timings
gh workflow run profile.yml                    # instructions and mispredicted branches per source line (callgrind),
                                               # and where the time is on 4 threads (perf, sampled on the CPU clock)
```

Both use `src/bin/synth.rs`, a stand-in for the organizers' generator that writes data of the same shape. Things measured and dropped: reading files through `mmap` (11 to 17% slower on a virtual machine), a larger link table, smaller read blocks.

## Tuning knobs

Environment variables, for experiments; the defaults are what the scorer runs.

| Variable | Meaning |
| --- | --- |
| `NCPU` | Threads (the scorer sets 8) |
| `SS_PERSON_BITS`, `SS_DEDUP_BITS` | log2 of the partition counts |
| `SS_JOB_BYTES`, `SS_BLOCK_BYTES` | File range and read block sizes; tiny values are for tests |
