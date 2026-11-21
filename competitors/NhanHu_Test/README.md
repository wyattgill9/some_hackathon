# Shopping Sessions: a multithreaded Rust pipeline

One static Rust binary, no dependencies, in a `FROM scratch` image. It reads `$DATA_DIR` and writes `$OUTPUT_DIR/user_daily/*.parquet` and `$OUTPUT_DIR/campaign_daily/*.parquet`, implementing `rules.md` exactly.

## Results checked

- All 36 fixtures and the public dev answer match exactly, both natively and in the image run with the scorer's flags (`scripts/docker_run.sh`).
- `scripts/reference.py` is a slow, literal Python version of the rules. It passes the fixtures and the dev answer. The pipeline also matches it on seeds 2, 3, 4, 5 and 15 (seed 15's window contains 29 February 2028) at 1, 3 and 8 threads.

## How it works

1. **Read.** Every CSV is memory-mapped (with zero padding after it) and split into 4 MiB chunks, which threads take from a shared queue. A line is split with 256-bit compares over a 128-byte window. Quotes are validated with a bit trick: for start-quoted fields `X` and separators `M`, `M & !(M - X)` marks each field's terminator, so the closing quotes must be that shifted right by one.
2. **IDs.** A string of at most 9 bytes from 0x01–0x7F is packed exactly, and in byte order, into a u64 with BMI2 `pext`. Anything else is interned. A lock-free, insert-only hash table maps user keys to ids, with lookups batched 16 lines at a time and prefetched.
3. **Links first.** Links are parsed, then a lock-free union-find joins them (the smaller id becomes the root) and an atomic minimum names each person. The table is then rewritten so every linked ID maps straight to its person, which means events and clicks carry the person id from the moment they're parsed.
4. **Events.** Timestamps are parsed with SWAR plus a per-thread cache of calendar dates. Each copy is reduced to a 24-byte record (event key, ingest, person, ts, price/category) and scattered by an event-id hash into partitions made of pooled huge-page blocks.
5. **Dedup.** Each event partition is deduplicated with a small cache-resident hash table (smallest `ingest_ts` wins, and validity is that of the kept copy). Survivors and clicks are scattered by person. The consumed blocks are recycled for the output.
6. **Sessions.** Each person partition is counting-sorted by person, then by `ts`. Sessions, the credited click (latest `ts`, ties by smallest `click_id`) and the distinct assisting campaigns (a sliding 7-day window with per-campaign counts) are computed in one pass.
7. **Output.** A small Parquet writer (PLAIN, uncompressed, flat schema, `day` as DATE) writes one file per thread.

A global allocator serves large allocations from `mmap` with `MADV_HUGEPAGE`, which keeps page faults low. If the user table ever overflows its size estimate, the run restarts with a bigger table.

## Run and test

```sh
cargo build --release                       # target/release/sessions [DATA OUT]
python3 scripts/run_fixtures.py fixtures -- target/release/sessions
make build fixtures                         # the image, the scorer's flags
make dev run-dev check-dev                  # needs `generate` and dev-answer/
```

Logs (stage timings) go to stderr. Set `QUIET=1` to silence them and `THREADS=n` to override `NCPU`.
