# Shopping Sessions

**Shipped pipeline (`cpp/`, built by the Dockerfile):** a C++ pipeline adapted from
[Dremix10/shopping-sessions-fast](https://github.com/Dremix10/shopping-sessions-fast)
(commit `954c08d`; earlier `1fe68bc`, `8452f3a`), reused under the organizers' open-reuse policy for this buildathon.
All credit for its design goes to its author; our changes on top of it are in this
repository's history.

**Reference pipeline (`src/`, Rust):** our own independent implementation of `rules.md`.
CI diffs the shipped pipeline against it row by row on the 1M dev set and a generated
20M set on every push, and runs the 36 fixtures against both.


## Rust reference design

1. **Parse (parallel).** Every CSV is memory-mapped and cut into ~4 MB newline-aligned chunks (the dialect never puts a newline inside a value, so any `\n` is a safe cut). Worker threads parse events and clicks; events are routed into 256 partitions by a hash of `event_id`. Links are joined on their own thread at the same time (union-find; the byte-wise smallest ID names the person).
2. **Dedup (parallel over event_id partitions).** Open-addressing table keeps the copy with the smallest `ingest_ts`; validity of each copy is decided at parse time, so an invalid earliest copy still wins and drops the event. Survivors and clicks are re-routed into 256 partitions by person.
3. **Sessions (parallel over person partitions).** Sort by (person, ts), split at gaps over 30 min, credit each session to the latest click within 7 days (ties: smallest `click_id`) with a two-pointer sweep, and count assisting campaigns with a sliding window over distinct campaigns. Each worker writes its own `user_daily` Parquet file.

IDs of 1–10 chars from `[0-9A-Z_a-z]` are packed into order-preserving 60-bit integers; anything else is interned, so comparisons stay exact.

## Checks

```sh
cargo build --release
python3 scripts/run_fixtures.py fixtures -- target/release/shopping   # 36/36
DATA_DIR=dev OUTPUT_DIR=out target/release/shopping && python3 scripts/compare.py out dev-answer
```

`SHOP_PROFILE=1` prints phase timings to stderr.

## Practice-size data on a low-memory machine

The official generator holds the whole dataset in RAM before writing. `tools/gen-lowmem` is a copy that generates users in batches and appends each batch to the part files: every row lands in the same file as with the official generator, only the line order inside a file differs (which the rules make irrelevant; verified on the 1M dev set: identical files up to line order, and the pipeline output still matches the dev answer).

```powershell
cargo build --release --manifest-path tools/gen-lowmem/Cargo.toml
tools/gen-lowmem/target/release/generate --seed 42 --events 20000000 --clicks 5000000 --threads 8 --output D:/data/p20
powershell -File tools/bench.ps1 -Data D:\data\p20 -Exe target\release\shopping.exe -Runs 3
```
