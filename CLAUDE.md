# CLAUDE.md

## What this is

A Databricks Buildathon entry for "Shopping Sessions": clean dirty click-stream CSVs in `/data`, merge linked user IDs into people, sessionize, attribute sessions to ad clicks, and write two Parquet tables to `/output/user_daily/` and `/output/campaign_daily/`. **The score is container wall time (lower is better), and any wrong row scores nothing.** Correctness comes first; after that, everything is about speed.

`rules.md` is the spec, and it overrides the starters wherever they disagree. Read it in full before you touch pipeline logic. The edge cases are deliberate, and the fixtures plus the generator's dirt table cover them.

## Repo state

**Engine: one zero-dependency Rust binary**, static, in a `FROM scratch` image. The `Dockerfile` is a profile-guided build tuned for the scorer (`target-cpu=cascadelake`, JCC branch alignment): it compiles the generator from `generator/`, trains an instrumented binary on a 2M-event set and recompiles with the profile, falling back to a plain build if any step fails. Local builds use `.cargo/config.toml` (`x86-64-v3`). Optimize for `perf stat` instructions/cycles and branch misses, not local wall time: the scorer's Cascade Lake cores are instruction-bound (board time ≈ 2.3× local on a Zen 5). Phases (`src/main.rs`): `reader.rs` (256 KiB `pread` ranges; `scan.rs` AVX2 index of `\n , "`; field walk) → `links.rs` (parallel CAS union-find; (id, name) table prefetched 16 lines ahead) → parse into hash partitions through `pool.rs` (16 KiB blocks on huge pages, filled through one staging line per partition and non-temporal stores; events as two parallel 16-byte streams by event_id, `EvKey` (eid, ingest) and the `Ev2` payload, pushed as a pair; 24-byte `Ck` by person) → dedup per partition (the key stream once through a table of 64-byte lines of 15 tag|position slots, losers to a bitmap; the payload pass copies survivors by person into blocks recycled from the consumed input) → `person.rs` per person partition (counting sorts by 30-minute bin then person; sessions from bin boundaries, no sort; attribution; per-worker `user_daily` file) → `campaign_daily`. `key.rs` packs IDs into order-preserving u64s (interned fallback), `ts.rs` has the timestamp fast paths, `parse.rs` the exact slow paths, `parquet.rs` the writer. It logs per-phase timings and the self-check to stderr. `competitors/TECHNIQUES.md` lists where the design comes from.

`ref/` is the **frozen first implementation**, the oracle: `cargo build --release --manifest-path ref/Cargo.toml`, then `scripts/diff_vs_ref.sh "2 3 4 5 15" 1000000` diffs the pipeline against it on generated seeds (NCPU 1/3/8, `SS_RANGE`, `SS_PBITS` overrides). Never edit `ref/`.
- `Makefile`, `scripts/`, `fixtures/` (the 36 cases), `.dockerignore`: the shared starter harness. It is identical in all three starters.
- `starters/{duckdb,polars,pyspark}/`: each starter's engine-specific `Dockerfile`, `run.sh`, `pipeline/`, `requirements.txt` and README. The pyspark `run.sh`/README holds the JVM-in-container workarounds. Reference only; not used.
- `generator/`: Rust source for `generate` (std only). The flake builds it with plain `rustc` because its `Cargo.lock` is stale.
- `dev-answer/`: the expected CSVs for the dev set (seed 1, 1M events, 250k clicks).

Submission requirements: a public GitHub repo with a `Dockerfile` **at the repo root**, at most 50 MB, and no data checked in. Never commit `dev/`, `out/`, generated datasets or `target/`. Every submitted commit must stay in the history until results are out.

## Local environment (NixOS)

Use the flake dev shell (`direnv allow`, or `nix develop`). It provides `generate`, `make` and `python3` with polars, which is all the harness needs. Outside it, `python3`, `make` and `docker` are not on PATH. Docker needs a system-level daemon (`virtualisation.docker.enable`), so a flake can't supply it. The shell also has `cargo`, `rustc` and `clippy`. Flakes see only git-tracked files, so `git add` new files before `nix develop` can find them.

nixpkgs ships polars 1.42. The starters pin `polars==2.1.0` and `duckdb==1.5.6`, so confirm any engine behavior you rely on inside the Docker image.

## Commands (`build`, `fixtures` and `run-dev` need Docker and a root `Dockerfile`)

```sh
make build                     # docker buildx build --platform linux/amd64 -t shopping-sessions:latest .
make fixtures                  # runs the image on all 36 fixtures with scorer flags, then diffs
make dev                       # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                   # timed run on dev/ -> out/   (CPUS=4 MEM=8g to lower limits)
make check-dev ANSWER=dev-answer   # exact diff of out/ vs unzipped dev-answer

# Native (no Docker): build, unit tests, fixtures, dev diff, reference diffs, benchmark (4 cores + SMT, like the scorer)
cargo build --release && cargo test --release
python3 scripts/run_fixtures.py fixtures -- target/release/pipeline
rm -rf out-dev && DATA_DIR=dev OUTPUT_DIR=out-dev target/release/pipeline && python3 scripts/compare.py out-dev dev-answer
scripts/diff_vs_ref.sh "2 3 4 5 15" 1000000
scripts/bench.sh ~/data/ss20m 5
python3 scripts/check_parquet.py out-dev        # the output through polars, pyarrow and duckdb

# Without Docker: all fixtures, or one fixture
python3 scripts/run_fixtures.py fixtures -- <cmd>     # cmd gets DATA_DIR/OUTPUT_DIR as env AND as the last two args
DATA_DIR=fixtures/E16/input OUTPUT_DIR=out <cmd> && python3 scripts/compare.py out fixtures/E16/expected
```

`compare.py` diffs any two folders. Each side can be `<table>.csv` files or `<table>/*.parquet` dirs. Integer columns may be any width, and `day` must be a Parquet `DATE`. `scripts/docker_run.sh DATA OUT` reproduces the scorer's exact `docker run` flags. Without a Docker daemon, rootless podman works: `nix shell nixpkgs#podman`, put a `docker` shim (`exec podman "$@"`) first on PATH, and build with `podman build --signature-policy <file with {"default":[{"type":"insecureAcceptAnything"}]}>` and `CONTAINERS_REGISTRIES_CONF` set to a file with `unqualified-search-registries = ["docker.io"]`.

Larger local sets live outside the repo in `~/data/` (`ss20m` seed 2, `ss60m` seed 3, `ssfeb29` seed 15, whose window holds Feb 29, 2028); `~/data/<set>.ref` holds the oracle's output for them, so `compare.py` can diff the pipeline at full size. Container runs leave files owned by a mapped uid: clean with `podman unshare rm -rf`.

## Scorer / container constraints

- linux/amd64 on Intel Xeon (Cascade Lake+, AVX-512), 8 vCPUs, 24 GB RAM, no swap. Use `NCPU`/`MEM_GB` from the env.
- At run time: no network, a read-only root FS, uid 1000, and only `/output`, `/scratch` (also `HOME`) and a 64 MB `/tmp` are writable. Point every temp/cache dir at `/scratch`. Log to stderr; stdout is capped at 50 MB.
- The clock runs from container start to exit, so engine startup (JVM, Python imports) counts.
- Practice data is ~20M events; the final is ~60M with a 3-minute hard cap per run (over the cap = no score). Aim for under ~60 s on practice. The baseline to beat is 172 s.
- 2 submissions/hour. Hidden runs return only the exit code, OOM status and missing/extra/differing row counts, with no logs. Validate locally first.

## Pipeline order and traps (rules.md is authoritative)

Order: **links** (union-find, person = byte-wise smallest ID) → **events**: parse → dedup on `event_id` keeping min `ingest_ts` *before* validation → validate → drop late (`ingest_ts - ts > 24h`) → sessionize per person (gap > 1,800,000 ms) → day = UTC date of session start → categories. **Clicks**: parse → dedup on `click_id` → validate (click window is `window_start - 7d`) → credit each session to the latest click with `0 <= start - click.ts <= 7d` (tie: smallest `click_id`), else `organic` → assists = every distinct campaign with a click in that same range.

Traps that off-the-shelf CSV readers and regexes get wrong (every starter README warns that DuckDB, Polars and Spark readers all diverge from the dialect):
- Only `\n` ends a line. A `\r` belongs to the last field and makes that field invalid wherever it is validated.
- Quotes are allowed only around a whole field. `""` is null (DuckDB reads it as `''`). Any other quote, a wrong field count, or an empty line makes the line malformed. No value ever contains a comma.
- Strings are never trimmed or case-folded. ` u1`, `u1 ` and `U1` are all distinct IDs.
- "Digits" means ASCII `0-9` only (`\d` in Rust/Polars regex matches Unicode digits). F3 timestamps may have leading zeros or overflow i64; both are "out of window", not malformed.
- Timestamps come as F1 (`...Z`), F2 (`±HH:MM` offset with hours ≤23 and minutes ≤59) or F3 (epoch ms). Calendar validity matters (Feb 29 in leap years, no hour 24 or second 60). Datasets may span Feb 29, 2028.
- Events that share a `ts` always share a session, so compare distinct timestamps when computing gaps.
- A row equal to the header line is data (fixture L3).
- Shared-device links merge thousands of people into one giant component, and chains run hundreds of links deep. Union-find needs path compression and must not recurse.
- The input file count and sizes vary per dataset (Zipf sizes, some header-only files). Never hard-code the window, whales or file counts; `manifest.json` gives the window.

Self-check: summed `sessions` and `revenue_cents` must be equal across the two tables.
