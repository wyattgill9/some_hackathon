# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Databricks Buildathon entry for "Shopping Sessions": clean dirty click-stream CSVs in `/data`, merge linked user IDs into people, sessionize, attribute sessions to ad clicks, and write two Parquet tables to `/output/user_daily/` and `/output/campaign_daily/`. **The score is container wall time (lower is better), and any wrong row scores nothing.** Correctness comes first; after that, everything is about speed.

`rules.md` is the spec, and it overrides the starters wherever they disagree. Read it in full before you touch pipeline logic. The edge cases are deliberate, and the fixtures plus the generator's dirt table cover them.

## Repo state

So far the repo holds only `rules.md` and the challenge zips. **No engine has been chosen yet.**
- `starter-{duckdb,polars,pyspark}.zip`: skeletons that write empty tables. Each has a `Dockerfile`, `run.sh`, `pipeline/pipeline.py`, `Makefile`, `scripts/` and the 36 `fixtures/`. The `scripts/` and `fixtures/` are identical across the three starters.
- `generator.zip`: Rust source for `generate`, which builds datasets of any size and seed.
- `dev-answer.zip`: the expected CSVs for the dev set (seed 1, 1M events, 250k clicks).

Submission requirements: a public GitHub repo with a `Dockerfile` **at the repo root**, at most 50 MB, and no data checked in. Never commit `dev/`, `out/`, generated datasets or `target/`. Every submitted commit must stay in the history until results are out.

## Local environment (NixOS)

`python3`, `unzip`, `docker`, `cargo` and `duckdb` are **not** on PATH, and there is no `NIX_PATH` channel, so `nix-shell -p` needs `-I nixpkgs=flake:nixpkgs`:

```sh
nix run nixpkgs#unzip -- -qo starter-duckdb.zip -d .                                    # extract a starter
nix-shell -I nixpkgs=flake:nixpkgs -p 'python3.withPackages(p: [p.polars p.duckdb])'   # test scripts need polars
nix-shell -I nixpkgs=flake:nixpkgs -p cargo rustc --run 'cargo build --release'        # build the generator (Rust >= 1.85)
```

nixpkgs ships polars 1.42 and duckdb 1.5.5. The starters pin `polars==2.1.0` and `duckdb==1.5.6`, so confirm any engine behavior you rely on inside the Docker image.

## Commands (from the starter Makefile/scripts, once a starter is extracted to the root)

```sh
make build                     # docker buildx build --platform linux/amd64 -t shopping-sessions:latest .
make fixtures                  # runs the image on all 36 fixtures with scorer flags, then diffs
make dev                       # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                   # timed run on dev/ -> out/   (CPUS=4 MEM=8g to lower limits)
make check-dev ANSWER=dev-answer   # exact diff of out/ vs unzipped dev-answer

# Without Docker: all fixtures, or one fixture
python3 scripts/run_fixtures.py fixtures -- <cmd>     # cmd gets DATA_DIR/OUTPUT_DIR as env AND as the last two args
DATA_DIR=fixtures/E16/input OUTPUT_DIR=out <cmd> && python3 scripts/compare.py out fixtures/E16/expected
```

`compare.py` diffs any two folders. Each side can be `<table>.csv` files or `<table>/*.parquet` dirs. Integer columns may be any width, and `day` must be a Parquet `DATE`. `scripts/docker_run.sh DATA OUT` reproduces the scorer's exact `docker run` flags.

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
