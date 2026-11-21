# Event pipeline (C++)

One static C++ binary, no runtime dependencies, hand-written Parquet writer.

- `src/main.cpp`: the fast path. SIMD-indexed CSV parse, radix partitioning by
  event id for dedup and by person for sessions, per-thread Parquet output.
- `src/fallback.cpp`: a slower, fully general pipeline used only for inputs the
  fast path's compact formats cannot hold exactly.

Build and test without Docker:

    ./build.sh
    python3 scripts/run_fixtures.py fixtures -- ./build/sessions      # 36/36
    ./build/sessions dev out && python3 scripts/compare.py out dev-answer   # MATCH

`VERBOSE=1` prints per-phase timings to stderr.
