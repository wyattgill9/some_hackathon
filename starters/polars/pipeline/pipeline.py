#!/usr/bin/env python3
"""
Shopping Sessions: Polars starter (skeleton).

This runs end to end inside the scorer's container and writes both output tables
with the exact schema, but the tables are EMPTY, so the checker will mark it
incorrect. It gives you the plumbing; the rules are yours to implement.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import glob
import json
import os

NCPU = os.environ.get("NCPU")
if NCPU:
    # Must be set before polars is imported.
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import polars as pl  # noqa: E402

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
print("window:", manifest["window_start"], "to", manifest["window_end"])
print("polars threads:", pl.thread_pool_size())

event_files = sorted(glob.glob(os.path.join(DATA, "events", "*.csv")))
click_files = sorted(glob.glob(os.path.join(DATA, "clicks", "*.csv")))
print(f"{len(event_files)} event files, {len(click_files)} click files")

# TODO: implement the rules.
#   Events: parse -> dedup on event_id (smallest ingest_ts, before validation)
#           -> validate -> drop late -> sessionize per user (gap > 30 min)
#           -> day of session start -> categories
#   Clicks: parse -> dedup -> validate -> credit each session to the latest
#           click at or before its start, within 7 days (ties: smallest click_id)
#
# Careful with Polars' CSV reader: it handles malformed lines and quoting its own
# way, which is not necessarily the spec's way. Check it against the fixtures.
# Lazy frames (pl.scan_csv / LazyFrame.collect(engine="streaming")) help with
# memory at full size; the container has about 24 GB and no swap.

USER_DAILY = {
    "user_id": pl.String, "day": pl.Date, "sessions": pl.Int64,
    "events": pl.Int64, "revenue_cents": pl.Int64, "top_category": pl.String,
}
CAMPAIGN_DAILY = {
    "campaign_id": pl.String, "channel": pl.String, "day": pl.Date,
    "sessions": pl.Int64, "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64,
    "assisted_sessions": pl.Int64,
}

# Write both tables with the exact schema the checker expects.
for name, schema in (("user_daily", USER_DAILY), ("campaign_daily", CAMPAIGN_DAILY)):
    folder = os.path.join(OUT, name)
    os.makedirs(folder, exist_ok=True)
    pl.DataFrame(schema=schema).write_parquet(os.path.join(folder, "part-0.parquet"))
print("wrote empty user_daily and campaign_daily (skeleton)")
