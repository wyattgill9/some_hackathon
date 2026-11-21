#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB starter (skeleton).

This runs end to end inside the scorer's container and writes both output tables
with the exact schema, but the tables are EMPTY, so the checker will mark it
incorrect. It gives you the plumbing; the rules are yours to implement.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import json
import os

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

con = duckdb.connect()  # in-memory database
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")  # leave headroom for Python
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
print("window:", manifest["window_start"], "to", manifest["window_end"])

events_glob = os.path.join(DATA, "events", "*.csv")
clicks_glob = os.path.join(DATA, "clicks", "*.csv")

# TODO: implement the rules.
#   Events: parse -> dedup on event_id (smallest ingest_ts, before validation)
#           -> validate -> drop late -> sessionize per user (gap > 30 min)
#           -> day of session start -> categories
#   Clicks: parse -> dedup -> validate -> credit each session to the latest
#           click at or before its start, within 7 days (ties: smallest click_id)
#
# Careful with DuckDB's CSV reader: it repairs or rejects malformed lines in its
# own way, and it reads a quoted empty field "" as '' rather than null. Check its
# behaviour against the fixtures before trusting it with the dialect rules.

# Write both tables with the exact schema the checker expects.
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
con.execute(f"""
    COPY (
        SELECT NULL::VARCHAR AS user_id, NULL::DATE AS day, NULL::BIGINT AS sessions,
               NULL::BIGINT AS events, NULL::BIGINT AS revenue_cents, NULL::VARCHAR AS top_category
        WHERE false
    ) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")
con.execute(f"""
    COPY (
        SELECT NULL::VARCHAR AS campaign_id, NULL::VARCHAR AS channel, NULL::DATE AS day,
               NULL::BIGINT AS sessions, NULL::BIGINT AS purchasing_sessions, NULL::BIGINT AS revenue_cents,
               NULL::BIGINT AS assisted_sessions
        WHERE false
    ) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
print("wrote empty user_daily and campaign_daily (skeleton)")
