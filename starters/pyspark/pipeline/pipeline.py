#!/usr/bin/env python3
"""
Shopping Sessions: PySpark starter (skeleton).

This runs end to end inside the scorer's container and writes both output tables
with the exact schema, but the tables are EMPTY, so the checker will mark it
incorrect. It gives you the plumbing (a Spark session that works under the
scorer's limits); the rules are yours to implement.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import glob
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import types as T

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

spark = (
    SparkSession.builder.master(f"local[{NCPU}]")
    .appName("shopping-sessions")
    .config("spark.driver.memory", f"{max(1, int(MEM_GB * 0.75))}g")
    .config("spark.local.dir", SCRATCH)
    .config("spark.sql.shuffle.partitions", str(NCPU * 4))
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.jars.ivy", os.path.join(SCRATCH, ".ivy2"))
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
print("window:", manifest["window_start"], "to", manifest["window_end"])

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
# Careful with Spark's CSV reader: it splits lines on \r as well as \n, and it
# repairs or skips malformed lines in its own way, which is not the spec's way.
# Reading lines with spark.read.option("lineSep", "\n").text(...) and splitting
# them yourself gives you full control. Check everything against the fixtures.

USER_DAILY = T.StructType([
    T.StructField("user_id", T.StringType()), T.StructField("day", T.DateType()),
    T.StructField("sessions", T.LongType()), T.StructField("events", T.LongType()),
    T.StructField("revenue_cents", T.LongType()), T.StructField("top_category", T.StringType()),
])
CAMPAIGN_DAILY = T.StructType([
    T.StructField("campaign_id", T.StringType()), T.StructField("channel", T.StringType()),
    T.StructField("day", T.DateType()), T.StructField("sessions", T.LongType()),
    T.StructField("purchasing_sessions", T.LongType()), T.StructField("revenue_cents", T.LongType()),
    T.StructField("assisted_sessions", T.LongType()),
])

# Write both tables with the exact schema the checker expects.
for name, schema in (("user_daily", USER_DAILY), ("campaign_daily", CAMPAIGN_DAILY)):
    spark.createDataFrame([], schema).write.mode("overwrite").parquet(os.path.join(OUT, name))
print("wrote empty user_daily and campaign_daily (skeleton)")
spark.stop()
