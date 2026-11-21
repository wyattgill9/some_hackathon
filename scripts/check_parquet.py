#!/usr/bin/env python3
"""Reads every table under OUT with polars, pyarrow and duckdb and prints schema and row count from each.
Usage: python3 scripts/check_parquet.py OUT"""
import sys
from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq

out = Path(sys.argv[1])
for table in ("user_daily", "campaign_daily"):
    files = sorted(p for p in (out / table).glob("*.parquet") if not p.name.startswith((".", "_")))
    a = pl.concat([pl.read_parquet(f) for f in files])
    b = sum(pq.read_table(f).num_rows for f in files)
    c = duckdb.sql(f"select count(*) from read_parquet({[str(f) for f in files]!r})").fetchone()[0]
    print(f"{table}: {len(files)} files; polars {a.height} rows {a.schema}; pyarrow {b} rows; duckdb {c} rows")
    assert a.height == b == c, "readers disagree"
