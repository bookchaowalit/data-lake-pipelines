#!/usr/bin/env python3
"""Run the DuckDB example queries in queries/ against the raw layer.

    python -m x_lakehouse.run_queries                       # X_LAKE_URI (r2://bucket/prefix)
    python -m x_lakehouse.run_queries --lake file:///tmp/x-lake 03_topic_count_spikes

For r2:// the R2 secret is created in memory from R2_ACCOUNT_ID / R2_ACCESS_KEY_ID /
R2_SECRET_ACCESS_KEY (DuckDB httpfs, S3 API). Nothing is written.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import urlsplit

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

import duckdb  # noqa: E402

from .config import Config  # noqa: E402

QUERIES = Path(__file__).resolve().parent / "queries"


def connect(cfg: Config, lake: str) -> tuple[duckdb.DuckDBPyConnection, str]:
    con = duckdb.connect()
    con.execute("set timezone = 'Asia/Bangkok'")
    parts = urlsplit(lake)
    if parts.scheme == "r2":
        con.execute("install httpfs; load httpfs")
        con.execute(
            "create secret x_lake_r2 (type r2, key_id ?, secret ?, account_id ?)",
            [cfg.require("R2_ACCESS_KEY_ID"), cfg.require("R2_SECRET_ACCESS_KEY"), cfg.require("R2_ACCOUNT_ID")],
        )
        return con, f"r2://{parts.netloc}/{parts.path.strip('/')}".rstrip("/")
    if parts.scheme == "file":
        return con, (parts.netloc + parts.path).rstrip("/")
    raise SystemExit("--lake must be r2://bucket[/prefix] or file:///dir")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("names", nargs="*", help="query file stems (default: all)")
    p.add_argument("--lake", help="override X_LAKE_URI")
    p.add_argument("--limit", type=int, default=12)
    args = p.parse_args(argv)
    cfg = Config()
    con, root = connect(cfg, args.lake or cfg.require("X_LAKE_URI"))
    files = sorted(QUERIES.glob("*.sql"))
    if args.names:
        files = [f for f in files if f.stem in args.names]
    for f in files:
        sql = f.read_text().replace("{lake}", root)
        rel = con.sql(sql)
        print(f"\n== {f.stem}")
        print(rel.limit(args.limit))
    return 0


if __name__ == "__main__":
    sys.exit(main())
