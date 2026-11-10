"""Gold on R2: the DuckDB queries in queries/ run over the raw lake and land as Parquet (serving audit 2026-11-10,
gap 8: "R2 queries only by hand").

Each cron run with ``X_LAKE_GOLD=write`` writes, per query, one immutable part

    gold/<query>/as_of=<ICT date>/run=<HHMMSS>.parquet

and one manifest ``_manifests/gold/<run id>.json`` (keys and row counts), through the same write-once store as
raw (never overwrites). Readers take the newest ``run=`` of the newest ``as_of=``. ``04_rows_per_partition`` is
the ops check and lands too. A failing query is logged by name and exception type and the other queries still
land; nothing here changes the lake run's result (scheduled.py).

    python -m x_lakehouse.gold            # dry run: row counts only
    python -m x_lakehouse.gold --write    # what the cron does with X_LAKE_GOLD=write
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

from .coerce import TZ  # noqa: E402
from .config import Config  # noqa: E402
from .lake import Store, _join, open_store, parquet_bytes  # noqa: E402
from .run_queries import QUERIES, connect, render  # noqa: E402


def run(cfg: Config, *, write: bool, now: datetime | None = None, store: Store | None = None,
        prefix: str | None = None, lake: str | None = None) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(TZ)
    # Parts and manifest share ONE stamp: a second run that would reuse a part name also reuses the manifest name,
    # and the write-once store refuses that manifest before any part, so a manifest never claims another run's parts.
    as_of, stamp = now.date().isoformat(), now.strftime("%H%M%S")
    run_id = f"gold-{now:%Y%m%dT}{stamp}"
    if write and store is None:
        store, prefix = open_store(cfg)
    con, root = connect(cfg, lake or cfg.require("X_LAKE_URI"))
    results: dict[str, Any] = {}
    tables: dict[str, Any] = {}
    try:
        for f in sorted(QUERIES.glob("*.sql")):
            try:
                tables[f.stem] = con.sql(render(f.read_text(), root, now)).to_arrow_table()
            except Exception as exc:  # noqa: BLE001 - one bad query must not stop the others
                results[f.stem] = {"status": "error", "error": type(exc).__name__}
                continue
            results[f.stem] = {"status": "ok", "rows": tables[f.stem].num_rows}
    finally:
        con.close()
    if write and tables:
        assert store is not None
        parts = [{"query": q, "key": _join(prefix or "", "gold", q, f"as_of={as_of}", f"run={stamp}.parquet"),
                  "rows": t.num_rows} for q, t in tables.items()]
        # Write-ahead, like raw: the manifest names every part before any is published, so a crash part-way
        # leaves a manifest whose missing keys show the run did not finish, never an unlisted part.
        body = {"run_id": run_id, "as_of": as_of, "written_at": now.isoformat(), "parts": parts}
        store.put_new(_join(prefix or "", "_manifests", "gold", f"{run_id}.json"),
                      json.dumps(body, sort_keys=True).encode())
        for part in parts:
            store.put_new(part["key"], parquet_bytes(tables[part["query"]]))
            results[part["query"]]["key"] = part["key"]
    ok = all(r["status"] == "ok" for r in results.values())
    return {"run_id": run_id, "as_of": as_of, "write": write, "ok": ok, "queries": results}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--write", action="store_true", help="land the results on the lake (default: counts only)")
    args = ap.parse_args(argv)
    report = run(Config(), write=args.write)
    for r in report["queries"].values():
        r.pop("key", None)  # counts only on stdout
    print(json.dumps(report, sort_keys=True))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
