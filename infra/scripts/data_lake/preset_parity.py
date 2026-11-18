#!/usr/bin/env python3
"""ADR-0016 P3: do the Preset dashboard datasets show the numbers jobs-insights shows?

The Preset jobs dashboard is built in the Preset UI (the Starter plan has no API) on
the SQL datasets in ``infra/bi/preset/jobs/datasets/``. After each MotherDuck load
this runs those same SQL files against the warehouse and compares the result with
what the static jobs-insights site computes from the same Gold run
(``jobs_bi_snapshot.py`` + ``jobs_bi_site.py``):

- the overview tiles (postings, last 90 days, last 7 days, boards, companies);
- postings per board over the 90-day window;
- the last complete week's top 20 tags;
- every company with its 30-day and all-time postings, in page order.

Each check appends one row to ``jobs._preset_parity`` in the warehouse. P3 is met
when every check for 14 days in a row matched (``p3_parity_met``). A warehouse
that does not yet hold the manifest's Gold run is skipped, not counted.

Output names only which comparisons differ, never the values (public CI log).
Exit codes: 0 match or skipped, 2 mismatch or failure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.jobs_bi_site import (
    Snapshot,
    load_snapshot,
    overview_kpis,
    skills_last_week,
)  # noqa: E402
from data_lake.jobs_bi_snapshot import build_bi_snapshot  # noqa: E402
from data_lake.motherduck_load import LOAD_LOG, _connect, _schema_ref, default_target  # noqa: E402

DATASETS_DIR = ROOT / "infra" / "bi" / "preset" / "jobs" / "datasets"
DATASETS = (
    "jobs_kpis",
    "jobs_daily_90d",
    "jobs_skills_last_week",
    "jobs_companies_30d",
)
PARITY_LOG = "_preset_parity"
PARITY_DAYS = 14


class ParityError(RuntimeError):
    """Raised when the check itself cannot run."""


def dataset_sql(name: str) -> str:
    if name not in DATASETS:
        raise ParityError(f"{name} is not a Preset jobs dataset")
    return (DATASETS_DIR / f"{name}.sql").read_text(encoding="utf-8")


def _rows(con: Any, sql: str) -> list[dict[str, Any]]:
    cursor = con.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def site_values(snapshot: Snapshot) -> dict[str, Any]:
    """What jobs-insights shows, from the site's own functions."""
    totals: dict[str, int] = {}
    for row in snapshot.daily:
        totals[row["board"]] = totals.get(row["board"], 0) + int(row["postings"])
    last_week, top = skills_last_week(snapshot)
    return {
        "as_of_date": snapshot.meta["as_of_date"],
        "kpis": overview_kpis(snapshot),
        "board_totals_90d": totals,
        "skills_last_week": [
            last_week,
            [(int(r["demand_rank"]), r["tag"], int(r["postings"])) for r in top],
        ],
        "companies": [
            (r["company"], int(r["postings_30d"]), int(r["postings_total"]))
            for r in snapshot.companies
        ],
    }


def warehouse_values(con: Any) -> dict[str, Any]:
    """The same numbers from the Preset dataset SQL."""
    kpi = _rows(con, dataset_sql("jobs_kpis"))[0]
    totals: dict[str, int] = {}
    for row in _rows(con, dataset_sql("jobs_daily_90d")):
        totals[row["board"]] = totals.get(row["board"], 0) + int(row["postings"])
    skills = _rows(con, dataset_sql("jobs_skills_last_week"))
    as_of = kpi["as_of_date"]
    last_week = (
        as_of - timedelta(days=as_of.weekday() + 7) if as_of is not None else None
    )
    if skills and any(row["posted_week"] != last_week for row in skills):
        raise ParityError(
            "jobs_skills_last_week returned a week other than the last complete one"
        )
    return {
        "as_of_date": as_of,
        "kpis": {
            key: int(kpi[key])
            for key in (
                "postings",
                "postings_90d",
                "postings_7d",
                "boards",
                "companies",
            )
        },
        "board_totals_90d": totals,
        "skills_last_week": [
            last_week,
            [(int(r["demand_rank"]), r["tag"], int(r["postings"])) for r in skills],
        ],
        "companies": [
            (r["company"], int(r["postings_30d"]), int(r["postings_total"]))
            for r in _rows(con, dataset_sql("jobs_companies_30d"))
        ],
    }


def compare(site: dict[str, Any], warehouse: dict[str, Any]) -> list[str]:
    """Names of the comparisons that differ (``kpis.<tile>`` per tile)."""
    diffs = [
        f"kpis.{key}"
        for key in site["kpis"]
        if site["kpis"][key] != warehouse["kpis"].get(key)
    ]
    diffs += [
        key
        for key in ("as_of_date", "board_totals_90d", "skills_last_week", "companies")
        if site[key] != warehouse[key]
    ]
    return diffs


def _warehouse_run(con: Any, ref: str) -> str | None:
    exists = con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = 'jobs' AND table_name = ?",
        [LOAD_LOG],
    ).fetchone()[0]
    if not exists:
        return None
    row = con.execute(
        f'SELECT gold_run_id FROM {ref}."{LOAD_LOG}" ORDER BY seq DESC LIMIT 1'
    ).fetchone()
    return row[0] if row else None


def _record(
    con: Any, ref: str, gold_run_id: str, diffs: list[str], now: datetime
) -> dict[str, Any]:
    con.execute(
        f'CREATE TABLE IF NOT EXISTS {ref}."{PARITY_LOG}" ('
        "seq BIGINT, gold_run_id VARCHAR, checked_at TIMESTAMPTZ, ok BOOLEAN, mismatches_json VARCHAR)"
    )
    con.execute(
        f'INSERT INTO {ref}."{PARITY_LOG}" SELECT coalesce(max(seq), 0) + 1, ?, ?, ?, ? FROM {ref}."{PARITY_LOG}"',
        [gold_run_id, now, not diffs, json.dumps(diffs)],
    )
    # The streak starts at the first matching check after the newest mismatch.
    since = con.execute(
        f'SELECT min(checked_at) FROM {ref}."{PARITY_LOG}" WHERE ok AND seq > '
        f'coalesce((SELECT max(seq) FROM {ref}."{PARITY_LOG}" WHERE NOT ok), 0)'
    ).fetchone()[0]
    if diffs or since is None:
        return {"matching_since": None, "matching_days": 0, "p3_parity_met": False}
    days = (now - since).total_seconds() / 86400
    return {
        "matching_since": since.isoformat(),
        "matching_days": int(days),
        "p3_parity_met": days >= PARITY_DAYS,
    }


def check_parity(
    manifest_key: str,
    target: str,
    *,
    data_lake_uri: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    with tempfile.TemporaryDirectory(prefix="jobs-preset-parity-") as temp_dir:
        snapshot_path = Path(temp_dir) / "jobs.duckdb"
        built = build_bi_snapshot(
            manifest_key, snapshot_path, data_lake_uri=data_lake_uri
        )
        site = site_values(load_snapshot(snapshot_path))
    gold_run_id = built["gold_run_id"]
    con = _connect(target)
    try:
        ref = _schema_ref(con)
        held = _warehouse_run(con, ref)
        if held != gold_run_id:
            # The load was skipped (older run, no token yet) or failed: nothing to compare.
            return {"status": "skipped_run_mismatch", "gold_run_id": gold_run_id}
        diffs = compare(site, warehouse_values(con))
        streak = _record(con, ref, gold_run_id, diffs, now)
    finally:
        con.close()
    return {
        "status": "mismatch" if diffs else "match",
        "gold_run_id": gold_run_id,
        "mismatches": diffs,
        **streak,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument(
        "--target",
        default=None,
        help="md:<database> or a local DuckDB file (default: md:empire_gold)",
    )
    parser.add_argument(
        "--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", "")
    )
    args = parser.parse_args(argv)
    try:
        target = args.target or default_target()
        result = check_parity(
            args.manifest_key, target, data_lake_uri=args.data_lake_uri
        )
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        print(json.dumps({"status": "failed", "error": type(exc).__name__}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 2 if result["status"] == "mismatch" else 0


if __name__ == "__main__":
    raise SystemExit(main())
