#!/usr/bin/env python3
"""Jobs Silver → dbt marts → Gold, as one bounded, replayable run.

Each Silver run is a full snapshot, so the marts must be built from exactly
one Silver part. This runner builds (or re-finds) the Silver snapshot of the
current Bronze, copies that one part to a scratch directory, runs the dbt jobs
models against it in a scratch DuckDB file, and publishes every mart as an
immutable Gold Parquet part plus one Gold manifest. A rerun over the same
Bronze, models and writer versions writes nothing new.

DuckDB stays a compute layer: its file is thrown away after the run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.product_adapter import default_data_lake_uri  # noqa: E402
from data_lake.silver import (  # noqa: E402
    SilverProductContract,
    _canonical_json,
    _pyarrow,
    transform_bronze_to_silver,
)
from data_lake.storage import ObjectStore  # noqa: E402

DBT_PROJECT_DIR = ROOT / "infra" / "dbt"
GOLD_SCHEMA_VERSION = "1"
GOLD_SOURCE = "book-job-data"
# Mart name → columns that order its rows, so exported bytes are stable.
JOBS_MARTS: dict[str, tuple[str, ...]] = {
    "jobs_postings_daily": ("posted_date", "board"),
    "jobs_board_health": ("board",),
    "jobs_skills_demand": ("posted_week", "demand_rank", "tag"),
    "jobs_company_hiring": ("company_key",),
}
DBT_SELECTOR = "+path:models/marts/jobs"


class JobsMartsError(RuntimeError):
    """Raised when dbt fails or the marts cannot be published."""


def resolve_dbt_bin() -> Path | None:
    on_path = shutil.which("dbt")
    if on_path:
        return Path(on_path)
    for candidate in (ROOT / ".venv" / "bin" / "dbt", ROOT / ".venv" / "Scripts" / "dbt.exe"):
        if candidate.exists():
            return candidate
    return None


def _model_fingerprint() -> str:
    """Hash of every file that shapes the marts, so a model change gets a new run."""
    digest = hashlib.sha256()
    paths = sorted(
        [
            *(DBT_PROJECT_DIR / "models" / "marts" / "jobs").glob("*"),
            DBT_PROJECT_DIR / "models" / "staging" / "stg_job_postings.sql",
            DBT_PROJECT_DIR / "macros" / "unique_combination.sql",
            DBT_PROJECT_DIR / "tests" / "jobs_marts_quality.sql",
        ]
    )
    for path in paths:
        digest.update(path.relative_to(DBT_PROJECT_DIR).as_posix().encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _writer_versions() -> dict[str, str]:
    import duckdb  # type: ignore

    pa, _ = _pyarrow()
    return {"duckdb": duckdb.__version__, "pyarrow": pa.__version__}


def _run_dbt(silver_part: Path, workdir: Path) -> dict[str, Any]:
    dbt = resolve_dbt_bin()
    if dbt is None:
        raise JobsMartsError("dbt not found (install the data-lake extras: uv sync --all-extras)")
    target = workdir / "target"
    target.mkdir()
    env = {**os.environ, "DBT_DUCKDB_PATH": str(workdir / "marts.duckdb")}
    result = subprocess.run(
        [
            str(dbt),
            "build",
            "--select",
            DBT_SELECTOR,
            "--profiles-dir",
            str(DBT_PROJECT_DIR),
            "--project-dir",
            str(DBT_PROJECT_DIR),
            "--target-path",
            str(target),
            "--log-path",
            str(workdir / "logs"),
            "--vars",
            json.dumps({"jobs_silver_part": str(silver_part)}),
        ],
        cwd=str(DBT_PROJECT_DIR),
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        tail = (result.stdout[-3000:] + result.stderr[-1000:]).strip()
        raise JobsMartsError(f"dbt build failed:\n{tail}")
    run_results = json.loads((target / "run_results.json").read_text(encoding="utf-8"))
    statuses: dict[str, int] = {}
    for item in run_results.get("results", []):
        statuses[item["status"]] = statuses.get(item["status"], 0) + 1
    return {"database": workdir / "marts.duckdb", "statuses": statuses}


def _export_mart(database: Path, mart: str, order_by: tuple[str, ...], destination: Path) -> int:
    import duckdb  # type: ignore

    _, pq = _pyarrow()
    con = duckdb.connect(str(database), read_only=True)
    try:
        table = con.sql(f"select * from {mart} order by {', '.join(order_by)}").to_arrow_table()
    finally:
        con.close()
    pq.write_table(table, destination, compression="zstd", version="2.6")
    return table.num_rows


def _freshness(database: Path, now: datetime, stale_hours: float) -> dict[str, Any]:
    import duckdb  # type: ignore

    con = duckdb.connect(str(database), read_only=True)
    try:
        newest = con.sql("select max(last_seen_at) from jobs_board_health").fetchall()[0][0]
    finally:
        con.close()
    if newest is None:
        return {"max_seen_at": None, "age_hours": None, "stale": True, "stale_hours": stale_hours}
    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=timezone.utc)
    age = (now - newest).total_seconds() / 3600
    return {
        "max_seen_at": newest.isoformat(),
        "age_hours": round(age, 2),
        "stale": age > stale_hours,
        "stale_hours": stale_hours,
    }


def run_jobs_marts(
    *,
    data_lake_uri: str = "",
    max_invalid_ratio: float = 0.02,
    stale_hours: float = 24.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build Silver for the current Bronze, then the jobs marts, then Gold."""
    uri = default_data_lake_uri(data_lake_uri=data_lake_uri)
    silver = transform_bronze_to_silver(
        SilverProductContract(max_invalid_ratio=max_invalid_ratio),
        data_lake_uri=uri,
    )
    store = ObjectStore(uri)
    gold_run_id = "gold-" + hashlib.sha256(
        _canonical_json(
            {
                "silver_run_id": silver["silver_run_id"],
                "models": _model_fingerprint(),
                "writers": _writer_versions(),
            }
        ).encode("utf-8")
    ).hexdigest()[:24]

    with tempfile.TemporaryDirectory(prefix="solo-empire-jobs-marts-") as temp_dir:
        workdir = Path(temp_dir)
        silver_part = workdir / "silver.parquet"
        silver_part.write_bytes(store.get_bytes(silver["silver_key"]))
        dbt_result = _run_dbt(silver_part, workdir)
        database = dbt_result["database"]
        freshness = _freshness(database, now or datetime.now(timezone.utc), stale_hours)

        marts: dict[str, dict[str, Any]] = {}
        for mart, order_by in JOBS_MARTS.items():
            local = workdir / f"{mart}.parquet"
            rows = _export_mart(database, mart, order_by, local)
            key = (
                f"gold/domain=jobs/dataset={mart}/schema_version={GOLD_SCHEMA_VERSION}/"
                f"part-{gold_run_id}.parquet"
            )
            put = store.put_bytes(key, local.read_bytes(), content_type="application/vnd.apache.parquet")
            marts[mart] = {"key": put.key, "sha256": put.sha256, "rows": rows, "existed": put.existed}

    manifest = {
        "manifest_version": "1",
        "layer": "gold",
        "source": GOLD_SOURCE,
        "domain": "jobs",
        "schema_version": GOLD_SCHEMA_VERSION,
        "gold_run_id": gold_run_id,
        "input": {
            "layer": "silver",
            "silver_run_id": silver["silver_run_id"],
            "silver_key": silver["silver_key"],
            "record_count": silver["record_count"],
        },
        "marts": [
            {"dataset": name, "key": mart["key"], "sha256": mart["sha256"], "rows": mart["rows"]}
            for name, mart in marts.items()
        ],
        "dbt": {"selector": DBT_SELECTOR, "statuses": dbt_result["statuses"]},
        "privacy_class": "internal",
    }
    manifest_key = (
        f"control/manifests/gold/source={GOLD_SOURCE}/domain=jobs/"
        f"gold_run_id={gold_run_id}.json"
    )
    manifest_put = store.put_bytes(
        manifest_key,
        (_canonical_json(manifest) + "\n").encode("utf-8"),
        content_type="application/json",
    )
    quality = silver["quality"]
    return {
        "status": "stale" if freshness["stale"] else "success",
        "layer": "gold",
        "data_lake": store.describe(),
        "silver_run_id": silver["silver_run_id"],
        "gold_run_id": gold_run_id,
        "silver": {
            "key": silver["silver_key"],
            "existed": silver["silver_existed"],
            "record_count": silver["record_count"],
            "invalid_count": quality.get("invalid_count", 0),
            "excluded_count": quality.get("excluded_count", 0),
            "rejects_key": silver["rejects_key"],
        },
        "marts": marts,
        "manifest_key": manifest_put.key,
        "manifest_existed": manifest_put.existed,
        "dbt": manifest["dbt"],
        "freshness": freshness,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", ""))
    parser.add_argument("--max-invalid-ratio", type=float, default=0.02)
    parser.add_argument("--stale-hours", type=float, default=24.0)
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run_jobs_marts(
            data_lake_uri=args.data_lake_uri,
            max_invalid_ratio=args.max_invalid_ratio,
            stale_hours=args.stale_hours,
        )
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        payload = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(f"Jobs marts: FAILED — {payload['error']}", file=sys.stderr)
        return 2
    if args.as_json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print(f"Jobs marts: {result['status'].upper()}")
        print(f"  Silver: {result['silver']['record_count']} rows ({result['silver_run_id']})")
        for name, mart in result["marts"].items():
            print(f"  {name}: {mart['rows']} rows → {mart['key']}")
        print(f"  manifest: {result['manifest_key']}")
        print(f"  freshness: {result['freshness']}")
    # Stale data is still published (it is correct for its input), but the
    # exit code lets the scheduler's heartbeat report it.
    return 3 if result["status"] == "stale" else 0


if __name__ == "__main__":
    raise SystemExit(main())
