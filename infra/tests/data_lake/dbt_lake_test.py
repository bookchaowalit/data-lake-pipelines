#!/usr/bin/env python3
"""
dbt-duckdb project smoke test.

infra/dbt/ reads the real local Bronze lake (data/lake), which is real
personal data that is intentionally never committed to git, so it never
exists in a clean checkout or CI. This test proves the dbt project itself
(models, manifest-aware input, schema tests, and config) is structurally
correct by pointing it at a small synthetic Bronze fixture instead.

Usage:
    python3 infra/tests/data_lake/dbt_lake_test.py

Exit code 0 = pass, 1 = failure.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = ROOT / "infra" / "dbt"


def resolve_dbt_bin() -> Path | None:
    on_path = shutil.which("dbt")
    if on_path:
        return Path(on_path)
    for venv_dbt in (
        ROOT / ".venv" / "bin" / "dbt",
        ROOT / ".venv" / "Scripts" / "dbt.exe",
        ROOT / "infra" / ".venv-datalake" / "bin" / "dbt",
    ):
        if venv_dbt.exists():
            return venv_dbt
    return None


DBT_BIN = resolve_dbt_bin()


def build_synthetic_bronze(root: Path) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("pyarrow required for this test") from exc

    now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(5):
        received = now - timedelta(hours=i * 6)
        rows.append(
            {
                "event_id": f"fixture-event-{i}",
                "source": "fixture",
                "source_record_id": f"msg-{i}",
                "domain": "communications",
                "dataset": "emails",
                "schema_version": "1",
                "received_at": received.isoformat(),
                "event_time": received.isoformat(),
                "content_type": "application/json",
                "raw_object_key": f"fixture/msg-{i}.json",
                "raw_sha256": "0" * 64,
                "payload_json": json.dumps(
                    {
                        "id": f"msg-{i}",
                        "thread_id": f"thread-{i % 2}",
                        "from_address": "fixture@example.invalid",
                        "from_name": "Fixture Sender",
                        "subject": "Synthetic fixture - not real content",
                        "snippet": "synthetic",
                        "to_addresses": ["nobody@example.invalid"],
                        "is_read": i % 2 == 0,
                        "is_important": i == 0,
                        "has_attachment": i % 3 == 0,
                        "labels": ["INBOX"],
                        "source": "fixture",
                        "received_at": received.isoformat(),
                    }
                ),
                "metadata_json": "{}",
                "ingest_run_id": "fixture-run",
                "privacy_class": "private",
                "retention_class": "operational",
                "ingest_date": received.date().isoformat(),
            }
        )

    table = pa.Table.from_pylist(rows)
    part_dir = (
        root
        / "domain=communications"
        / "dataset=emails"
        / "schema_version=1"
        / f"ingest_date={now.date().isoformat()}"
        / "source=fixture"
    )
    part_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = part_dir / "part-fixture.parquet"
    pq.write_table(table, parquet_path)

    bronze_key = parquet_path.relative_to(root.parent).as_posix()
    manifest_path = (
        root.parent
        / "control"
        / "manifests"
        / "source=fixture"
        / "batch_id=fixture.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "source": "fixture",
                "batch_id": "fixture",
                "domain": "communications",
                "dataset": "emails",
                "schema_version": "1",
                "record_count": len(rows),
                "bronze": {
                    "key": bronze_key,
                    "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def build_synthetic_x_feed(lake_root: Path) -> None:
    """Twenty fake X sightings through the real lake writer, plus a re-export with one changed row."""
    sys.path.insert(0, str(ROOT / "infra" / "scripts" / "utils"))
    sys.path.insert(0, str(ROOT / "infra" / "scripts"))
    sys.path.insert(0, str(ROOT / "infra" / "tests" / "fixtures"))
    from data_lake.ingest import ingest_records
    from x_feed_rows import x_feed_rows

    rows = x_feed_rows("test-pull-dbt")
    for batch in (rows, [{**rows[0], "likes": 999}, *rows[1:]]):
        ingest_records(
            batch,
            source="x-feed",
            domain="social",
            dataset="x_feed",
            data_lake_uri=str(lake_root),
            source_record_id_field="sighting_key",
            event_time_field="collected_at",
            metadata={"adapter": "dbt_lake_test", "contract": "x_feed.v1"},
        )


def run() -> bool:
    if DBT_BIN is None:
        message = "dbt not on PATH (install infra/requirements-data-lake.txt)"
        if os.environ.get("SOLO_EMPIRE_CI") == "1":
            print(f"FAIL: {message}; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
            return False
        print(f"SKIP: {message}")
        return True

    with tempfile.TemporaryDirectory() as tmp:
        bronze_root = Path(tmp) / "bronze"
        build_synthetic_bronze(bronze_root)
        build_synthetic_x_feed(Path(tmp))
        target_dir = Path(tmp) / "target"
        # dbt-duckdb does not create the target directory itself before
        # opening the database file there.
        target_dir.mkdir(parents=True, exist_ok=True)

        result = subprocess.run(
            [
                str(DBT_BIN),
                "build",
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--target-path",
                str(target_dir),
                "--vars",
                json.dumps(
                    {
                        "bronze_root": str(bronze_root),
                        "bronze_manifest_root": str(
                            Path(tmp) / "control" / "manifests"
                        ),
                    }
                ),
            ],
            cwd=str(DBT_PROJECT_DIR),
            capture_output=True,
            text=True,
        )
        print(result.stdout[-4000:])
        if result.returncode != 0:
            print(result.stderr[-2000:])
            print("FAIL: dbt build against synthetic fixture failed")
            return False

        if "ERROR=0" not in result.stdout:
            print("FAIL: dbt reported one or more model/test errors")
            return False

        print("PASS: dbt build and quality tests succeeded against a synthetic Bronze fixture")
        return True


if __name__ == "__main__":
    success = run()
    print("\nResult:", "PASS" if success else "FAIL")
    sys.exit(0 if success else 1)
