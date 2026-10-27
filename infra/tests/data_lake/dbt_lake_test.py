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

sys.path.insert(0, str(ROOT / "infra" / "scripts"))
REGISTRY_FIXTURES = ROOT / "infra" / "tests" / "fixtures" / "registry_lake_export"
METADATA_MODELS = (
    "stg_meta_data_sources",
    "stg_meta_systems",
    "stg_meta_catalog_assets",
    "stg_meta_lineage_edges",
    "meta_lake_datasets",
    "meta_source_lineage",
    "meta_source_coverage",
)


def export_registry_fixture(lake_root: Path) -> None:
    from data_lake.registry_lake_export import export

    export(
        REGISTRY_FIXTURES / "registries",
        REGISTRY_FIXTURES / "metadata-catalog",
        lake_root.as_uri(),
    )


def check_metadata_outputs(target_dir: Path) -> list[str]:
    """Problems with the metadata models' results and the generated dbt docs."""
    import duckdb

    problems: list[str] = []
    manifest = json.loads((target_dir / "manifest.json").read_text(encoding="utf-8"))
    built = {node["name"] for node in manifest["nodes"].values() if node["resource_type"] == "model"}
    problems += [f"model {name} missing from dbt docs manifest" for name in METADATA_MODELS if name not in built]
    if not (target_dir / "catalog.json").is_file():
        problems.append("dbt docs generate wrote no catalog.json")

    with duckdb.connect(str(DBT_PROJECT_DIR / "local.duckdb"), read_only=True) as con:
        rows = con.execute(
            "select source_id, automation, system_registered, catalog_assets, consumers "
            "from meta_source_coverage order by source_id"
        ).fetchall()
        expected = [
            ("bank-accounts", "not-captured", False, 0, 0),
            ("health-checkin", "automated", False, 0, 0),
            ("job-capture", "automated", True, 1, 1),
        ]
        if rows != expected:
            problems.append(f"meta_source_coverage: expected {expected}, got {rows}")
        datasets = con.execute(
            "select count(*) from meta_lake_datasets where domain = 'metadata'"
        ).fetchone()
        if datasets is None or datasets[0] != 4:
            problems.append(f"meta_lake_datasets: expected 4 metadata datasets, got {datasets}")
    return problems


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
        export_registry_fixture(Path(tmp))
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

        docs = subprocess.run(
            [
                str(DBT_BIN),
                "docs",
                "generate",
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
        if docs.returncode != 0:
            print(docs.stdout[-2000:])
            print("FAIL: dbt docs generate failed")
            return False

        problems = check_metadata_outputs(target_dir)
        for problem in problems:
            print(f"FAIL: {problem}")
        if problems:
            return False

        print("PASS: dbt build, quality tests, metadata models and dbt docs succeeded against synthetic fixtures")
        return True


if __name__ == "__main__":
    success = run()
    print("\nResult:", "PASS" if success else "FAIL")
    sys.exit(0 if success else 1)
