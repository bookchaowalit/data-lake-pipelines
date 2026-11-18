#!/usr/bin/env python3
"""Jobs Gold → MotherDuck ``empire_gold`` (ADR-0016 P2).

After each Gold publish this replaces the jobs tables in the warehouse with the
marts of one Gold manifest. R2 Gold stays the system of record: the warehouse is
a rebuildable copy, and R2 wins on any disagreement.

- Only Gold leaves R2 (ADR-0016 §2). The manifest must be a jobs Gold manifest
  that lists exactly the allow-listed jobs marts, and every part must match the
  sha256 the manifest recorded.
- All tables are replaced in one transaction, then their row counts are compared
  with the manifest (the P2 done criterion). A mismatch rolls back.
- ``jobs._gold_loads`` records each load, so a rerun of the same Gold run that
  still matches is a no-op (``unchanged``) and costs no warehouse compute.
- With no target configured (no ``MOTHERDUCK_TOKEN``), the CLI reports
  ``skipped_no_token`` and exits 0, so the workflow can carry the step before
  the account exists.

- ``--require-latest`` (set by the scheduled job) skips a manifest that is no
  longer the newest Gold run (``skipped_not_latest``), so re-running an old
  workflow run cannot put older data back. Pass an old ``--manifest-key``
  without it to restore an earlier run on purpose.

Exit codes: 0 loaded, unchanged or skipped; 2 failed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.jobs_marts import GOLD_POINTER_PREFIX, GOLD_SCHEMA_VERSION, GOLD_SOURCE, JOBS_MARTS  # noqa: E402
from data_lake.product_adapter import default_data_lake_uri  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

GOLD_MANIFEST_PREFIX = f"control/manifests/gold/source={GOLD_SOURCE}/domain=jobs/"
WAREHOUSE_DATABASE = "empire_gold"
WAREHOUSE_SCHEMA = "jobs"
LOAD_LOG = "_gold_loads"
# Datasets cleared to leave R2 (ADR-0016 §2: jobs = public postings and aggregates).
ALLOWED_DATASETS = frozenset(JOBS_MARTS)


class WarehouseLoadError(RuntimeError):
    """Raised when the manifest, a part or the loaded tables cannot be trusted."""


def default_target() -> str:
    """``md:empire_gold`` when a MotherDuck token is set, else '' (nothing to load into)."""
    explicit = os.environ.get("JOBS_WAREHOUSE_TARGET", "").strip()
    if explicit:
        return explicit
    if os.environ.get("MOTHERDUCK_TOKEN", "").strip():
        return f"md:{WAREHOUSE_DATABASE}"
    return ""


def _connect(target: str):
    import duckdb  # type: ignore

    if target.startswith("md:"):
        # The motherduck extension reads MOTHERDUCK_TOKEN from the environment;
        # the token never appears in the connection string or in output.
        database = target[3:] or WAREHOUSE_DATABASE
        con = duckdb.connect("md:")
        con.execute(f'CREATE DATABASE IF NOT EXISTS "{database}"')
        con.execute(f'USE "{database}"')
        return con
    Path(target).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(target)


def _read_manifest(store: ObjectStore, manifest_key: str) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    if not (manifest_key.startswith(GOLD_MANIFEST_PREFIX) and manifest_key.endswith(".json")):
        raise WarehouseLoadError("not a jobs Gold manifest key")
    manifest = json.loads(store.get_bytes(manifest_key))
    marts = {mart["dataset"]: mart for mart in manifest.get("marts", [])}
    if manifest.get("layer") != "gold" or manifest.get("domain") != "jobs":
        raise WarehouseLoadError("manifest is not jobs Gold")
    if set(marts) != ALLOWED_DATASETS:
        raise WarehouseLoadError("manifest does not list exactly the allow-listed jobs marts")
    gold_run_id = str(manifest.get("gold_run_id", ""))
    for name, mart in marts.items():
        # Each table may only be filled from its own mart's part of this Gold run.
        expected_key = (
            f"gold/domain=jobs/dataset={name}/schema_version={GOLD_SCHEMA_VERSION}/"
            f"part-{gold_run_id}.parquet"
        )
        if mart.get("key") != expected_key:
            raise WarehouseLoadError(f"{name}: part key is not this mart's part of the Gold run")
    return manifest, marts


def _schema_ref(con) -> str:
    """``"<catalog>"."jobs"``: qualified so an attached database named ``jobs`` cannot shadow it."""
    catalog = con.execute("SELECT current_database()").fetchone()[0]
    return f'"{catalog}"."{WAREHOUSE_SCHEMA}"'


def _log_exists(con) -> bool:
    return bool(
        con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_catalog = current_database() AND table_schema = ? AND table_name = ?",
            [WAREHOUSE_SCHEMA, LOAD_LOG],
        ).fetchone()[0]
    )


def _already_loaded(con, ref: str, gold_run_id: str, marts: dict[str, dict[str, Any]]) -> bool:
    if not _log_exists(con):
        return False
    row = con.execute(f'SELECT gold_run_id FROM {ref}."{LOAD_LOG}" ORDER BY seq DESC LIMIT 1').fetchone()
    if not row or row[0] != gold_run_id:
        return False
    return _table_counts(con, ref, marts) == {name: int(mart["rows"]) for name, mart in marts.items()}


def _table_counts(con, ref: str, marts: dict[str, dict[str, Any]]) -> dict[str, int | None]:
    counts: dict[str, int | None] = {}
    for name in marts:
        try:
            counts[name] = int(con.execute(f'SELECT count(*) FROM {ref}."{name}"').fetchone()[0])
        except Exception:  # noqa: BLE001 - a missing table counts as a mismatch
            counts[name] = None
    return counts


def load_jobs_gold(
    manifest_key: str,
    target: str,
    *,
    data_lake_uri: str = "",
    published_key: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Replace the jobs tables in ``target`` with the marts of one Gold manifest.

    ``published_key`` is the Gold pointer key of this run (keys sort by publish
    time). When given, the load is refused inside its own transaction if the
    warehouse already holds a run published later, so a slow or concurrent
    load of an older run cannot replace newer tables. Two loads racing on the
    same tables conflict in the warehouse and one of them fails (exit 2).
    Without it (a deliberate restore) no order check is made.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    store = ObjectStore(default_data_lake_uri(data_lake_uri=data_lake_uri))
    manifest, marts = _read_manifest(store, manifest_key)
    gold_run_id = manifest["gold_run_id"]
    expected = {name: int(mart["rows"]) for name, mart in marts.items()}
    result: dict[str, Any] = {
        "layer": "warehouse",
        "warehouse": "motherduck" if target.startswith("md:") else "duckdb-file",
        "database": WAREHOUSE_DATABASE,
        "schema": WAREHOUSE_SCHEMA,
        "gold_run_id": gold_run_id,
        "manifest_key": manifest_key,
        "expected_rows": expected,
    }

    con = _connect(target)
    try:
        ref = _schema_ref(con)
        if _already_loaded(con, ref, gold_run_id, marts):
            return {**result, "status": "unchanged", "loaded_rows": expected}

        with tempfile.TemporaryDirectory(prefix="solo-empire-jobs-warehouse-") as temp_dir:
            workdir = Path(temp_dir)
            parts: dict[str, Path] = {}
            for name, mart in marts.items():
                data = store.get_bytes(mart["key"])
                if hashlib.sha256(data).hexdigest() != mart["sha256"]:
                    raise WarehouseLoadError(f"{name}: sha256 does not match the Gold manifest")
                parts[name] = workdir / f"{name}.parquet"
                parts[name].write_bytes(data)

            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {ref}")
                con.execute(
                    f'CREATE TABLE IF NOT EXISTS {ref}."{LOAD_LOG}" (seq BIGINT, gold_run_id VARCHAR, '
                    "manifest_key VARCHAR, published_key VARCHAR, rows_json VARCHAR, loaded_at TIMESTAMPTZ)"
                )
                if published_key:
                    newest = con.execute(f'SELECT max(published_key) FROM {ref}."{LOAD_LOG}"').fetchone()[0]
                    if newest and newest > published_key:
                        con.execute("ROLLBACK")
                        return {**result, "status": "skipped_not_latest"}
                for name, path in parts.items():
                    con.execute(
                        f'CREATE OR REPLACE TABLE {ref}."{name}" AS SELECT * FROM read_parquet(?)',
                        [str(path)],
                    )
                loaded = _table_counts(con, ref, marts)
                if loaded != expected:
                    raise WarehouseLoadError("loaded row counts do not match the Gold manifest")
                con.execute(
                    # seq orders loads even when two share a timestamp; it is written in the
                    # same transaction as the tables, so the newest row is what they hold.
                    f'INSERT INTO {ref}."{LOAD_LOG}" '
                    f'SELECT coalesce(max(seq), 0) + 1, ?, ?, ?, ?, ? FROM {ref}."{LOAD_LOG}"',
                    [gold_run_id, manifest_key, published_key, json.dumps(expected, sort_keys=True), now],
                )
                con.execute("COMMIT")
            except BaseException:
                con.execute("ROLLBACK")
                raise
        return {**result, "status": "loaded", "loaded_rows": loaded}
    finally:
        con.close()


def latest_pointer(*, data_lake_uri: str = "") -> tuple[str, str]:
    """(pointer key, manifest key) of the newest jobs Gold publish; pointer keys sort by publish time."""
    store = ObjectStore(default_data_lake_uri(data_lake_uri=data_lake_uri))
    pointers = sorted(key for key in store.list_keys(GOLD_POINTER_PREFIX) if key.endswith(".json"))
    if not pointers:
        raise WarehouseLoadError("no jobs Gold pointer found")
    return pointers[-1], json.loads(store.get_bytes(pointers[-1]))["manifest_key"]


def latest_manifest_key(*, data_lake_uri: str = "") -> str:
    """Newest jobs Gold manifest, found through the publish-time pointer."""
    return latest_pointer(data_lake_uri=data_lake_uri)[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--manifest-key", default="", help="jobs Gold manifest (default: newest pointer)")
    parser.add_argument("--target", default="", help="md:<db> or a local DuckDB file (default: md:empire_gold when MOTHERDUCK_TOKEN is set)")
    parser.add_argument("--data-lake-uri", default="")
    parser.add_argument(
        "--require-latest",
        action="store_true",
        help="skip (skipped_not_latest) unless --manifest-key is the newest jobs Gold run; "
        "the scheduled job sets it so a re-run of an old run cannot overwrite newer tables",
    )
    parser.add_argument("--json", action="store_true", help="print the result as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    target = args.target or default_target()
    if not target:
        result: dict[str, Any] = {"status": "skipped_no_token", "layer": "warehouse"}
        code = 0
    else:
        try:
            # A given --manifest-key without --require-latest is a restore: the pointer
            # is not read, so it works even when no pointer is readable.
            published_key: str | None = None
            manifest_key = args.manifest_key
            if args.require_latest or not manifest_key:
                published_key, latest = latest_pointer(data_lake_uri=args.data_lake_uri)
                manifest_key = manifest_key or latest
            if args.require_latest and manifest_key != latest:
                result = {"status": "skipped_not_latest", "layer": "warehouse"}
            else:
                result = load_jobs_gold(
                    manifest_key,
                    target,
                    data_lake_uri=args.data_lake_uri,
                    published_key=published_key if args.require_latest else None,
                )
            code = 0
        except Exception as exc:  # noqa: BLE001 - reported as a failed load
            result = {"status": "failed", "layer": "warehouse", "error": f"{type(exc).__name__}: {exc}"}
            code = 2
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(f"warehouse: {result['status']}")
        for name, rows in (result.get("loaded_rows") or {}).items():
            print(f"  {name}: {rows} rows")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
