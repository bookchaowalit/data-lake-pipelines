#!/usr/bin/env python3
"""Sales Gold → one DuckDB file for the revenue page (sales_bi_site.py, infra/bi/sales).

Same contract as jobs_bi_snapshot.py: take one sales Gold manifest, check every
mart part against the sha256 the manifest recorded, and load the marts plus a
one-row ``gold_meta`` table into a fresh DuckDB file (a build input only).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.sales_marts import GOLD_MANIFEST_PREFIX, SALES_MARTS  # noqa: E402
from data_lake.product_adapter import default_data_lake_uri  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

DEFAULT_TARGET = ROOT / "infra" / "bi" / "sales" / "sales.duckdb"


class BiSnapshotError(RuntimeError):
    """Raised when the manifest or a mart part cannot be trusted."""


def build_bi_snapshot(manifest_key: str, target: Path, *, data_lake_uri: str = "") -> dict[str, Any]:
    import duckdb  # type: ignore

    if not (manifest_key.startswith(GOLD_MANIFEST_PREFIX) and manifest_key.endswith(".json")):
        raise BiSnapshotError(f"{manifest_key} is not a sales Gold manifest")
    store = ObjectStore(default_data_lake_uri(data_lake_uri=data_lake_uri))
    manifest = json.loads(store.get_bytes(manifest_key))
    marts = {mart["dataset"]: mart for mart in manifest.get("marts", [])}
    if manifest.get("layer") != "gold" or set(marts) != set(SALES_MARTS):
        raise BiSnapshotError(f"{manifest_key} does not list exactly the sales marts")

    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="solo-empire-sales-bi-") as temp_dir:
        workdir = Path(temp_dir)
        for name, mart in marts.items():
            data = store.get_bytes(mart["key"])
            if hashlib.sha256(data).hexdigest() != mart["sha256"]:
                raise BiSnapshotError(f"{name}: sha256 does not match the Gold manifest")
            (workdir / f"{name}.parquet").write_bytes(data)

        staging = target.with_name(f".{target.name}.partial-{os.getpid()}")
        staging.unlink(missing_ok=True)
        try:
            con = duckdb.connect(str(staging))
            try:
                for name in SALES_MARTS:
                    con.execute(
                        f"create table {name} as select * from read_parquet(?)",
                        [str(workdir / f"{name}.parquet")],
                    )
                con.execute(
                    """
                    create table gold_meta as
                    select ? as gold_run_id,
                        (select max(business_date) from sales_daily_revenue) as as_of_date,
                        (select max(received_at) from fact_sales) as max_received_at
                    """,
                    [manifest["gold_run_id"]],
                )
            finally:
                con.close()
            os.replace(staging, target)
        finally:
            staging.unlink(missing_ok=True)
            staging.with_name(staging.name + ".wal").unlink(missing_ok=True)

    return {
        "gold_run_id": manifest["gold_run_id"],
        "marts": {name: mart["rows"] for name, mart in marts.items()},
        "target": str(target),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--target", type=Path, default=DEFAULT_TARGET)
    parser.add_argument("--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", ""))
    args = parser.parse_args()
    try:
        result = build_bi_snapshot(args.manifest_key, args.target, data_lake_uri=args.data_lake_uri)
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        print(json.dumps({"status": "failed", "error": type(exc).__name__}))
        return 2
    print(json.dumps({"status": "success", **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
