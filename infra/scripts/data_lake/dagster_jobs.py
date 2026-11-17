#!/usr/bin/env python3
"""Jobs domain as Dagster software-defined assets with asset checks (ADR-0016 P1).

``jobs_marts.run_jobs_marts`` stays the one bounded, replayable run that builds
Silver, the dbt marts, the Gold manifest and the pointer. This module declares
what that run produces as Dagster assets (Bronze is external, written by
career-job-scraper), attaches asset checks to them, and runs it through Dagster.

There is no Dagster webserver or daemon: each scheduled run materializes the
assets in-process on an ephemeral instance, prints the same JSON and exits with
the same codes as ``jobs_marts.py`` (0 success, 3 stale, 2 failed), so the
workflow, its heartbeat and the BI job do not change. The asset graph is written
as lineage JSON to ``control/lineage/dagster/domain=jobs/`` (write-once, keyed by
its digest) for the catalog.

``defs`` is a normal Definitions object, so ``dagster asset materialize -m
data_lake.dagster_jobs --select '*'`` works locally too; ``dagster dev`` also needs
``--with dagster-webserver==1.13.28`` (local only, not in the lock).
"""

# No `from __future__ import annotations`: Dagster reads the Config annotation at run time.
import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

# No usage reporting from scheduled runs; set before dagster is imported.
os.environ.setdefault("DAGSTER_DISABLE_TELEMETRY", "1")

try:
    import dagster as dg
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise SystemExit(
        "dagster is not installed; run `uv sync --extra orchestration` (pyproject.toml)"
    ) from exc

from data_lake.jobs_marts import (  # noqa: E402
    ENVELOPE_DATASETS,
    HISTORY_DATASETS,
    JOBS_MARTS,
    run_jobs_marts,
)
from data_lake.product_adapter import default_data_lake_uri  # noqa: E402
from data_lake.silver import _canonical_json  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

DOMAIN = "jobs"
GROUP = "jobs"
LINEAGE_VERSION = "1"
LINEAGE_PREFIX = f"control/lineage/dagster/domain={DOMAIN}/"
BRONZE_OWNER = "career-job-scraper"
# Dataset produced by dbt from Silver job_postings; the envelope/history Silver
# datasets are read by the API only (best-effort, see jobs_marts.py).
MAIN_DATASET = "job_postings"
SIDE_DATASETS = ENVELOPE_DATASETS + HISTORY_DATASETS
# dbt result statuses that are not a failure (statuses are counted per status).
DBT_OK = {"success", "pass", "warn", "skipped", "no-op"}


def _key(layer: str, name: str) -> dg.AssetKey:
    return dg.AssetKey([DOMAIN, layer, name])


BRONZE_KEYS = {name: _key("bronze", name) for name in (MAIN_DATASET, *SIDE_DATASETS)}
SILVER_KEY = _key("silver", MAIN_DATASET)
SIDE_SILVER_KEYS = {name: _key("silver", name) for name in SIDE_DATASETS}
MART_KEYS = {mart: _key("gold", mart) for mart in JOBS_MARTS}
MANIFEST_KEY = _key("gold", "manifest")
POINTER_KEY = _key("gold", "pointer")

bronze_specs = [
    dg.AssetSpec(
        key,
        group_name=GROUP,
        description=f"Bronze {name} batches landed by {BRONZE_OWNER} (external to this run).",
        kinds={"r2", "parquet"},
        owners=[f"team:{BRONZE_OWNER}"],
        metadata={"layer": "bronze", "privacy_class": "internal"},
    )
    for name, key in BRONZE_KEYS.items()
]

lake_specs = [
    dg.AssetSpec(
        SILVER_KEY,
        deps=[BRONZE_KEYS[MAIN_DATASET]],
        group_name=GROUP,
        description="Full Silver snapshot of the current Bronze job postings (quality-gated).",
        kinds={"r2", "parquet"},
        metadata={"layer": "silver", "privacy_class": "internal"},
    ),
    *[
        dg.AssetSpec(
            key,
            deps=[BRONZE_KEYS[name]],
            group_name=GROUP,
            description=(
                f"Silver {name} for the API; best-effort, a failure falls back to Bronze."
            ),
            kinds={"r2", "parquet"},
            skippable=True,
            metadata={"layer": "silver", "privacy_class": "internal"},
        )
        for name, key in SIDE_SILVER_KEYS.items()
    ],
    *[
        dg.AssetSpec(
            key,
            deps=[SILVER_KEY],
            group_name=GROUP,
            description=f"Gold mart {mart} (dbt-duckdb model marts/jobs/{mart}).",
            kinds={"dbt", "duckdb", "parquet"},
            metadata={"layer": "gold", "privacy_class": "internal", "dbt_model": mart},
        )
        for mart, key in MART_KEYS.items()
    ],
    dg.AssetSpec(
        MANIFEST_KEY,
        deps=list(MART_KEYS.values()),
        group_name=GROUP,
        description="Gold manifest that links the marts to their Silver input.",
        kinds={"r2", "json"},
        metadata={"layer": "gold", "privacy_class": "internal"},
    ),
    dg.AssetSpec(
        POINTER_KEY,
        deps=[MANIFEST_KEY, *SIDE_SILVER_KEYS.values()],
        group_name=GROUP,
        description="Sortable pointer readers use to find the newest Gold run.",
        kinds={"r2", "json"},
        metadata={"layer": "gold", "privacy_class": "internal"},
    ),
]

check_specs = [
    dg.AssetCheckSpec(
        "quarantine_ratio",
        asset=SILVER_KEY,
        description="Invalid Bronze rows stay within --max-invalid-ratio (the run fails before Gold otherwise).",
    ),
    *[
        dg.AssetCheckSpec("has_rows", asset=key, description="The mart is not empty.")
        for key in MART_KEYS.values()
    ],
    dg.AssetCheckSpec(
        "dbt_build_ok",
        asset=MANIFEST_KEY,
        description="Every dbt model and test in the jobs selector succeeded.",
    ),
    dg.AssetCheckSpec(
        "fresh",
        asset=POINTER_KEY,
        description="The newest posting is younger than --stale-hours (stale Gold is still published).",
    ),
    dg.AssetCheckSpec(
        "side_silver_built",
        asset=POINTER_KEY,
        description="Every envelope/history Silver dataset was built for the API.",
    ),
]


class JobsMartsConfig(dg.Config):
    data_lake_uri: str = ""
    max_invalid_ratio: float = 0.02
    stale_hours: float = 24.0
    # ISO-8601 clock override for replays and tests; empty means the current time.
    now: str = ""


def _check(
    asset: dg.AssetKey, name: str, passed: bool, metadata: dict[str, Any]
) -> dg.AssetCheckResult:
    # WARN, not ERROR: the exit code (0/3/2) stays what jobs_marts.py returns, so the
    # heartbeat is unchanged; the check results are added to the JSON for the summary.
    return dg.AssetCheckResult(
        asset_key=asset,
        check_name=name,
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        metadata=metadata,
    )


def materialization_events(
    result: dict[str, Any], max_invalid_ratio: float
) -> Iterator[Any]:
    """Turn one run_jobs_marts result into Dagster materializations and check results."""
    silver = result["silver"]
    total = silver["record_count"] + silver["invalid_count"]
    ratio = silver["invalid_count"] / total if total else 0.0
    yield dg.MaterializeResult(
        asset_key=SILVER_KEY,
        metadata={
            "key": silver["key"],
            "silver_run_id": result["silver_run_id"],
            "existed": silver["existed"],
            "dagster/row_count": silver["record_count"],
            "invalid_count": silver["invalid_count"],
            "excluded_count": silver["excluded_count"],
        },
    )
    yield _check(
        SILVER_KEY,
        "quarantine_ratio",
        ratio <= max_invalid_ratio,
        {"ratio": round(ratio, 6), "max": max_invalid_ratio},
    )

    built_side: list[str] = []
    for name, key in SIDE_SILVER_KEYS.items():
        built = result["envelope_silver"].get(name, {})
        if "error" in built:
            continue
        built_side.append(name)
        yield dg.MaterializeResult(
            asset_key=key,
            metadata={
                "key": built["silver_key"],
                "silver_run_id": built["silver_run_id"],
                "dagster/row_count": built["record_count"],
            },
        )

    for mart, key in MART_KEYS.items():
        built = result["marts"][mart]
        yield dg.MaterializeResult(
            asset_key=key,
            metadata={
                "key": built["key"],
                "sha256": built["sha256"],
                "existed": built["existed"],
                "dagster/row_count": built["rows"],
                "gold_run_id": result["gold_run_id"],
            },
        )
        yield _check(key, "has_rows", built["rows"] > 0, {"rows": built["rows"]})

    statuses = result["dbt"]["statuses"]
    yield dg.MaterializeResult(
        asset_key=MANIFEST_KEY,
        metadata={
            "key": result["manifest_key"],
            "existed": result["manifest_existed"],
            "gold_run_id": result["gold_run_id"],
        },
    )
    failed = {status: count for status, count in sorted(statuses.items()) if status not in DBT_OK}
    yield _check(
        MANIFEST_KEY, "dbt_build_ok", not failed, {"failed": failed}
    )

    yield dg.MaterializeResult(
        asset_key=POINTER_KEY,
        metadata={
            "key": result["pointer_key"],
            "existed": result["pointer_existed"],
            "gold_run_id": result["gold_run_id"],
            # The full run result, read back by run() for the CLI's JSON output.
            "run_result": dg.MetadataValue.json(result),
        },
    )
    yield _check(
        POINTER_KEY,
        "fresh",
        not result["freshness"].get("stale", False),
        {key: str(value) for key, value in result["freshness"].items()},
    )
    missing = sorted(set(SIDE_DATASETS) - set(built_side))
    yield _check(POINTER_KEY, "side_silver_built", not missing, {"missing": missing})


@dg.multi_asset(
    name="jobs_lake",
    specs=lake_specs,
    check_specs=check_specs,
    description="One jobs_marts.py run: Silver → dbt marts → Gold manifest → pointer.",
)
def jobs_lake(
    context: dg.AssetExecutionContext, config: JobsMartsConfig
) -> Iterator[Any]:
    result = run_jobs_marts(
        data_lake_uri=config.data_lake_uri,
        max_invalid_ratio=config.max_invalid_ratio,
        stale_hours=config.stale_hours,
        now=datetime.fromisoformat(config.now) if config.now else None,
    )
    context.log.info(
        f"jobs marts {result['status']} gold_run_id={result['gold_run_id']}"
    )
    yield from materialization_events(result, config.max_invalid_ratio)


defs = dg.Definitions(assets=[*bronze_specs, jobs_lake])


def lineage_document() -> dict[str, Any]:
    """The asset graph as a stable JSON document (no run data, so it changes only with code)."""
    graph = defs.resolve_asset_graph()
    nodes = []
    for key in sorted(graph.get_all_asset_keys(), key=lambda k: k.to_user_string()):
        node = graph.get(key)
        nodes.append(
            {
                "key": key.to_user_string(),
                "layer": key.path[1],
                "group": node.group_name,
                "kinds": sorted(node.kinds),
                "external": not node.is_materializable,
                "upstream": sorted(
                    parent.to_user_string() for parent in node.parent_keys
                ),
                "checks": sorted(check.name for check in node.check_keys),
                "description": node.description or "",
            }
        )
    return {
        "lineage_version": LINEAGE_VERSION,
        "producer": "dagster",
        "dagster_version": dg.__version__,
        "domain": DOMAIN,
        "assets": nodes,
    }


def publish_lineage(data_lake_uri: str) -> dict[str, Any]:
    body = (_canonical_json(lineage_document()) + "\n").encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()[:16]
    store = ObjectStore(default_data_lake_uri(data_lake_uri=data_lake_uri))
    put = store.put_bytes(
        f"{LINEAGE_PREFIX}graph-{digest}.json", body, content_type="application/json"
    )
    return {"key": put.key, "existed": put.existed}


def _error_text(run: dg.ExecuteInProcessResult) -> str:
    for event in run.all_events:
        if (
            event.is_step_failure
            and event.step_failure_data
            and event.step_failure_data.error
        ):
            error = event.step_failure_data.error
            # The user code error is the cause; Dagster wraps it in its own error.
            while error.cause is not None and (error.cls_name or "").startswith("Dagster"):
                error = error.cause
            return f"{error.cls_name}: {error.message.strip()}"
    return "DagsterRunFailed: run did not succeed"


def run(
    *,
    data_lake_uri: str = "",
    max_invalid_ratio: float = 0.02,
    stale_hours: float = 24.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Materialize the jobs assets once, in-process, and return the jobs_marts.py result."""
    config: dict[str, Any] = {
        "data_lake_uri": data_lake_uri,
        "max_invalid_ratio": max_invalid_ratio,
        "stale_hours": stale_hours,
        "now": now.isoformat() if now else "",
    }
    executed = dg.materialize(
        [*bronze_specs, jobs_lake],
        instance=dg.DagsterInstance.ephemeral(),
        run_config=dg.RunConfig(ops={"jobs_lake": JobsMartsConfig(**config)}),
        raise_on_error=False,
    )
    if not executed.success:
        return {"status": "failed", "error": _error_text(executed)}
    result: dict[str, Any] | None = None
    for materialization in executed.asset_materializations_for_node("jobs_lake"):
        if materialization.asset_key == POINTER_KEY:
            result = dict(materialization.metadata["run_result"].value)  # type: ignore[union-attr, arg-type]
    if result is None:
        return {
            "status": "failed",
            "error": "DagsterRunFailed: no pointer materialization",
        }
    checks = {
        f"{evaluation.asset_key.to_user_string()}:{evaluation.check_name}": evaluation.passed
        for evaluation in executed.get_asset_check_evaluations()
    }
    result["checks"] = dict(sorted(checks.items()))
    result["orchestrator"] = {
        "name": "dagster",
        "version": dg.__version__,
        "run_id": executed.run_id,
    }
    # Best-effort: Gold is already published, so a lineage write failure is reported, not fatal.
    try:
        result["lineage"] = publish_lineage(data_lake_uri)
    except Exception as exc:  # noqa: BLE001 - reported in the JSON
        result["lineage"] = {"error": f"{type(exc).__name__}: {exc}"}
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", "")
    )
    parser.add_argument("--max-invalid-ratio", type=float, default=0.02)
    parser.add_argument("--stale-hours", type=float, default=24.0)
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument(
        "--print-lineage",
        action="store_true",
        help="print the lineage JSON and exit (no lake access)",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.print_lineage:
        print(
            json.dumps(lineage_document(), ensure_ascii=False, indent=2, sort_keys=True)
        )
        return 0
    try:
        result = run(
            data_lake_uri=args.data_lake_uri,
            max_invalid_ratio=args.max_invalid_ratio,
            stale_hours=args.stale_hours,
        )
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        result = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
    if args.as_json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    elif result["status"] == "failed":
        print(f"Jobs assets: FAILED — {result['error']}", file=sys.stderr)
    else:
        failed = [name for name, passed in result["checks"].items() if not passed]
        print(f"Jobs assets: {result['status'].upper()} ({result['gold_run_id']})")
        print(
            f"  checks: {len(result['checks']) - len(failed)} passed, failed: {failed or 'none'}"
        )
        print(f"  lineage: {result['lineage'].get('key', result['lineage'].get('error'))}")
    if result["status"] == "failed":
        return 2
    return 3 if result["status"] == "stale" else 0


if __name__ == "__main__":
    raise SystemExit(main())
