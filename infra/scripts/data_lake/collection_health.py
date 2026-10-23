#!/usr/bin/env python3
"""Build collection health from the redacted scraping coverage audit.

This command intentionally consumes only the JSON coverage-audit output. It
does not infer health from scheduler timestamps, read scheduler state, inspect
raw records, read credentials, or call remote providers.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_COVERAGE_REPORT = ROOT / "data" / "generated" / "briefings" / "scraping-data-coverage.json"
SCHEMA_VERSION = "solo-empire.collection-health.v1"
COVERAGE_SCHEMA_VERSION = "solo-empire.scraping-data-coverage.v1"


class CollectionHealthError(ValueError):
    """Raised when a coverage audit report cannot be consumed safely."""


def _load_report(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise CollectionHealthError(f"missing coverage audit report: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CollectionHealthError(f"invalid coverage audit report: {path}") from exc
    if not isinstance(payload, dict):
        raise CollectionHealthError("coverage audit report must be a JSON object")
    return payload


def _safe_gaps(report: dict[str, Any]) -> list[dict[str, Any]]:
    gaps = report.get("gaps")
    if not isinstance(gaps, list):
        raise CollectionHealthError("coverage audit gaps must be a list")
    safe: list[dict[str, Any]] = []
    for gap in gaps:
        if not isinstance(gap, dict):
            raise CollectionHealthError("coverage audit gaps must contain objects")
        if gap.get("priority") not in {"P0", "P1", "P2"}:
            raise CollectionHealthError("coverage audit gap priority is invalid")
        for field in ("product_id", "code", "detail", "owner", "next_action"):
            if not isinstance(gap.get(field), str) or not gap[field].strip():
                raise CollectionHealthError(f"coverage audit gap {field} must be a non-empty string")
        safe.append(
            {
                "priority": gap.get("priority"),
                "product_id": gap.get("product_id"),
                "code": gap.get("code"),
                "detail": gap.get("detail"),
                "owner": gap.get("owner"),
                "next_action": gap.get("next_action"),
            }
        )
    return safe


def build_health_report(report: dict[str, Any], *, source_report: str) -> dict[str, Any]:
    if report.get("contract") != COVERAGE_SCHEMA_VERSION:
        raise CollectionHealthError("unsupported coverage audit contract")
    generated_at: Any = report.get("generated_at")
    try:
        generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError):
        raise CollectionHealthError("coverage audit generated_at must be an ISO timestamp") from None
    if generated.tzinfo is None:
        raise CollectionHealthError("coverage audit generated_at requires a timezone")
    validation_errors = report.get("validation_errors")
    if not isinstance(validation_errors, list) or any(not isinstance(error, str) for error in validation_errors):
        raise CollectionHealthError("coverage audit validation_errors must be a list of strings")
    summary = report.get("summary")
    if not isinstance(summary, dict):
        raise CollectionHealthError("coverage audit report is missing summary")
    gaps = _safe_gaps(report)
    for field in ("required_products", "end_to_end_ready", "p0_gaps", "p1_gaps", "p2_gaps"):
        value = summary.get(field)
        if type(value) is not int or value < 0:
            raise CollectionHealthError(f"coverage audit summary {field} must be a non-negative integer")
    required = summary["required_products"]
    end_to_end_ready = summary["end_to_end_ready"]
    if required == 0 or end_to_end_ready > required:
        raise CollectionHealthError("coverage audit readiness counts are inconsistent")
    percentage = summary.get("end_to_end_ready_pct")
    if (
        type(percentage) not in (int, float)
        or not math.isfinite(percentage)  # type: ignore[arg-type]  # narrowed by the type() check above
        or percentage != round(end_to_end_ready / required * 100, 1)
    ):
        raise CollectionHealthError("coverage audit readiness percentage is inconsistent")
    for priority in ("P0", "P1", "P2"):
        if summary[f"{priority.lower()}_gaps"] != sum(gap["priority"] == priority for gap in gaps):
            raise CollectionHealthError("coverage audit gap counts are inconsistent")
    if validation_errors:
        status = "error"
    elif end_to_end_ready != required or gaps:
        status = "attention"
    else:
        status = "ok"
    return {
        "contract": SCHEMA_VERSION,
        "source": "scraping_coverage_audit",
        "source_report": source_report,
        "generated_at": report.get("generated_at"),
        "status": status,
        "summary": {
            "required_products": required,
            "end_to_end_ready": end_to_end_ready,
            "end_to_end_ready_pct": summary.get("end_to_end_ready_pct"),
            "validation_error_count": len(validation_errors),
            "gap_count": len(gaps),
            "p0_gaps": summary.get("p0_gaps", 0),
            "p1_gaps": summary.get("p1_gaps", 0),
            "p2_gaps": summary.get("p2_gaps", 0),
        },
        "validation_errors": [str(error) for error in validation_errors],
        "gaps": gaps,
        "safety": {
            "scheduler_state": "not-used",
            "network": "not-used",
            "credentials": "not-read",
            "raw_records": "not-read",
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-report", type=Path, default=DEFAULT_COVERAGE_REPORT)
    parser.add_argument("--json", action="store_true", help="Print the health report as JSON")
    parser.add_argument("--output", type=Path, help="Write the redacted health report to this path")
    parser.add_argument("--strict", action="store_true", help="Fail on validation errors or runtime coverage gaps")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    coverage_path = args.coverage_report.resolve()
    try:
        report = build_health_report(
            _load_report(coverage_path),
            source_report=coverage_path.as_posix(),
        )
    except CollectionHealthError as exc:
        print(f"ERROR: {exc}")
        return 1
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(encoded, encoding="utf-8")
    if args.json:
        print(encoded, end="")
    else:
        summary = report["summary"]
        print(
            "Collection health: "
            f"{report['status']} "
            f"({summary['end_to_end_ready']}/{summary['required_products']} end-to-end, "
            f"{summary['gap_count']} gap(s), source=scraping_coverage_audit)"
        )
    if report["validation_errors"]:
        return 1
    if args.strict and report["status"] != "ok":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
