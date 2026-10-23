#!/usr/bin/env python3
"""Regenerate coverage and collection health; external delivery is opt-in."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SCRIPTS = Path(__file__).resolve().parents[1]
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.capture_lock import LockBusyError, acquire_lock
from data_lake.collection_health import CollectionHealthError, build_health_report
from data_lake.scraping_coverage import ROOT, build_report, render_markdown, write_report

DEFAULT_OUTPUT = ROOT / "data" / "generated" / "briefings"


class MonitorDeliveryError(RuntimeError):
    """An aggregate alert failed through the existing delivery adapter."""


def _deliver(message: str) -> None:
    # Loading the existing adapter can resolve secrets, so import only after
    # the explicit external-delivery gate, never for local monitoring.
    utils = SCRIPTS / "utils"
    if str(utils) not in sys.path:
        sys.path.insert(0, str(utils))
    from alerts import AlertDeliveryError, send_telegram_message

    try:
        send_telegram_message(message)
    except AlertDeliveryError as exc:
        raise MonitorDeliveryError(str(exc)) from None


def run_monitor(
    output_dir: Path,
    *,
    telegram: bool = False,
    delivery_approved: bool = False,
    probe_api: bool = False,
) -> dict[str, Any]:
    if telegram and not delivery_approved:
        raise ValueError("external_alert_delivery_requires_operator_approval")
    output_dir = output_dir.resolve()
    with acquire_lock(output_dir / "scraping-collection-monitor.lock"):
        coverage = build_report(probe_api=probe_api)
        coverage_path = output_dir / "scraping-data-coverage.json"
        health = build_health_report(coverage, source_report=str(coverage_path))
        day = str(coverage["generated_at"])[:10]
        coverage_json = json.dumps(coverage, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        markdown = render_markdown(coverage)
        documents = {
            coverage_path: coverage_json,
            output_dir / "scraping-data-coverage.md": markdown,
            output_dir / f"scraping-data-coverage-{day}.json": coverage_json,
            output_dir / f"scraping-data-coverage-{day}.md": markdown,
            output_dir / "scraping-collection-health.json": (
                json.dumps(health, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
            ),
        }
        for path, content in documents.items():
            write_report(path, content)

        summary = health["summary"]
        result = {
            "contract": "solo-empire.collection-monitor.v1",
            "generated_at": health["generated_at"],
            "status": health["status"],
            "summary": summary,
            "delivery": "not-requested",
        }
        if telegram:
            if health["status"] == "ok":
                result["delivery"] = "not-needed"
            else:
                message = (
                    f"Scraping collection: {health['status']}; "
                    f"end-to-end {summary['end_to_end_ready']}/{summary['required_products']}; "
                    f"gaps P0={summary['p0_gaps']}, P1={summary['p1_gaps']}, P2={summary['p2_gaps']}; "
                    f"validation errors={summary['validation_error_count']}; "
                    f"audit={health['generated_at']}"
                )
                _deliver(message)
                result["delivery"] = "delivered"
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--probe-api", action="store_true", help="Probe only declared loopback API ports")
    parser.add_argument("--telegram", action="store_true", help="Send aggregate attention/error alerts")
    parser.add_argument(
        "--delivery-approved", action="store_true",
        help="Operator has approved credential use and external aggregate alert delivery",
    )
    parser.add_argument("--strict", action="store_true", help="Exit 2 for incomplete end-to-end readiness")
    parser.add_argument("--schedule-plan", action="store_true", help="Print host-neutral approval-gated metadata only")
    args = parser.parse_args(argv)
    if args.schedule_plan:
        print(json.dumps({
            "contract": "solo-empire.collection-monitor-plan.v1",
            "status": "plan-not-installed",
            "host": "unselected",
            "interval_hours": 1,
            "command": [
                sys.executable, "-X", "utf8", str(Path(__file__).resolve()),
                "--output-dir", str(args.output_dir.resolve()),
            ],
            "working_directory": str(ROOT),
            "external_delivery": "disabled",
            "approval_required": ["host", "identity", "log_retention", "rollback"],
            "overlap_policy": "nonblocking-lock",
        }, indent=2))
        return 0
    try:
        result = run_monitor(
            args.output_dir,
            telegram=args.telegram,
            delivery_approved=args.delivery_approved,
            probe_api=args.probe_api,
        )
    except (CollectionHealthError, LockBusyError, OSError, ValueError) as exc:
        error = type(exc).__name__ if isinstance(exc, OSError) else str(exc)
        print(json.dumps({"status": "error", "error": error}))
        return 1
    except MonitorDeliveryError as exc:
        # The alert adapter returns safe delivery codes, not provider bodies.
        print(json.dumps({"status": "error", "error": str(exc)}))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    if result["status"] == "error":
        return 1
    return 2 if args.strict and result["status"] != "ok" else 0


if __name__ == "__main__":
    from data_lake.observability import run_with_reporting

    raise SystemExit(run_with_reporting("collection-monitor", main))
