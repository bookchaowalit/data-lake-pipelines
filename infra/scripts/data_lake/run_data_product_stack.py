#!/usr/bin/env python3
"""Run or smoke-check the local read-only data-product APIs.

This is an operator helper, not a new backend. It starts the existing product
API modules with one shared lake URI so the frontend and dashboard can read
the same Bronze datasets that the capture replay command populated. The
separate Track B ``book-job-data`` API is included on :8109. ``--require-job-
readiness`` is an opt-in fail-closed barrier that checks the bounded job lake
before any child API is started.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    # Task runs this file from ``infra/`` as a script, so Python otherwise
    # exposes only ``infra/scripts/data_lake`` and cannot import the sibling
    # data-lake package used by the readiness gate.
    sys.path.insert(0, str(SCRIPTS))

from data_lake.workspace import OWNER_ROOT  # noqa: E402


@dataclass(frozen=True)
class ProductRuntime:
    product: str
    repository: str
    module: str
    port: int
    relative_path: str = ""


PRODUCTS: tuple[ProductRuntime, ...] = (
    ProductRuntime("crypto", "book-crypto-data", "book_crypto.api", 8101, "public/book-products/market-data/sources/crypto"),
    ProductRuntime("stocks", "book-stock-data", "book_stock.api", 8102, "public/book-products/market-data/sources/stock"),
    ProductRuntime("fx", "book-fx-data", "book_fx.api", 8103, "public/book-products/market-data/sources/fx"),
    ProductRuntime("defi", "book-defi-data", "book_defi.api", 8104, "public/book-products/market-data/sources/defi"),
    ProductRuntime("flights", "book-flight-data", "book_flight.api", 8105, "private/book-products/travel-flight-price-data"),
    ProductRuntime("seo", "book-seo-data", "book_seo.api", 8106, "private/book-products/marketing-seo-ranking-data"),
    ProductRuntime("news", "book-news-scraping", "news_data.api", 8108, "public/book-apps/tools/content-news-scraper"),
    ProductRuntime("discovery", "book-discovery-data", "discovery_data.api", 8110, "public/book-apps/tools/tech-discovery-data"),
    ProductRuntime("jobs", "book-job-data", "book_job_data.api", 8109, "public/book-apps/tools/book-job-data-public"),
)


def product_workspace(product: ProductRuntime) -> Path:
    """Resolve a product workspace from the Book Dev owner boundary."""
    owner_root = OWNER_ROOT
    if product.relative_path:
        return owner_root / product.relative_path
    # Keep compatibility for callers constructing ProductRuntime with the
    # original fields only; repositories sit under a visibility folder.
    for visibility in ("private", "public"):
        candidate = owner_root / visibility / "book-apps/tools" / product.repository
        if candidate.exists():
            return candidate
    return owner_root / "private/book-apps/tools" / product.repository


def select_products(values: Iterable[str] | None = None) -> tuple[ProductRuntime, ...]:
    """Select a bounded API subset while preserving the declared order."""
    if not values:
        return PRODUCTS
    names: list[str] = []
    for value in values:
        names.extend(part.strip() for part in value.split(",") if part.strip())
    by_name = {product.product: product for product in PRODUCTS}
    unknown = sorted({name for name in names if name not in by_name})
    if unknown:
        raise ValueError(
            "Unknown product(s): "
            + ", ".join(unknown)
            + ". Choose from: "
            + ", ".join(product.product for product in PRODUCTS)
        )
    selected: list[ProductRuntime] = []
    seen: set[str] = set()
    for name in names:
        if name not in seen:
            selected.append(by_name[name])
            seen.add(name)
    return tuple(selected)


def _api_url(product: ProductRuntime, endpoint: str = "/healthz") -> str:
    return f"http://127.0.0.1:{product.port}{endpoint}"


def check_job_readiness(
    lake_uri: str,
    *,
    datasets: Iterable[str] | None = None,
    max_age_hours: float | None = None,
) -> dict[str, object]:
    """Return a redacted job-data readiness report for the stack barrier."""
    try:
        from data_lake.job_data_readiness import build_job_readiness_report

        report = build_job_readiness_report(
            lake_uri,
            datasets=tuple(datasets) if datasets else ("job_postings", "job_matches"),
            max_age_hours=max_age_hours,
        )
        if not isinstance(report, dict):
            return {"status": "failed", "error": "invalid_readiness_report"}
        return report
    except Exception as exc:  # noqa: BLE001 - stack must fail closed
        # Do not echo provider errors, object keys, or payloads into stack logs.
        return {"status": "failed", "error": type(exc).__name__}


def _readiness_summary(report: dict[str, object]) -> dict[str, object]:
    """Keep the stack log to aggregate gate evidence only."""
    freshness = report.get("freshness")
    reconciliation = report.get("reconciliation")
    alerts = report.get("alerts")
    safe_alerts: list[dict[str, object]] = []
    if isinstance(alerts, list):
        for alert in alerts:
            if isinstance(alert, dict):
                safe_alerts.append(
                    {
                        "code": alert.get("code"),
                        "severity": alert.get("severity"),
                        "dataset": alert.get("dataset"),
                    }
                )
    return {
        "status": report.get("status", "failed"),
        "reconciliation": reconciliation if isinstance(reconciliation, dict) else {},
        "freshness": freshness if isinstance(freshness, dict) else {},
        "alerts": safe_alerts,
    }


def check_health(
    products: Iterable[ProductRuntime], *, timeout: float = 3.0
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for product in products:
        try:
            request = urllib.request.Request(
                _api_url(product), headers={"Accept": "application/json"}
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = json.loads(response.read(65536).decode("utf-8"))
            if (
                not isinstance(body, dict)
                or body.get("status") != "ok"
                or body.get("repository") != product.repository
            ):
                raise ValueError("Unexpected health response")
            results.append(
                {
                    "product": product.product,
                    "port": product.port,
                    "ok": True,
                    "body": body,
                }
            )
        except (OSError, urllib.error.URLError, ValueError) as exc:
            results.append(
                {
                    "product": product.product,
                    "port": product.port,
                    "ok": False,
                    "error": type(exc).__name__,
                }
            )
    return results


def start_products(
    products: Iterable[ProductRuntime], *, lake_uri: str, log_dir: Path
) -> list[subprocess.Popen[bytes]]:
    products = tuple(products)
    # Reject occupied ports before starting any child. Child liveness is also
    # checked after startup to cover a bind race after this preflight.
    for product in products:
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", product.port))
    log_dir.mkdir(parents=True, exist_ok=True)
    processes: list[subprocess.Popen[bytes]] = []
    try:
        for product in products:
            repo_dir = product_workspace(product)
            if not repo_dir.is_dir():
                raise OSError(f"Missing product workspace: {product.repository}")
            src_dir = repo_dir / "src"
            pythonpath = [str(src_dir)] if src_dir.is_dir() else []
            pythonpath.extend([str(repo_dir), str(SCRIPTS)])
            env = os.environ.copy()
            env.update(
                {
                    "API_HOST": "127.0.0.1",
                    "API_PORT": str(product.port),
                    "PORT": str(product.port),
                    "FREE_ONLY": "true",
                    "ALLOW_PAID_PROVIDERS": "false",
                    "ALLOW_EXTERNAL_WRITES": "false",
                    "ALLOW_REFRESH": "false",
                    "SOLO_EMPIRE_DATA_LAKE_URI": lake_uri,
                    "PYTHONPATH": os.pathsep.join(pythonpath),
                }
            )
            with (log_dir / f"{product.product}.log").open("ab") as log_file:
                processes.append(
                    subprocess.Popen(
                        [sys.executable, "-m", product.module],
                        cwd=repo_dir,
                        env=env,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                    )
                )
    except BaseException:
        stop_products(processes)
        raise
    return processes


def stop_products(processes: Iterable[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
    deadline = time.monotonic() + 5
    for process in processes:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lake-uri", default=str(ROOT / "data" / "lake-local" / "scraping-captures-v2")
    )
    parser.add_argument(
        "--check", action="store_true", help="Start APIs, verify health, then stop them"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print the selected products without starting APIs"
    )
    parser.add_argument(
        "--products",
        action="append",
        metavar="NAME[,NAME...]",
        help="Run a bounded subset (repeat or comma-separate product names; default: all)",
    )
    parser.add_argument(
        "--require-job-readiness",
        action="store_true",
        help="Run the read-only job-data gate before starting any API",
    )
    parser.add_argument(
        "--readiness-dataset",
        action="append",
        dest="readiness_datasets",
        help="Dataset for --require-job-readiness; repeat for multiple datasets",
    )
    parser.add_argument(
        "--readiness-max-age-hours",
        type=float,
        help="Override the job-data freshness threshold for the stack gate",
    )
    parser.add_argument(
        "--log-dir", type=Path, default=ROOT / "data" / "runtime" / "scraping-stack"
    )
    parser.add_argument("--startup-seconds", type=float, default=1.5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        products = select_products(args.products)
    except ValueError as exc:
        print(f"Stack blocked: {exc}")
        return 2
    if not products:
        print("Stack blocked: at least one product is required.")
        return 2
    if args.dry_run:
        print(json.dumps({
            "status": "dry-run",
            "products": [product.product for product in products],
            "network": "not-used",
            "writes": "not-used",
        }, ensure_ascii=False, sort_keys=True))
        return 0
    if args.require_job_readiness:
        readiness = check_job_readiness(
            args.lake_uri,
            datasets=args.readiness_datasets,
            max_age_hours=args.readiness_max_age_hours,
        )
        print(
            "job-data readiness: "
            + json.dumps(
                _readiness_summary(readiness), ensure_ascii=False, sort_keys=True
            )
        )
        if readiness.get("status") != "ok":
            print("Stack blocked: book-job-data readiness gate did not pass.")
            return 2
    try:
        processes = start_products(
            products, lake_uri=args.lake_uri, log_dir=args.log_dir
        )
    except OSError:
        print(
            "Cannot start stack: check occupied ports, repository paths, and log permissions."
        )
        return 1
    try:
        time.sleep(max(0.1, args.startup_seconds))
        if any(process.poll() is not None for process in processes):
            print("A data-product API failed to start; inspect the product logs.")
            return 1
        results = check_health(products)
        failed = [item for item in results if not item["ok"]]
        for item in results:
            if item["ok"]:
                body = item.get("body")
                data_status = body.get("data_status", "unknown") if isinstance(body, dict) else "unknown"
                status = f"ok (data_status={data_status})"
            else:
                status = f"failed ({item['error']})"
            print(f"{item['product']} :{item['port']} {status}")
        if failed or args.check:
            return 1 if failed else 0
        print("Data-product APIs are running. Press Ctrl-C to stop.")
        while True:
            exited = [process for process in processes if process.poll() is not None]
            if exited:
                print("A data-product API exited unexpectedly; stopping the stack.")
                return 1
            time.sleep(1)
    except KeyboardInterrupt:
        return 0
    finally:
        stop_products(processes)


if __name__ == "__main__":
    raise SystemExit(main())
