#!/usr/bin/env python3
"""Run bounded lake-first captures for approved public market products.

The product repositories own provider calls and their local CSV projections.
This control-plane runner only supplies one explicit lake URI, a shared source
lock, bounded child timeouts, and finite retries. It never calls a product API
or enables refresh/external writes. Product stdout is kept in local logs and
the JSON report contains only stable run metadata and aggregate counts.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence


ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
DEFAULT_LAKE_URI = str(ROOT / "data" / "lake-local" / "scraping-captures-v2")
DEFAULT_LOCK_PATH = ROOT / "data" / "market-feeds.lock"
DEFAULT_LOG_DIR = ROOT / "data" / "logs" / "market-capture"
MAX_CHILD_OUTPUT_BYTES = 2 * 1024 * 1024

if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.capture_lock import acquire_lock as _acquire_lock  # noqa: E402
from data_lake.workspace import OWNER_ROOT  # noqa: E402


@dataclass(frozen=True)
class MarketProduct:
    """One approved public market product and its bounded live command."""

    product: str
    module: str
    relative_workspace: str
    cron_tag: str
    schedule: str
    arguments: tuple[str, ...]
    timeout_seconds: float = 180.0

    @property
    def workspace(self) -> Path:
        return OWNER_ROOT / self.relative_workspace

    @property
    def output_dir(self) -> Path:
        return self.workspace / "data" / "exported"


MARKET_PRODUCTS: tuple[MarketProduct, ...] = (
    MarketProduct(
        "crypto",
        "book_crypto.ingest",
        "public/book-products/market-data/sources/crypto",
        "book-crypto-data-feeds",
        "0 */4 * * *",
        ("--coins", "bitcoin,ethereum", "--vs-currencies", "usd,thb", "--no-trending"),
        120.0,
    ),
    MarketProduct(
        "stocks",
        "book_stock.ingest",
        "public/book-products/market-data/sources/stock",
        "book-stock-data-feeds",
        "0 8 * * *",
        ("--symbols", "AAPL,MSFT"),
        120.0,
    ),
    MarketProduct(
        "fx",
        "book_fx.ingest",
        "public/book-products/market-data/sources/fx",
        "book-fx-data-feeds",
        "0 */6 * * *",
        ("--base", "THB", "--symbols", "USD,EUR"),
        120.0,
    ),
    MarketProduct(
        "defi",
        "book_defi.ingest",
        "public/book-products/market-data/sources/defi",
        "book-defi-data-feeds",
        "0 7 * * *",
        ("--chains", "Ethereum,Arbitrum", "--min-apy", "5"),
        180.0,
    ),
)


class MarketCycleError(RuntimeError):
    """A bounded market cycle failed a safe operational gate."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class LockBusyError(MarketCycleError):
    """Another market cycle owns the shared source lock."""


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def select_products(values: Sequence[str] | None = None) -> tuple[MarketProduct, ...]:
    """Resolve repeated or comma-separated product selectors deterministically."""
    if not values:
        return MARKET_PRODUCTS
    names: list[str] = []
    for value in values:
        names.extend(part.strip() for part in value.split(",") if part.strip())
    by_name = {product.product: product for product in MARKET_PRODUCTS}
    unknown = sorted({name for name in names if name not in by_name})
    if unknown:
        raise ValueError(
            "Unknown product(s): "
            + ", ".join(unknown)
            + ". Choose from: "
            + ", ".join(product.product for product in MARKET_PRODUCTS)
        )
    selected: list[MarketProduct] = []
    seen: set[str] = set()
    for name in names:
        if name not in seen:
            selected.append(by_name[name])
            seen.add(name)
    return tuple(selected)


def validate_lake_uri(lake_uri: str) -> None:
    """Require an explicit existing writable local lake or a URI scheme."""
    if not str(lake_uri).strip():
        raise MarketCycleError("lake_uri_required")
    if "://" in str(lake_uri):
        return
    path = Path(lake_uri).expanduser()
    if not path.is_dir() or not os.access(path, os.W_OK):
        raise MarketCycleError("lake_unavailable")


def require_dependencies() -> None:
    """Fail before provider calls when the parent runtime is incomplete."""
    missing: list[str] = []
    for module in ("duckdb", "pyarrow", "httpx"):
        try:
            importlib.import_module(module)
        except ImportError:
            missing.append(module)
    if missing:
        raise MarketCycleError("dependencies_missing")


@contextmanager
def acquire_lock(path: Path, *, timeout_seconds: float = 0.0) -> Iterator[None]:
    """Acquire an advisory lock and fail closed after a bounded wait."""
    with _acquire_lock(
        path,
        timeout_seconds=timeout_seconds,
        busy_error=LockBusyError,
    ):
        yield


def _safe_counts(output: str) -> dict[str, int]:
    """Extract aggregate counts without returning provider rows or payloads."""
    patterns = {
        "coins": r"Got (\d+) coins",
        "quotes": r"Got (\d+) quotes",
        "rates": r"Got (\d+) rates",
        "pools": r"Total: (\d+) pools tracked",
        "lake_records": r"Lake [^\n]+records=(\d+)",
        "landed_batches": r"Landed batches: (\d+)",
    }
    counts: dict[str, int] = {}
    for name, pattern in patterns.items():
        matches = [int(value) for value in re.findall(pattern, output)]
        if matches:
            counts[name] = max(matches)
    return counts


def _write_child_log(log_dir: Path, product: MarketProduct, output: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{product.product}.log"
    encoded = output.encode("utf-8", errors="replace")
    if len(encoded) > MAX_CHILD_OUTPUT_BYTES:
        encoded = encoded[-MAX_CHILD_OUTPUT_BYTES:]
        prefix = b"[output truncated to the last 2 MiB]\n"
    else:
        prefix = b""
    with path.open("ab") as handle:
        handle.write(f"\n[{utc_now_iso()}]\n".encode("utf-8"))
        handle.write(prefix)
        handle.write(encoded)
        if not encoded.endswith(b"\n"):
            handle.write(b"\n")


def run_product(
    product: MarketProduct,
    *,
    lake_uri: str,
    log_dir: Path,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Run one nested product's lake-first CLI with a redacted result."""
    workspace = product.workspace
    if not workspace.is_dir() or not (workspace / "src").is_dir():
        return {"product": product.product, "status": "failed", "error": "workspace_missing"}
    child_timeout = timeout_seconds if timeout_seconds is not None else product.timeout_seconds
    if child_timeout <= 0:
        raise ValueError("child timeout must be positive")
    command = [
        sys.executable,
        "-m",
        product.module,
        *product.arguments,
        "--data-lake-uri",
        str(lake_uri),
        "--output-dir",
        str(product.output_dir),
    ]
    env = {
        **os.environ,
        "FREE_ONLY": "true",
        "ALLOW_PAID_PROVIDERS": "false",
        "ALLOW_EXTERNAL_WRITES": "false",
        "ALLOW_REFRESH": "false",
        "SOLO_EMPIRE_DATA_LAKE_URI": str(lake_uri),
        "PYTHONPATH": os.pathsep.join(
            [str(workspace / "src"), str(workspace), str(SCRIPTS)]
            + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else [])
        ),
    }
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=workspace,
            env=env,
            capture_output=True,
            text=True,
            timeout=child_timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        output = "\n".join(
            part for part in (exc.stdout or "", exc.stderr or "") if isinstance(part, str)
        )
        _write_child_log(log_dir, product, output)
        return {
            "product": product.product,
            "status": "failed",
            "error": "child_timeout",
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    except OSError as exc:
        return {
            "product": product.product,
            "status": "failed",
            "error": f"child_{type(exc).__name__}",
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    output = (completed.stdout or "") + (completed.stderr or "")
    _write_child_log(log_dir, product, output)
    report: dict[str, Any] = {
        "product": product.product,
        "status": "ok" if completed.returncode == 0 else "failed",
        "exit_code": completed.returncode,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
    counts = _safe_counts(output)
    if counts:
        report["counts"] = counts
    if completed.returncode != 0:
        report["error"] = "child_failed"
    return report


def run_market_cycle(
    *,
    lake_uri: str,
    products: Sequence[MarketProduct],
    lock_path: Path = DEFAULT_LOCK_PATH,
    log_dir: Path = DEFAULT_LOG_DIR,
    attempts: int = 2,
    retry_backoff_seconds: float = 1.0,
    lock_timeout_seconds: float = 0.0,
    timeout_seconds: float | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run selected products sequentially and retry only failed products."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    if retry_backoff_seconds < 0:
        raise ValueError("retry backoff must be non-negative")
    started_at = utc_now_iso()
    if dry_run:
        return {
            "status": "dry-run",
            "products": [product.product for product in products],
            "attempts": 0,
            "started_at": started_at,
            "completed_at": utc_now_iso(),
            "plan": {
                "network": "not-used",
                "writes": "not-used",
                "timeout_seconds": timeout_seconds,
            },
            "results": [],
        }
    validate_lake_uri(lake_uri)
    require_dependencies()
    try:
        with acquire_lock(lock_path, timeout_seconds=lock_timeout_seconds):
            pending = list(products)
            results: dict[str, dict[str, Any]] = {}
            for attempt in range(1, attempts + 1):
                next_pending: list[MarketProduct] = []
                for product in pending:
                    result = run_product(
                        product,
                        lake_uri=lake_uri,
                        log_dir=log_dir,
                        timeout_seconds=timeout_seconds,
                    )
                    result["attempt"] = attempt
                    results[product.product] = result
                    if result["status"] != "ok":
                        next_pending.append(product)
                pending = next_pending
                if not pending:
                    break
                if attempt < attempts and retry_backoff_seconds:
                    time.sleep(retry_backoff_seconds * (2 ** (attempt - 1)))
            ordered = [results[product.product] for product in products]
            return {
                "status": "ok" if not pending else "failed",
                "products": [product.product for product in products],
                "attempts": max((int(item.get("attempt", 0)) for item in ordered), default=0),
                "started_at": started_at,
                "completed_at": utc_now_iso(),
                "results": ordered,
            }
    except LockBusyError as exc:
        return {
            "status": "locked",
            "products": [product.product for product in products],
            "attempts": 0,
            "error": exc.code,
            "started_at": started_at,
            "completed_at": utc_now_iso(),
            "results": [],
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", action="append", help="Bounded product selector (repeatable or comma-separated)")
    parser.add_argument(
        "--lake-uri",
        default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_LAKE_URI),
    )
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--attempts", type=int, default=2)
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--lock-timeout-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Print the selected products without collection or writes")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    products: tuple[MarketProduct, ...] = ()
    try:
        products = select_products(args.product)
        report = run_market_cycle(
            lake_uri=args.lake_uri,
            products=products,
            lock_path=args.lock_path,
            log_dir=args.log_dir,
            attempts=args.attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
            lock_timeout_seconds=args.lock_timeout_seconds,
            timeout_seconds=args.timeout_seconds,
            dry_run=args.dry_run,
        )
    except (MarketCycleError, ValueError) as exc:
        report = {
            "status": "blocked",
            "error": exc.code if isinstance(exc, MarketCycleError) else type(exc).__name__,
            "products": [product.product for product in products],
            "started_at": utc_now_iso(),
            "completed_at": utc_now_iso(),
            "results": [],
        }
    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "market cycle: "
            f"{report['status']} (products={','.join(report.get('products', []))}, "
            f"error={report.get('error', 'none')})"
        )
        for item in report.get("results", []):
            print(f"  {item.get('product')}: {item.get('status')} (attempt={item.get('attempt', 0)})")
    return 0 if report["status"] in {"ok", "dry-run"} else 2 if report["status"] in {"locked", "blocked"} else 1


if __name__ == "__main__":
    from data_lake.observability import run_with_reporting

    raise SystemExit(run_with_reporting("market", main))
