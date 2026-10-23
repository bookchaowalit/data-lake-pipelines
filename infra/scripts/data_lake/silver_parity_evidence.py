#!/usr/bin/env python3
"""Run a bounded offline Bronze/Silver parity evidence check.

The runner seeds only product-specific Bronze fixtures into a temporary local
lake, creates the matching Silver snapshot, exercises each product store in
``compare`` and ``silver`` modes, and emits a JSON evidence report. It never
calls an upstream provider and never writes CSV projections or Object Storage
unless an explicit ``--data-lake-uri`` is supplied.
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.silver import SilverProductContract, transform_bronze_to_silver  # noqa: E402
from data_lake.workspace import OWNER_ROOT  # noqa: E402


PRODUCT_ROOTS = {
    "crypto": OWNER_ROOT / "public/book-products/market-data/sources/crypto",
    "stock": OWNER_ROOT / "public/book-products/market-data/sources/stock",
    "fx": OWNER_ROOT / "public/book-products/market-data/sources/fx",
}

CRYPTO_SAMPLE = {
    "bitcoin": {
        "usd": 50000.0,
        "usd_24h_change": 2.5,
        "usd_24h_vol": 1e9,
        "usd_market_cap": 1e12,
        "last_updated_at": 1720000000,
    },
    "ethereum": {
        "usd": 3000.0,
        "usd_24h_change": -1.2,
        "usd_24h_vol": 5e8,
        "usd_market_cap": 3e11,
        "last_updated_at": 1720000000,
    },
}
STOCK_SAMPLE = [
    {
        "symbol": "AAPL",
        "price": 190.5,
        "prev_close": 189.0,
        "change": 1.5,
        "change_pct": 0.79,
        "currency": "USD",
        "exchange": "NMS",
        "timestamp": "2026-08-01 16:00:00",
    },
    {
        "symbol": "MSFT",
        "price": 420.0,
        "prev_close": 418.0,
        "change": 2.0,
        "change_pct": 0.48,
        "currency": "USD",
        "exchange": "NMS",
        "timestamp": "2026-08-01 16:00:00",
    },
]
FX_SAMPLE = {
    "base": "THB",
    "date": "2026-08-01",
    "rates": {"USD": 0.028, "EUR": 0.026},
}
FX_TRENDS = {
    "USD": {"direction": "stable", "change_pct": 0.1},
    "EUR": {"direction": "stable", "change_pct": -0.2},
}


def _load_product(name: str) -> tuple[ModuleType, ModuleType, ModuleType]:
    root = PRODUCT_ROOTS[name]
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))
    package = {"crypto": "book_crypto", "stock": "book_stock", "fx": "book_fx"}[name]
    return (
        importlib.import_module(f"{package}.config"),
        importlib.import_module(f"{package}.lake"),
        importlib.import_module(f"{package}.store"),
    )


def _seed(name: str, store: ModuleType, lake_uri: str) -> None:
    if name == "crypto":
        store.seed_lake_from_sample(CRYPTO_SAMPLE, ["usd"], data_lake_uri=lake_uri)
    elif name == "stock":
        store.seed_lake_from_quotes(STOCK_SAMPLE, data_lake_uri=lake_uri)
    else:
        store.seed_lake_from_rates(
            FX_SAMPLE,
            data_lake_uri=lake_uri,
            trends=FX_TRENDS,
        )


def _contract(name: str, config: ModuleType) -> SilverProductContract:
    if name == "crypto":
        dataset = config.LAKE_DATASET_PRICES
        return SilverProductContract(
            source=config.LAKE_SOURCE,
            domain=config.LAKE_DOMAIN,
            bronze_dataset=dataset,
            silver_dataset=dataset,
            product_schema_version="crypto.silver.v1",
            bronze_product_schema_version=config.SCHEMA_VERSION,
            bronze_schema_version=config.LAKE_BRONZE_SCHEMA_VERSION,
            normalizer="crypto_prices",
            required_fields=("source_record_id", "coin_id", "currency", "price"),
            privacy_class=config.LAKE_PRIVACY_CLASS,
            retention_class=config.LAKE_RETENTION_CLASS,
        )
    if name == "stock":
        dataset = config.LAKE_DATASET_PRICES
        return SilverProductContract(
            source=config.LAKE_SOURCE,
            domain=config.LAKE_DOMAIN,
            bronze_dataset=dataset,
            silver_dataset=dataset,
            product_schema_version="stock.silver.v1",
            bronze_product_schema_version=config.SCHEMA_VERSION,
            bronze_schema_version=config.LAKE_BRONZE_SCHEMA_VERSION,
            normalizer="stock_prices",
            required_fields=("source_record_id", "symbol", "price", "currency"),
            privacy_class=config.LAKE_PRIVACY_CLASS,
            retention_class=config.LAKE_RETENTION_CLASS,
        )
    dataset = config.LAKE_DATASET_RATES
    return SilverProductContract(
        source=config.LAKE_SOURCE,
        domain=config.LAKE_DOMAIN,
        bronze_dataset=dataset,
        silver_dataset=dataset,
        product_schema_version="fx.silver.v1",
        bronze_product_schema_version=config.SCHEMA_VERSION,
        bronze_schema_version=config.LAKE_BRONZE_SCHEMA_VERSION,
        normalizer="fx_rates",
        required_fields=("source_record_id", "base_currency", "quote_currency", "rate"),
        privacy_class=config.LAKE_PRIVACY_CLASS,
        retention_class=config.LAKE_RETENTION_CLASS,
    )


def _write_poison_csv(config: ModuleType, data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    data_dir.joinpath(config.RECORDS_FILE).write_text(
        "poison,should-not-be-read\n", encoding="utf-8"
    )


def _run_product(name: str, lake_uri: str, workspace: Path) -> dict[str, Any]:
    config: Any
    config, _lake, store = _load_product(name)
    config.DATA_LAKE_URI = lake_uri
    config.DATA_DIR = workspace / "data"
    config.LAKE_READ_MODE = "parquet"
    config.LAKE_READ_FALLBACK = "error"
    _write_poison_csv(config, config.DATA_DIR)
    _seed(name, store, lake_uri)
    transform = transform_bronze_to_silver(_contract(name, config), data_lake_uri=lake_uri)

    config.SILVER_READ_MODE = "compare"
    compare = store.load_records()
    config.SILVER_READ_MODE = "silver"
    silver = store.load_records()
    history = store.load_history()
    poison_seen = any(
        str(item.get("symbol") or item.get("coin_id") or item.get("base")) == "poison"
        for item in silver["items"]
    )
    passed = (
        compare.get("silver_parity", {}).get("status") == "passed"
        and compare.get("source_kind", "").startswith("bronze")
        and silver.get("source_kind", "").startswith("silver")
        and silver.get("data_status") in {"ok", "stale"}
        and history.get("source_kind", "").startswith("bronze")
        and not poison_seen
    )
    return {
        "status": "passed" if passed else "failed",
        "product": name,
        "transform": {
            "silver_run_id": transform["silver_run_id"],
            "record_count": transform["record_count"],
            "silver_key": transform["silver_key"],
            "manifest_key": transform["manifest_key"],
        },
        "compare": {
            "source_kind": compare.get("source_kind"),
            "data_status": compare.get("data_status"),
            "parity": compare.get("silver_parity"),
        },
        "silver": {
            "source_kind": silver.get("source_kind"),
            "data_status": silver.get("data_status"),
            "record_count": len(silver.get("items", [])),
        },
        "history": {
            "source_kind": history.get("source_kind"),
            "parity_status": history.get("silver_parity", {}).get("status"),
        },
        "csv_projection": {
            "present": config.DATA_DIR.joinpath(config.RECORDS_FILE).exists(),
            "used": poison_seen,
        },
    }


def run_evidence(data_lake_uri: str = "") -> dict[str, Any]:
    """Run all product checks and return a JSON-safe evidence report."""
    with tempfile.TemporaryDirectory(prefix="solo-empire-silver-evidence-") as temp:
        root = Path(temp)
        lake_uri = data_lake_uri or str(root / "lake")
        products = []
        for name in ("crypto", "stock", "fx"):
            try:
                products.append(_run_product(name, lake_uri, root / name))
            except Exception as exc:  # noqa: BLE001 - evidence boundary
                products.append({
                    "status": "failed",
                    "product": name,
                    "error": f"{type(exc).__name__}: {exc}",
                })
        return {
            "status": "passed" if all(item["status"] == "passed" for item in products) else "failed",
            "mode": "offline-fixture",
            "network": "not-used",
            "csv": "not-used-as-source",
            "products": products,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-lake-uri",
        default="",
        help="Optional explicit local/S3-compatible lake target; default is a temporary local lake",
    )
    parser.add_argument("--output", type=Path, help="Optional JSON evidence output path")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report = run_evidence(args.data_lake_uri)
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if report["status"] == "passed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
