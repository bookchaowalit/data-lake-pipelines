#!/usr/bin/env python3
"""Guard frozen data-product contracts across producers and consumers.

Validates:
  - systems/architecture/data-product-contracts-v4.yaml is present and well-formed
  - five producer repos expose matching SCHEMA_VERSION, API_PORT, free-only defaults
  - envelope required keys exist in store.envelope
  - POST /v1/refresh remains default-forbidden in api.py
  - consumer catalogs match the frozen port/schema map
  - five source products retain the reserved 8101–8104 and 8106 ports (v4 retired
    the flights row on :8105, v3 the ai_tools row on :8107; both ports are never
    reused and must not reappear)

Usage:
  python3 infra/scripts/utils/data_product_contract_guard.py
  task contracts:check

Track B job data is checked separately from the frozen five-product v4 map:
  systems/architecture/job-data-contract-v1.yaml → book-job-data → job.v1 / :8109
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

from _env import PROJECT_ROOT


PORTABLE_CI = os.environ.get("SOLO_EMPIRE_CI") == "1"
FREEZE_PATH = PROJECT_ROOT / "systems" / "architecture" / "data-product-contracts-v4.yaml"
FREEZE_ID = "data-product-contracts-v4"
RETIRED_PORTS = (8105, 8107)
FROZEN_PORTS = [8101, 8102, 8103, 8104, 8106]


def is_external_boundary(path: Path) -> bool:
    try:
        rel = path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return False
    return rel == "projects/product/engineering/book-dev" or rel.startswith("projects/product/engineering/book-dev/")


def report_missing(errors: list[str], message: str, path: Path) -> None:
    if PORTABLE_CI and is_external_boundary(path):
        print(f"  WARN: portable CI deferred: {message}")
    else:
        fail(errors, message)


ENVELOPE_KEYS = (
    "schema_version",
    "source",
    "retrieved_at",
    "data_status",
    "items",
    "next_cursor",
)

# Hardcoded mirror of the freeze (must match YAML). Used if YAML list parsing
# needs a fallback for consumer catalog file lists.
EXPECTED_PRODUCTS = [
    {"id": "crypto", "port": 8101, "schema_version": "crypto.v1", "repository": "book-crypto-data", "package": "book_crypto"},
    {"id": "stocks", "port": 8102, "schema_version": "stock.v1", "repository": "book-stock-data", "package": "book_stock"},
    {"id": "fx", "port": 8103, "schema_version": "fx.v1", "repository": "book-fx-data", "package": "book_fx"},
    {"id": "defi", "port": 8104, "schema_version": "defi.v1", "repository": "book-defi-data", "package": "book_defi"},
    {"id": "seo", "port": 8106, "schema_version": "seo.v1", "repository": "book-seo-data", "package": "book_seo"},
]

CONSUMER_SPECS = [
    {
        "id": "insights-dashboard",
        "path": "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-insights-web",
        "catalog_files": [
            "src/data-products/catalog.ts",
            "js/data-products-browser.js",
        ],
        "ports": list(FROZEN_PORTS),
    },
    {
        "id": "bookchaowalit-portfolio-frontend",
        "path": "projects/product/engineering/book-dev/github/bookchaowalit-website/public/book-products/portfolio-site-web",
        "catalog_files": ["src/lib/data-products/catalog.ts"],
        "ports": list(FROZEN_PORTS),
    },
]

JOB_CONSUMER_FILES = (
    "infra/scripts/opportunities/import_job_scraper.py",
    "infra/scripts/workflows/dashboard.py",
    "projects/marketing/growth/book-sales/scripts/sync_leads.py",
)

JOB_CSV_FRAGMENTS = (
    "job_postings.csv",
    "job_postings_history.csv",
    "matched_jobs.csv",
    "leads.csv",
    "jobsdb_jobs.csv",
)

JOB_CSV_SCAN_ROOTS = (
    "catalog",
    "infra/scripts",
    "projects/marketing",
)

JOB_CSV_SCAN_ALLOWLIST = {
    "infra/scripts/utils/data_product_contract_guard.py",
    "infra/scripts/utils/repository_catalog_guard.py",
}

PRODUCER_ROOT = (
    PROJECT_ROOT
    / "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools"
)


def fail(errors: list[str], message: str) -> None:
    errors.append(message)


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _contains_csv_reference(text: str, fragment: str) -> bool:
    """Match a CSV basename without flagging a longer embedded filename."""

    # ``leads.csv`` is a protected job projection, while
    # ``property_social_leads.csv`` is a different property capture. Require
    # basename boundaries so the latter does not trip the former's guard.
    pattern = rf"(?<![A-Za-z0-9_]){re.escape(fragment)}(?![A-Za-z0-9_.-])"
    return re.search(pattern, text) is not None


def extract_assignment(text: str, name: str) -> str | None:
    m = re.search(
        rf"^{re.escape(name)}\s*=\s*['\"]([^'\"]+)['\"]",
        text,
        re.MULTILINE,
    )
    return m.group(1) if m else None


def extract_port_default(text: str) -> int | None:
    patterns = (
        r'API_PORT\s*=\s*int\(\s*os\.environ\.get\(\s*["\']API_PORT["\']\s*,\s*["\'](\d+)["\']\s*\)\s*\)',
        r'API_PORT\s*=\s*int\(\s*os\.environ\.get\(\s*["\']PORT["\']\s*,\s*os\.environ\.get\(\s*["\']API_PORT["\']\s*,\s*["\'](\d+)["\']\s*\)\s*\)\s*\)',
    )
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return int(m.group(1))
    return None


def extract_env_bool_default(text: str, name: str) -> bool | None:
    m = re.search(
        rf"{re.escape(name)}\s*=\s*env_bool\(\s*[\"']{re.escape(name)}[\"']\s*,\s*(True|False)\s*\)",
        text,
    )
    if not m:
        return None
    return m.group(1) == "True"


def parse_freeze_yaml() -> dict:
    """Parse the freeze YAML with a tiny subset parser + hardcoded product fallback."""
    if not FREEZE_PATH.is_file():
        raise FileNotFoundError(f"Missing freeze file: {FREEZE_PATH}")
    text = read(FREEZE_PATH)
    freeze_id = ""
    status = ""
    m = re.search(r"^freeze_id:\s*(\S+)", text, re.M)
    if m:
        freeze_id = m.group(1)
    m = re.search(r"^status:\s*(\S+)", text, re.M)
    if m:
        status = m.group(1)

    # Products from YAML when present
    products: list[dict] = []
    block = re.search(r"^products:\n((?:  .*\n)+)", text, re.M)
    if block:
        for item in re.finditer(
            r"  - id:\s*(\S+)\n((?:    .*\n)+)",
            block.group(0),
        ):
            prod = {"id": item.group(1)}
            for km in re.finditer(r"    (\w+):\s*(.+)", item.group(2)):
                key, val = km.group(1), km.group(2).strip()
                if val.startswith("[") and val.endswith("]"):
                    inner = val[1:-1].strip()
                    prod[key] = [
                        int(x.strip()) if x.strip().isdigit() else x.strip().strip("'\"")
                        for x in inner.split(",")
                        if x.strip()
                    ]
                elif val.isdigit():
                    prod[key] = int(val)
                else:
                    prod[key] = val.strip("'\"")
            products.append(prod)

    if not products:
        products = [dict(p) for p in EXPECTED_PRODUCTS]

    # Envelope keys
    envelope_keys = list(ENVELOPE_KEYS)
    ek = re.search(r"required_keys:\n((?:    - .*\n)+)", text)
    if ek:
        envelope_keys = re.findall(r"    - (\S+)", ek.group(1))

    return {
        "freeze_id": freeze_id,
        "status": status,
        "products": products,
        "envelope_keys": envelope_keys,
    }


def check_freeze_file(errors: list[str], freeze: dict) -> None:
    if freeze.get("status") != "frozen":
        fail(errors, f"freeze status must be 'frozen' (got {freeze.get('status')!r})")
    if freeze.get("freeze_id") != FREEZE_ID:
        fail(errors, f"unexpected freeze_id {freeze.get('freeze_id')!r}")

    products = freeze["products"]
    if len(products) != len(EXPECTED_PRODUCTS):
        fail(errors, f"expected {len(EXPECTED_PRODUCTS)} products, got {len(products)}")

    ports = sorted(int(p["port"]) for p in products)
    if ports != FROZEN_PORTS:
        fail(errors, f"ports must be {FROZEN_PORTS} uniquely, got {ports}")
    for port in RETIRED_PORTS:
        if port in ports:
            fail(errors, f"retired port {port} must not be reassigned in {FREEZE_ID}")

    schemas = [p["schema_version"] for p in products]
    if len(set(schemas)) != len(products):
        fail(errors, f"schema_version values must be unique: {schemas}")

    # Hardcoded mirror must match freeze (prevents silent drift of EXPECTED_PRODUCTS)
    for expected, product in zip(EXPECTED_PRODUCTS, sorted(products, key=lambda p: p["port"])):
        if (
            expected["port"] != product["port"]
            or expected["schema_version"] != product["schema_version"]
            or expected["repository"] != product["repository"]
        ):
            fail(
                errors,
                f"EXPECTED_PRODUCTS drift at port {expected['port']}: "
                f"code mirror {expected} vs freeze {product}",
            )

    for key in ENVELOPE_KEYS:
        if key not in freeze["envelope_keys"]:
            fail(errors, f"freeze envelope missing required key {key}")

    # Freeze file must mention policy defaults
    text = read(FREEZE_PATH)
    for needle in (
        "FREE_ONLY: true",
        "ALLOW_PAID_PROVIDERS: false",
        "ALLOW_EXTERNAL_WRITES: false",
        "ALLOW_REFRESH: false",
        "POST",
        "/v1/refresh",
        "403",
    ):
        if needle not in text:
            fail(errors, f"freeze YAML missing {needle!r}")


def check_producer(errors: list[str], product: dict) -> None:
    declared_path = product.get("path")
    if not isinstance(declared_path, str) or not declared_path.strip():
        fail(errors, f"{product['id']}: producer path is required in the frozen contract")
        root = PRODUCER_ROOT / product["repository"]
    else:
        root = PROJECT_ROOT / declared_path
        try:
            root.resolve().relative_to(
                (PROJECT_ROOT / "projects/product/engineering/book-dev").resolve()
            )
        except ValueError:
            fail(
                errors,
                f"{product['id']}: producer path must stay under "
                "projects/product/engineering/book-dev/",
            )
    if not root.is_dir():
        report_missing(errors, f"producer missing: {root.relative_to(PROJECT_ROOT)}", root)
        return

    package = product["package"]
    config_path = root / "src" / package / "config.py"
    store_path = root / "src" / package / "store.py"
    api_path = root / "src" / package / "api.py"
    for path in (config_path, store_path, api_path):
        if not path.is_file():
            report_missing(errors, f"missing {path.relative_to(PROJECT_ROOT)}", path)
            return

    config_text = read(config_path)
    store_text = read(store_path)
    api_text = read(api_path)

    schema = extract_assignment(config_text, "SCHEMA_VERSION")
    if schema != product["schema_version"]:
        fail(
            errors,
            f"{product['id']}: SCHEMA_VERSION={schema!r} expected {product['schema_version']!r}",
        )

    port = extract_port_default(config_text)
    if port != product["port"]:
        fail(errors, f"{product['id']}: API_PORT default={port!r} expected {product['port']}")

    for flag, expected in (
        ("FREE_ONLY", True),
        ("ALLOW_PAID_PROVIDERS", False),
        ("ALLOW_EXTERNAL_WRITES", False),
        ("ALLOW_REFRESH", False),
    ):
        got = extract_env_bool_default(config_text, flag)
        if got is None:
            fail(errors, f"{product['id']}: missing {flag} default in config.py")
        elif got != expected:
            fail(errors, f"{product['id']}: {flag} default={got} expected {expected}")

    # Lake-first products may delegate envelope() to shared product_store helpers
    # (kwargs only, no local string literals). Accept either pattern.
    uses_shared_envelope = (
        "product_store" in store_text
        or "_ps_mod()" in store_text
        or "from data_lake" in store_text
        or "data_lake.product_store" in store_text
    )
    if not uses_shared_envelope:
        for key in ENVELOPE_KEYS:
            if f'"{key}"' not in store_text and f"'{key}'" not in store_text:
                fail(errors, f"{product['id']}: store.py missing envelope key {key!r}")
    else:
        if "def envelope" not in store_text:
            fail(errors, f"{product['id']}: store.py missing envelope() when using shared product_store")

    if "def do_GET" not in api_text:
        fail(errors, f"{product['id']}: api.py missing do_GET")
    if "def do_POST" not in api_text:
        fail(errors, f"{product['id']}: api.py missing do_POST")
    if "/v1/refresh" not in api_text:
        fail(errors, f"{product['id']}: api.py missing /v1/refresh")
    if "403" not in api_text:
        fail(errors, f"{product['id']}: api.py missing 403 for refresh")
    if re.search(r"ALLOW_REFRESH\s*=\s*True", config_text):
        fail(errors, f"{product['id']}: ALLOW_REFRESH must default False")

    for path in ("/healthz", "/v1/metadata", "/v1/records", "/v1/history"):
        if path not in api_text:
            fail(errors, f"{product['id']}: api.py missing path {path}")


def check_catalog_file(
    errors: list[str],
    *,
    name: str,
    root: Path,
    file_rel: str,
    products: list[dict],
) -> None:
    full = root / file_rel
    if not full.is_file():
        report_missing(errors, f"{name}: missing {file_rel}", full)
        return
    text = read(full)
    for product in products:
        if product["schema_version"] not in text:
            fail(errors, f"{name}: {file_rel} missing schema {product['schema_version']}")
        port = product["port"]
        if (
            f"127.0.0.1:{port}" not in text
            and f"port: {port}" not in text
            and f'"port": {port}' not in text
            and f"'port': {port}" not in text
            and not re.search(rf"\bport\b[^\n]{{0,40}}\b{port}\b", text)
        ):
            fail(errors, f"{name}: {file_rel} missing port {port}")


def check_consumers(errors: list[str], products: list[dict]) -> None:
    by_port = {p["port"]: p for p in products}
    for spec in CONSUMER_SPECS:
        root = PROJECT_ROOT / spec["path"]
        if not root.is_dir():
            report_missing(errors, f"consumer missing: {spec['path']}", root)
            continue
        expected = [by_port[p] for p in spec["ports"] if p in by_port]
        for file_rel in spec["catalog_files"]:
            check_catalog_file(
                errors,
                name=spec["id"],
                root=root,
                file_rel=file_rel,
                products=expected,
            )
def check_versioning_docs(errors: list[str]) -> None:
    policy = PROJECT_ROOT / "systems/architecture/data-product-versioning-policy.md"
    portfolio = PROJECT_ROOT / "systems/architecture/data-product-portfolio.md"
    if not policy.is_file():
        fail(errors, "missing systems/architecture/data-product-versioning-policy.md")
    else:
        text = read(policy)
        for needle in (
            "breaking change",
            "schema_version",
            "data-product-contracts-v4.yaml",
            "FREE_ONLY",
            "8101",
            "8106",
        ):
            if needle not in text:
                fail(errors, f"versioning policy missing {needle!r}")
    if portfolio.is_file():
        text = read(portfolio)
        if "data-product-versioning-policy" not in text and "data-product-contracts-v1" not in text:
            fail(errors, "data-product-portfolio.md must link to contracts/versioning freeze")


def check_job_contract(errors: list[str]) -> None:
    """Validate Track B without changing the frozen v4 generation (8101–8104, 8106)."""
    contract_path = PROJECT_ROOT / "systems/architecture/job-data-contract-v1.yaml"
    if not contract_path.is_file():
        fail(errors, "missing systems/architecture/job-data-contract-v1.yaml")
    else:
        text = read(contract_path)
        for needle in ("source: book-job-data", "schema_version: job.v1", "port: 8109", "file_capture_only", "bronze_duckdb"):
            if needle not in text:
                fail(errors, f"job-data contract missing {needle!r}")

    root = PRODUCER_ROOT / "book-job-data"
    config_path = root / "src/book_job_data/config.py"
    api_path = root / "src/book_job_data/api.py"
    store_path = root / "src/book_job_data/store.py"
    for path in (config_path, api_path, store_path):
        if not path.is_file():
            fail(errors, f"job-data missing {path.relative_to(PROJECT_ROOT)}")
    if config_path.is_file():
        text = read(config_path)
        if extract_assignment(text, "SCHEMA_VERSION") != "job.v1":
            fail(errors, "job-data: SCHEMA_VERSION must be job.v1")
        if extract_port_default(text) != 8109:
            fail(errors, "job-data: API_PORT default must be 8109")
    if api_path.is_file():
        text = read(api_path)
        for path in ("/healthz", "/v1/metadata", "/v1/records", "/v1/history", "/v1/refresh"):
            if path not in text:
                fail(errors, f"job-data: api.py missing {path}")
        if "lake_first_job_bronze_duckdb" not in text or "403" not in text:
            fail(errors, "job-data: API must be Bronze-backed and refresh-forbidden")

    for rel in JOB_CONSUMER_FILES:
        path = PROJECT_ROOT / rel
        if not path.is_file():
            report_missing(errors, f"job consumer missing: {rel}", path)
            continue
        text = read(path)
        if "job_client" not in text:
            fail(errors, f"{rel}: must use infra/scripts/utils/job_client.py")
        for fragment in JOB_CSV_FRAGMENTS:
            if _contains_csv_reference(text, fragment):
                fail(errors, f"{rel}: must not open job capture/projection {fragment!r}")
        if "opportunity.v1" in text or "8108" in text:
            fail(errors, f"{rel}: Track B consumer must not use opportunity.v1 / :8108")

    # Production-side scan: scraper-owned CSVs are allowed only inside the
    # book-job-scraping repository and in this guard's own ban-list constants.
    # Tests may mention the names to assert the boundary, but they are not
    # runtime consumers and are intentionally outside these roots.
    for root_rel in JOB_CSV_SCAN_ROOTS:
        root = PROJECT_ROOT / root_rel
        if not root.is_dir():
            continue
        for path in root.rglob("*.py"):
            if any(part in {".git", "__pycache__", "node_modules", "archive"} for part in path.parts):
                continue
            rel = str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
            if rel in JOB_CSV_SCAN_ALLOWLIST:
                continue
            text = read(path)
            for fragment in JOB_CSV_FRAGMENTS:
                if _contains_csv_reference(text, fragment):
                    fail(
                        errors,
                        f"{rel}: external job consumer must use job_client.py; "
                        f"found direct job CSV reference {fragment!r}",
                    )


def main() -> int:
    errors: list[str] = []
    print("=" * 60)
    print("  Solo Empire — Data Product Contract Guard (v4 freeze)")
    print("=" * 60)

    try:
        freeze = parse_freeze_yaml()
    except Exception as exc:
        print(f"  FAIL: cannot load freeze: {exc}")
        return 1

    check_freeze_file(errors, freeze)
    products = freeze["products"]
    for product in products:
        # ensure package field
        if "package" not in product:
            match = next((p for p in EXPECTED_PRODUCTS if p["port"] == product["port"]), None)
            if match:
                product["package"] = match["package"]
                product["repository"] = match["repository"]
        check_producer(errors, product)

    check_consumers(errors, products)
    check_versioning_docs(errors)
    check_job_contract(errors)

    print(f"  Freeze: {FREEZE_PATH.relative_to(PROJECT_ROOT)}")
    print(f"  Products: {len(products)}")
    print(f"  Consumer specs: {len(CONSUMER_SPECS)}")
    print(f"  Track B job consumers: {len(JOB_CONSUMER_FILES)}")

    if errors:
        print()
        print(f"  FAIL ({len(errors)}):")
        for err in errors:
            print(f"    - {err}")
        print("=" * 60)
        return 1

    print("  Result: OK — v4 contracts frozen and compatible")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
