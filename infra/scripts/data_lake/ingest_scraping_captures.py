#!/usr/bin/env python3
"""Replay existing scraper captures into the shared Bronze data lake.

The scraper repositories own collection and their local CSV files are
projections. This command makes the current captures useful to the existing
read-only product APIs without contacting upstream providers:

    capture CSV -> immutable landing bytes -> Bronze Parquet -> API -> UI

Each input is identified by its product contract. Re-running the command with
unchanged bytes is idempotent because the lake uses the capture checksum as
the batch identity. Coordinators may supply the full landing bytes together
with a validated record slice for append-only projections such as News history.
The event timestamps are preserved; an old capture is reported as stale by the
API instead of being made to look fresh.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.ingest import ingest_payload  # noqa: E402
from data_lake.workspace import product_path  # noqa: E402
from data_lake.product_adapter import LakeProductContract, write_lineage  # noqa: E402


@dataclass(frozen=True)
class CaptureSpec:
    product: str
    source: str
    domain: str
    schema_version: str
    dataset: str
    path: str
    id_fields: tuple[str, ...]
    event_time_field: str | None
    history: bool = False
    deduplicate_exact_rows: bool = False


CAPTURES: tuple[CaptureSpec, ...] = (
    CaptureSpec(
        "crypto",
        "book-crypto-data",
        "market",
        "crypto.v1",
        "crypto_prices",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-data/sources/crypto/data/exported/crypto_prices.csv",
        ("coin_id", "currency"),
        None,
    ),
    CaptureSpec(
        "crypto",
        "book-crypto-data",
        "market",
        "crypto.v1",
        "crypto_prices",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-data/sources/crypto/data/exported/crypto_history.csv",
        ("coin_id", "currency", "date"),
        None,
        True,
        True,
    ),
    CaptureSpec(
        "stocks",
        "book-stock-data",
        "market",
        "stock.v1",
        "stock_prices",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-data/sources/stock/data/exported/stock_prices.csv",
        ("symbol",),
        None,
    ),
    CaptureSpec(
        "stocks",
        "book-stock-data",
        "market",
        "stock.v1",
        "stock_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-data/sources/stock/data/exported/stock_history.csv",
        ("symbol", "timestamp", "scraped_at"),
        None,
        True,
        True,
    ),
    CaptureSpec(
        "fx",
        "book-fx-data",
        "market",
        "fx.v1",
        "exchange_rates",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-data/sources/fx/data/exported/exchange_rates.csv",
        ("base", "currency"),
        "date",
    ),
    CaptureSpec(
        "fx",
        "book-fx-data",
        "market",
        "fx.v1",
        "exchange_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-data/sources/fx/data/exported/exchange_history.csv",
        ("base", "currency", "date"),
        "date",
        True,
        True,
    ),
    CaptureSpec(
        "defi",
        "book-defi-data",
        "market",
        "defi.v1",
        "defi_yields",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-data/sources/defi/data/exported/defi_yields.csv",
        ("pool_id",),
        None,
    ),
    CaptureSpec(
        "defi",
        "book-defi-data",
        "market",
        "defi.v1",
        "defi_yields_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-products/market-data/sources/defi/data/exported/defi_yields_history.csv",
        ("pool_id", "scraped_at"),
        None,
        True,
        True,
    ),
    CaptureSpec(
        "flights",
        "book-flight-data",
        "travel",
        "flight.v1",
        "flight_prices",
        "projects/product/engineering/book-dev/github/bookchaowalit/private/book-products/travel-flight-price-data/data/exported/flight_prices.csv",
        ("origin", "destination", "departure"),
        "scraped_at",
    ),
    CaptureSpec(
        "flights",
        "book-flight-data",
        "travel",
        "flight.v1",
        "flight_prices_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/private/book-products/travel-flight-price-data/data/exported/flight_prices_history.csv",
        ("origin", "destination", "departure"),
        "date",
        True,
    ),
    CaptureSpec(
        "seo",
        "book-seo-data",
        "seo",
        "seo.v1",
        "seo_rankings",
        "projects/product/engineering/book-dev/github/bookchaowalit/private/book-products/marketing-seo-ranking-data/data/exported/seo_rankings.csv",
        ("keyword", "target_domain", "location", "device"),
        "observed_at",
    ),
    CaptureSpec(
        "seo",
        "book-seo-data",
        "seo",
        "seo.v1",
        "seo_rankings_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/private/book-products/marketing-seo-ranking-data/data/exported/seo_rankings_history.csv",
        ("keyword", "target_domain", "observed_at"),
        "observed_at",
        True,
        True,
    ),
    CaptureSpec(
        "news",
        "book-news-scraping",
        "news",
        "news.v1",
        "articles",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/content-news-scraper/data/exported/matichon_news.csv",
        ("source", "article_id"),
        "captured_at",
    ),
    CaptureSpec(
        "news",
        "book-news-scraping",
        "news",
        "news.v1",
        "articles_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/content-news-scraper/data/exported/matichon_news_history.csv",
        ("source", "article_id", "captured_at"),
        "captured_at",
        True,
        True,
    ),
    CaptureSpec(
        "news",
        "book-news-scraping",
        "news",
        "news.v1",
        "articles",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/content-news-scraper/data/exported/notebookspec_tech.csv",
        ("source", "article_id"),
        "captured_at",
    ),
    CaptureSpec(
        "news",
        "book-news-scraping",
        "news",
        "news.v1",
        "articles_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/content-news-scraper/data/exported/notebookspec_tech_history.csv",
        ("source", "article_id", "captured_at"),
        "captured_at",
        True,
        True,
    ),
    CaptureSpec(
        "news",
        "book-news-scraping",
        "news",
        "news.v1",
        "articles",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/content-news-scraper/data/exported/thai_business_news.csv",
        ("source", "article_id"),
        "captured_at",
    ),
    CaptureSpec(
        "news",
        "book-news-scraping",
        "news",
        "news.v1",
        "articles_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/content-news-scraper/data/exported/thai_business_news_history.csv",
        ("source", "article_id", "captured_at"),
        "captured_at",
        True,
        True,
    ),
    CaptureSpec(
        "news",
        "book-news-scraping",
        "news",
        "news.v1",
        "articles",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/content-news-scraper/data/exported/thai_tech_news.csv",
        ("source", "article_id"),
        "captured_at",
    ),
    CaptureSpec(
        "news",
        "book-news-scraping",
        "news",
        "news.v1",
        "articles_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/content-news-scraper/data/exported/thai_tech_news_history.csv",
        ("source", "article_id", "captured_at"),
        "captured_at",
        True,
        True,
    ),
    # Property captures remain held behind the source permission gate below.
    # They are registered here so --validate-only can reconcile headers and
    # lake-first lineage without silently enabling collection or publication.
    CaptureSpec(
        "property",
        "book-property-scraping",
        "property",
        "property.v1",
        "property_listings",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/career-job-scraper/data/exported/property_listings.csv",
        ("url",),
        "scraped_at",
    ),
    CaptureSpec(
        "property",
        "book-property-scraping",
        "property",
        "property.v1",
        "property_history",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/career-job-scraper/data/exported/property_history.csv",
        ("title", "type"),
        "date",
        True,
    ),
    CaptureSpec(
        "property",
        "book-property-scraping",
        "property",
        "property.v1",
        "property_social_leads",
        "projects/product/engineering/book-dev/github/bookchaowalit/public/book-apps/tools/career-job-scraper/data/exported/property_social_leads.csv",
        ("url",),
        "captured_at",
    ),
)


def _normalize_event_timestamp(value: str) -> str:
    """Validate event timestamps without inventing a timezone."""
    stamp = value.strip()
    try:
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("Invalid event timestamp") from None
    if len(stamp) == 10:
        return stamp
    if parsed.tzinfo is None:
        raise ValueError("Timestamp requires a timezone")
    return stamp


def _records(raw: bytes, id_fields: Iterable[str], event_time_field: str | None = None) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    fields = tuple(id_fields)
    headers = reader.fieldnames or []
    required = set(fields) | ({event_time_field} if event_time_field else set())
    if len(headers) != len(set(headers)) or not required.issubset(headers):
        raise ValueError("Capture headers do not match the replay contract")
    records: list[dict[str, Any]] = []
    for index, row in enumerate(reader, start=2):
        if None in row or any(value is None for value in row.values()):
            raise ValueError(f"Malformed CSV at row {index}")
        normalized = {str(key): value for key, value in row.items()}
        parts = [str(normalized[field]).strip() for field in fields]
        if not parts or any(not part or "|" in part for part in parts):
            raise ValueError(f"Missing or ambiguous identity at row {index}")
        if event_time_field:
            stamp = str(normalized[event_time_field]).strip()
            try:
                normalized[event_time_field] = _normalize_event_timestamp(stamp)
            except ValueError:
                raise ValueError(f"Invalid event timestamp at row {index}") from None
        normalized["id"] = "|".join(parts)
        records.append(normalized)
    return records


def _deduplicate_exact_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for record in records:
        fingerprint = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        unique.append(record)
    return unique


def _map_records(spec: CaptureSpec, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Map an approved capture to the domain product's published semantics.

    News captures retain the source fields while adding stable headline,
    canonical URL, publisher, and attribution markers for ``news.v1``.
    The SEO scheduler keeps the historical ``seo_rankings`` filename, but its
    approved source is an owned-page reachability check, not a SERP provider.
    Preserve the raw fields while filling the ``seo.v1`` provenance fields so
    consumers never mistake an empty rank for a collected ranking.
    """
    if spec.product == "news":
        mapped: list[dict[str, Any]] = []
        for row in records:
            item = dict(row)
            title = str(row.get("title") or "").strip()
            url = str(row.get("url") or "").strip()
            source = str(row.get("source") or "").strip()
            observed_at = str(row.get(spec.event_time_field or "") or "").strip()
            item.update(
                {
                    "headline": title,
                    "canonical_url": url,
                    "publisher": source,
                    "observed_at": observed_at,
                    "capture_kind": "rss_article",
                    "source_feed_url": str(row.get("source_url") or "").strip(),
                    "id": str(row.get("id") or ""),
                }
            )
            mapped.append(item)
        return mapped
    if spec.product != "seo":
        return records
    mapped = []
    for row in records:
        status_raw = str(row.get("http_status") or "").strip()
        try:
            status = int(status_raw)
        except ValueError:
            status = 0
        page_status = str(row.get("data_status") or "").strip().lower()
        found_raw = str(row.get("found") or "").strip()
        if status_raw or page_status:
            reachable = 200 <= status < 400 and page_status == "ok"
        else:
            reachable = found_raw.lower() in {"1", "true", "yes"}
        url = str(row.get("result_url") or row.get("best_url") or row.get("url") or "").strip()
        domain = str(row.get("target_domain") or row.get("domain") or row.get("best_domain") or "").strip().lower()
        keyword = str(row.get("keyword") or "").strip()
        location = str(row.get("location") or "global").strip() or "global"
        device = str(row.get("device") or "desktop").strip() or "desktop"
        provider = str(row.get("provider") or row.get("source") or "public_page_check").strip()
        observed_at = str(row.get(spec.event_time_field or "") or "").strip()
        item = dict(row)
        item.update(
            {
                "keyword": keyword or (f"owned-page|{url}" if url else "owned-page"),
                "target_domain": domain,
                "location": location,
                "device": device,
                "provider": provider,
                "found": found_raw or ("true" if reachable else "false"),
                "rank": str(row.get("rank") or ""),
                "best_rank": str(row.get("best_rank") or ""),
                "result_url": url,
                "best_url": str(row.get("best_url") or url).strip(),
                "result_title": str(row.get("result_title") or row.get("title") or "").strip(),
                "best_title": str(row.get("best_title") or row.get("result_title") or row.get("title") or "").strip(),
                "total_results": str(row.get("total_results") or ""),
                "observed_at": observed_at,
                "capture_kind": "owned_page_provenance" if provider == "public_page_check" else "serp_ranking",
                "page_http_status": status if status_raw else "",
                "page_canonical_url": str(row.get("canonical") or "").strip(),
                "page_content_length": str(row.get("content_length") or "").strip(),
                "page_data_status": page_status,
                "id": str(row.get("id") or ""),
            }
        )
        mapped.append(item)
    return mapped


def _lineage_contract(spec: CaptureSpec, lake_uri: str) -> LakeProductContract:
    return LakeProductContract(
        source=spec.source,
        domain=spec.domain,
        product_schema_version=spec.schema_version,
        privacy_class="public",
        retention_class="operational-plus-history" if spec.history else "operational",
        bronze_schema_version="1",
        project_root=ROOT,
        data_lake_uri=lake_uri,
        lineage_filename="lake_lineage.json",
        datasets=tuple(sorted({item.dataset for item in CAPTURES if item.product == spec.product})),
    )


def ingest_capture(
    spec: CaptureSpec,
    *,
    lake_uri: str,
    validate_only: bool = False,
    update_lineage: bool = False,
    raw_override: bytes | None = None,
    records_override: list[dict[str, Any]] | None = None,
    capture_mode: str | None = None,
) -> dict[str, Any]:
    """Validate or ingest one capture contract.

    ``raw_override`` and ``records_override`` let a coordinator preserve the
    exact full source bytes while committing only a safe record slice.  News
    history files are append-only projections, so the bounded News runner uses
    this to retain the full landing object and send only newly appended rows to
    Bronze.  Normal callers continue to read and map the capture file itself.
    """
    path = product_path(spec.path)
    if raw_override is None:
        with path.open("rb") as capture:
            raw = capture.read(32 * 1024 * 1024 + 1)
    else:
        raw = bytes(raw_override)
    if len(raw) > 32 * 1024 * 1024:
        raise ValueError("Capture exceeds 32 MiB replay limit")
    records = (
        list(records_override)
        if records_override is not None
        else _map_records(spec, _records(raw, spec.id_fields, spec.event_time_field))
    )
    if not records:
        raise ValueError(f"capture is empty: {spec.path}")
    raw_record_count = len(records)
    if spec.deduplicate_exact_rows:
        records = _deduplicate_exact_records(records)
    if validate_only:
        return {"product": spec.product, "dataset": spec.dataset, "record_count": len(records), "validated": True}
    metadata = {
        "adapter": spec.source,
        "capture_mapping_version": (
            "4" if spec.product in {"news", "seo"} else "3"
        ),
        "event_time_field": spec.event_time_field,
        "id_fields": list(spec.id_fields),
        "product": spec.product,
        "product_schema_version": spec.schema_version,
        "capture_path": spec.path,
        "dataset_role": "history" if spec.history else "snapshot",
        "raw_record_count": raw_record_count,
        "replay_record_count": len(records),
    }
    # Keep the legacy metadata shape for ordinary full-file replays.  The
    # append-delta marker is needed only when a coordinator deliberately maps
    # a slice of a cumulative source projection.
    if capture_mode and capture_mode != "snapshot":
        metadata["capture_mode"] = capture_mode
    result = ingest_payload(
        raw,
        records,
        source=spec.source,
        domain=spec.domain,
        dataset=spec.dataset,
        data_lake_uri=lake_uri,
        input_format="csv",
        schema_version="1",
        source_record_id_field="id",
        event_time_field=spec.event_time_field,
        content_type="text/csv",
        metadata=metadata,
        privacy_class="public",
        retention_class="operational-plus-history" if spec.history else "operational",
    )
    # Keep the product's ignored local lineage projection in sync so its
    # metadata endpoint can show the same lake run as the shared API read.
    if update_lineage:
        write_lineage(
            _lineage_contract(spec, lake_uri),
            result,
            dataset=spec.dataset,
            data_dir=product_path(spec.path).parents[1],
        )
    return {
        "product": spec.product,
        "dataset": spec.dataset,
        "capture_path": spec.path,
        "record_count": len(records),
        "run_id": result.get("run_id"),
        "raw_key": result.get("raw_key"),
        "bronze_key": result.get("bronze_key"),
        "manifest_key": result.get("manifest_key"),
        "raw_existed": result.get("raw_existed"),
        "bronze_existed": result.get("bronze_existed"),
        "manifest_existed": result.get("manifest_existed"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-uri", default=str(ROOT / "data" / "lake-local" / "scraping-captures-v2"))
    parser.add_argument("--product", action="append", choices=sorted({spec.product for spec in CAPTURES}))
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--validate-only", action="store_true", help="Validate captures without writing lake or lineage artifacts")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writes; report missing captures without failing")
    parser.add_argument("--update-lineage", action="store_true", help="Update product-local lineage projections after reconciliation")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # Conditional/blocked sources require a separate source-owner decision;
    # this operational replay never treats their presence as approval.
    # SEO is approved for owned-page provenance only; news is approved for
    # publisher RSS/Atom captures with attribution. Flight collection remains
    # held behind its source-owner gate. The AI tools product was retired in
    # data-product-contracts-v3 (2026-10-23) and has no capture spec.
    approved = {"crypto", "stocks", "fx", "defi", "seo", "news"}
    selected = set(args.product or approved)
    validate_only = args.validate_only or args.dry_run
    if not validate_only and selected - approved:
        print("Replay held: selected sources require source permission and consumer mapping review.", file=sys.stderr)
        return 1
    results: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    for spec in CAPTURES:
        if spec.product not in selected:
            continue
        path = product_path(spec.path)
        if not path.is_file():
            skipped.append({"product": spec.product, "dataset": spec.dataset, "path": spec.path})
            continue
        try:
            results.append(ingest_capture(spec, lake_uri=args.lake_uri, validate_only=validate_only, update_lineage=args.update_lineage))
        except (OSError, ValueError, RuntimeError) as exc:
            errors.append({"product": spec.product, "dataset": spec.dataset, "error": type(exc).__name__})
    summary = {"lake_uri": args.lake_uri, "validate_only": validate_only, "dry_run": args.dry_run, "results": results, "skipped": skipped, "errors": errors}
    if args.as_json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"Scraping capture replay: {len(results)} dataset(s) {'validated' if validate_only else 'committed'}")
        for item in results:
            state = "validated" if validate_only else "existing" if item["manifest_existed"] else "new"
            print(f"  {item['product']}/{item['dataset']}: {item['record_count']} record(s), {state}")
        for item in skipped:
            print(f"  skipped missing: {item['product']}/{item['dataset']}")
        for item in errors:
            print(f"  failed: {item['product']}/{item['dataset']} ({item['error']}); immutable prior mappings require an isolated replay/reconciliation, not overwrite")
    # A dry-run is a non-writing readiness plan: missing local captures are
    # reported in the JSON/text result but do not make the plan command fail.
    return 1 if errors or (not args.dry_run and (skipped or not results)) else 0


if __name__ == "__main__":
    raise SystemExit(main())
