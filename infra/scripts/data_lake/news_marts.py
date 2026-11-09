#!/usr/bin/env python3
"""News Silver → dbt marts → Gold, as one bounded, replayable run.

Same shape as jobs_marts.py (docs/specs/news-marts.md): build (or re-find) the
Silver snapshot of the current news Bronze, run the dbt news models against
that one part in a scratch DuckDB file, and publish every mart as an immutable
Gold Parquet part, one Gold manifest and one publish pointer. A rerun over the
same Bronze, models and writer versions writes nothing new.
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

from data_lake.jobs_marts import _export_mart, _run_dbt, _writer_versions  # noqa: E402
from data_lake.product_adapter import default_data_lake_uri  # noqa: E402
from data_lake.silver import SilverProductContract, _canonical_json, transform_bronze_to_silver  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

DBT_PROJECT_DIR = ROOT / "infra" / "dbt"
GOLD_SCHEMA_VERSION = "1"
GOLD_SOURCE = "book-news-scraping"
GOLD_DOMAIN = "news"
GOLD_MANIFEST_PREFIX = f"control/manifests/gold/source={GOLD_SOURCE}/domain={GOLD_DOMAIN}/"
GOLD_POINTER_PREFIX = f"control/pointers/gold/source={GOLD_SOURCE}/domain={GOLD_DOMAIN}/"
TRANSFORM_VERSION = "news-articles-silver.v1"
# The four approved feeds (run_news_capture.NEWS_FEEDS); a missing one is stale.
EXPECTED_PUBLISHERS = ("Bangkok Post Business", "Blognone", "Matichon", "Notebookspec")
# Mart name → columns that order its rows, so exported bytes are stable.
NEWS_MARTS: dict[str, tuple[str, ...]] = {
    "news_articles_daily": ("published_date", "publisher", "section"),
    "news_feed_health": ("publisher",),
    "news_latest_articles": ("publisher", "rank"),
}
DBT_SELECTOR = "+path:models/marts/news"

def news_silver_contract(max_invalid_ratio: float = 0.02) -> SilverProductContract:
    """Silver contract for news articles (docs/specs/news-marts.md item 1)."""
    return SilverProductContract(
        source=GOLD_SOURCE,
        domain=GOLD_DOMAIN,
        bronze_dataset="articles",
        silver_dataset="articles",
        product_schema_version="news.silver.v1",
        bronze_product_schema_version="news.v1",
        transform_version=TRANSFORM_VERSION,
        normalizer="news_articles",
        required_fields=("source_record_id", "headline", "canonical_url", "published_at"),
        privacy_class="public",
        max_invalid_ratio=max_invalid_ratio,
    )


def _model_fingerprint() -> str:
    """Hash of every file that shapes the marts, so a model change gets a new run."""
    digest = hashlib.sha256()
    paths = sorted(
        [
            *(DBT_PROJECT_DIR / "models" / "marts" / "news").glob("*"),
            DBT_PROJECT_DIR / "models" / "staging" / "stg_news_articles.sql",
            DBT_PROJECT_DIR / "macros" / "unique_combination.sql",
            DBT_PROJECT_DIR / "tests" / "news_marts_quality.sql",
            # Holds news_timezone, which changes every date column.
            DBT_PROJECT_DIR / "dbt_project.yml",
        ]
    )
    for path in paths:
        digest.update(path.relative_to(DBT_PROJECT_DIR).as_posix().encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _freshness(database: Path, now: datetime, stale_hours: float) -> dict[str, Any]:
    import duckdb  # type: ignore

    con = duckdb.connect(str(database), read_only=True)
    try:
        rows = dict(con.sql("select publisher, last_seen_at from news_feed_health").fetchall())
    finally:
        con.close()
    seen: dict[str, str | None] = {}
    stale: list[str] = []
    for publisher in EXPECTED_PUBLISHERS:
        last = rows.get(publisher)
        if last is None:
            seen[publisher] = None
            stale.append(publisher)
            continue
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        seen[publisher] = last.isoformat()
        if (now - last).total_seconds() / 3600 > stale_hours:
            stale.append(publisher)
    return {
        "last_seen_at": seen,
        "stale_publishers": sorted(stale),
        "stale": bool(stale),
        "stale_hours": stale_hours,
    }


def run_news_marts(
    *,
    data_lake_uri: str = "",
    max_invalid_ratio: float = 0.02,
    stale_hours: float = 6.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build Silver for the current news Bronze, then the news marts, then Gold."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    uri = default_data_lake_uri(data_lake_uri=data_lake_uri)
    silver = transform_bronze_to_silver(
        news_silver_contract(max_invalid_ratio),
        data_lake_uri=uri,
    )
    store = ObjectStore(uri)
    gold_run_id = "gold-" + hashlib.sha256(
        _canonical_json(
            {
                "silver_run_id": silver["silver_run_id"],
                "models": _model_fingerprint(),
                "writers": _writer_versions(),
            }
        ).encode("utf-8")
    ).hexdigest()[:24]

    with tempfile.TemporaryDirectory(prefix="solo-empire-news-marts-") as temp_dir:
        workdir = Path(temp_dir)
        silver_part = workdir / "silver.parquet"
        silver_part.write_bytes(store.get_bytes(silver["silver_key"]))
        dbt_result = _run_dbt(silver_part, workdir, selector=DBT_SELECTOR, silver_var="news_silver_part")
        database = dbt_result["database"]
        freshness = _freshness(database, now, stale_hours)

        marts: dict[str, dict[str, Any]] = {}
        for mart, order_by in NEWS_MARTS.items():
            local = workdir / f"{mart}.parquet"
            rows = _export_mart(database, mart, order_by, local)
            key = (
                f"gold/domain={GOLD_DOMAIN}/dataset={mart}/schema_version={GOLD_SCHEMA_VERSION}/"
                f"part-{gold_run_id}.parquet"
            )
            put = store.put_bytes(key, local.read_bytes(), content_type="application/vnd.apache.parquet")
            marts[mart] = {"key": put.key, "sha256": put.sha256, "rows": rows, "existed": put.existed}

    manifest = {
        "manifest_version": "1",
        "layer": "gold",
        "source": GOLD_SOURCE,
        "domain": GOLD_DOMAIN,
        "schema_version": GOLD_SCHEMA_VERSION,
        "gold_run_id": gold_run_id,
        "input": {
            "layer": "silver",
            "silver_run_id": silver["silver_run_id"],
            "silver_key": silver["silver_key"],
            "record_count": silver["record_count"],
        },
        "marts": [
            {"dataset": name, "key": mart["key"], "sha256": mart["sha256"], "rows": mart["rows"]}
            for name, mart in marts.items()
        ],
        "dbt": {"selector": DBT_SELECTOR, "statuses": dbt_result["statuses"]},
        "privacy_class": "public",
    }
    manifest_put = store.put_bytes(
        f"{GOLD_MANIFEST_PREFIX}gold_run_id={gold_run_id}.json",
        (_canonical_json(manifest) + "\n").encode("utf-8"),
        content_type="application/json",
    )
    # Write-once store: readers find the newest run through a key that sorts by
    # publish time (same scheme as docs/specs/jobs-gold-pointer.md).
    pointer = {
        "pointer_version": "1",
        "layer": "gold",
        "source": GOLD_SOURCE,
        "domain": GOLD_DOMAIN,
        "published_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "gold_run_id": gold_run_id,
        "manifest_key": manifest_put.key,
        "silver_run_id": silver["silver_run_id"],
        "silver_key": silver["silver_key"],
        "silver_record_count": silver["record_count"],
    }
    # A replay of unchanged inputs writes nothing: when the newest pointer
    # already names this Gold run, it is reused (spec item 5). Only a new Gold
    # run publishes a new pointer.
    existing = store.list_keys(GOLD_POINTER_PREFIX)
    newest = max(existing) if existing else ""
    if newest and f"-{gold_run_id}-" in newest.rsplit("/", 1)[-1]:
        pointer_key, pointer_existed = newest, True
    else:
        body = (_canonical_json(pointer) + "\n").encode("utf-8")
        pointer_put = store.put_bytes(
            f"{GOLD_POINTER_PREFIX}published_at={now.strftime('%Y%m%dT%H%M%SZ')}-{gold_run_id}-"
            f"{hashlib.sha256(body).hexdigest()[:12]}.json",
            body,
            content_type="application/json",
        )
        pointer_key, pointer_existed = pointer_put.key, pointer_put.existed
    quality = silver["quality"]
    return {
        "status": "stale" if freshness["stale"] else "success",
        "layer": "gold",
        "data_lake": store.describe(),
        "silver_run_id": silver["silver_run_id"],
        "gold_run_id": gold_run_id,
        "silver": {
            "key": silver["silver_key"],
            "existed": silver["silver_existed"],
            "record_count": silver["record_count"],
            "invalid_count": quality.get("invalid_count", 0),
            "excluded_count": quality.get("excluded_count", 0),
            "rejects_key": silver["rejects_key"],
        },
        "marts": marts,
        "manifest_key": manifest_put.key,
        "manifest_existed": manifest_put.existed,
        "pointer_key": pointer_key,
        "pointer_existed": pointer_existed,
        "dbt": manifest["dbt"],
        "freshness": freshness,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", ""))
    parser.add_argument("--max-invalid-ratio", type=float, default=0.02)
    parser.add_argument("--stale-hours", type=float, default=6.0)
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run_news_marts(
            data_lake_uri=args.data_lake_uri,
            max_invalid_ratio=args.max_invalid_ratio,
            stale_hours=args.stale_hours,
        )
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        payload = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(f"News marts: FAILED — {payload['error']}", file=sys.stderr)
        return 2
    if args.as_json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print(f"News marts: {result['status'].upper()}")
        print(f"  Silver: {result['silver']['record_count']} rows ({result['silver_run_id']})")
        for name, mart in result["marts"].items():
            print(f"  {name}: {mart['rows']} rows → {mart['key']}")
        print(f"  manifest: {result['manifest_key']}")
        print(f"  stale publishers: {result['freshness']['stale_publishers'] or 'none'}")
    # Stale data is still published (it is correct for its input), but the
    # exit code lets the scheduler's heartbeat report it.
    return 3 if result["status"] == "stale" else 0


if __name__ == "__main__":
    raise SystemExit(main())
