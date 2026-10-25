#!/usr/bin/env python3
"""Tests for shared data-product lake adapter."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.product_adapter import (  # noqa: E402
    LakeProductContract,
    ingest_to_lake,
    landing_object_bytes,
    list_bronze_parquet_files,
    read_bronze_rows,
    remote_bronze_dataset_glob,
    select_latest_bronze_rows,
    write_lineage,
)
from data_lake.parquet import BRONZE_COLUMNS, write_bronze_parquet  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402
from data_lake.product_store import (  # noqa: E402
    load_bronze_dataset,
    storage_model_for_source_kind,
)


def _skip_unless_runtime(available: bool, reason: str):
    if not available and os.environ.get("SOLO_EMPIRE_CI") == "1":
        raise RuntimeError(f"{reason}; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
    return unittest.skipUnless(available, reason)


@_skip_unless_runtime(
    __import__("importlib.util").util.find_spec("pyarrow") is not None,
    "pyarrow required",
)
class ProductAdapterTests(unittest.TestCase):
    def test_stock_and_crypto_share_domain_market(self):
        crypto = LakeProductContract(
            source="book-crypto-data",
            domain="market",
            product_schema_version="crypto.v1",
            project_root=ROOT,
        )
        stock = LakeProductContract(
            source="book-stock-data",
            domain="market",
            product_schema_version="stock.v1",
            project_root=ROOT,
        )
        self.assertEqual(crypto.domain, stock.domain)
        self.assertNotEqual(crypto.source, stock.source)

    def test_ingest_and_replay_landing(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            data_dir = Path(tmp) / "proj"
            data_dir.mkdir()
            contract = LakeProductContract(
                source="book-stock-data",
                domain="market",
                product_schema_version="stock.v1",
                project_root=ROOT,
                data_lake_uri=lake_uri,
            )
            raw = b'{"symbol":"AAPL","price":1}'
            records = [
                {
                    "id": "AAPL",
                    "symbol": "AAPL",
                    "price": 1,
                    "event_time": "2026-08-07T00:00:00Z",
                }
            ]
            result = ingest_to_lake(
                contract,
                raw=raw,
                records=records,
                dataset="stock_prices",
                data_lake_uri=lake_uri,
                provider="yahoo_public",
            )
            self.assertEqual(result["status"], "success")
            self.assertEqual(landing_object_bytes(lake_uri, result["raw_key"]), raw)
            lineage = write_lineage(
                contract, result, dataset="stock_prices", data_dir=data_dir
            )
            body = json.loads(lineage.read_text(encoding="utf-8"))
            self.assertEqual(body["source"], "book-stock-data")
            parts = list_bronze_parquet_files(contract, "stock_prices", data_lake_uri=lake_uri)
            self.assertEqual(len(parts), 1)
            rows = read_bronze_rows(contract, "stock_prices", data_lake_uri=lake_uri)
            self.assertEqual(len(select_latest_bronze_rows(rows)), 1)

            payload = load_bronze_dataset(
                contract,
                "stock_prices",
                data_lake_uri=lake_uri,
                latest_only=True,
                id_fields=["symbol"],
            )
            self.assertEqual(payload["source_kind"], "bronze_parquet")
            self.assertEqual(len(payload["items"]), 1)

    def test_remote_bronze_read_uses_bounded_s3_glob(self):
        contract = LakeProductContract(
            source="book-job-data",
            domain="jobs",
            product_schema_version="job.v1",
            project_root=ROOT,
            bronze_schema_version="1",
        )
        lake_uri = "s3://portfolio/prefix"
        captured: dict[str, object] = {}

        def fake_query(paths, sql):
            captured["paths"] = paths
            captured["sql"] = sql
            return [{"source_record_id": "job-1", "payload_json": '{"title":"DE"}'}]

        with patch(
            "data_lake.product_adapter.load_query_runtime",
            return_value=(fake_query, RuntimeError, ROOT),
        ):
            rows = read_bronze_rows(
                contract, "job_postings", data_lake_uri=lake_uri, require_committed=False
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(
            captured["paths"],
            remote_bronze_dataset_glob(
                contract, "job_postings", data_lake_uri=lake_uri
            ),
        )
        self.assertIn("ingest_date=*/source=*/part-*.parquet", str(captured["paths"]))

    def test_remote_store_reports_cloud_bronze_source(self):
        contract = LakeProductContract(
            source="book-job-data",
            domain="jobs",
            product_schema_version="job.v1",
            project_root=ROOT,
        )
        with patch(
            "data_lake.product_store.read_bronze_rows",
            return_value=[
                {
                    "source_record_id": "job-1",
                    "event_id": "event-1",
                    "event_time": "2026-08-08T00:00:00Z",
                    "payload_json": '{"id":"job-1","title":"DE"}',
                }
            ],
        ):
            payload = load_bronze_dataset(
                contract,
                "job_postings",
                data_lake_uri="s3://portfolio/prefix",
                latest_only=True,
                id_fields=["id"],
            )

        self.assertEqual(payload["source_kind"], "bronze_s3_parquet")
        self.assertEqual(storage_model_for_source_kind(payload["source_kind"], product="job"), "lake_first_job_bronze_s3_duckdb")
        self.assertEqual(len(payload["items"]), 1)

    def test_latest_row_compares_timezone_offsets_by_instant(self):
        rows = [
            {
                "source_record_id": "same",
                "event_time": "2026-09-08T09:00:00+07:00",
                "received_at": "2026-09-08T02:00:00Z",
                "event_id": "older-instant",
                "payload_json": '{"version":"older"}',
            },
            {
                "source_record_id": "same",
                "event_time": "2026-09-08T03:00:00Z",
                "received_at": "2026-09-08T03:00:00Z",
                "event_id": "newer-instant",
                "payload_json": '{"version":"newer"}',
            },
        ]
        self.assertEqual(select_latest_bronze_rows(rows)[0]["event_id"], "newer-instant")

    def test_bronze_reader_ignores_part_without_final_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            lake_uri = str(Path(tmp) / "lake")
            contract = LakeProductContract(
                source="book-stock-data",
                domain="market",
                product_schema_version="stock.v1",
                project_root=ROOT,
            )
            ingest_to_lake(
                contract,
                raw=b'{"symbol":"AAPL","price":1}',
                records=[{"id": "AAPL", "symbol": "AAPL", "price": 1}],
                dataset="stock_prices",
                data_lake_uri=lake_uri,
            )
            orphan = {
                column: None for column in BRONZE_COLUMNS
            }
            orphan.update(
                {
                    "event_id": "orphan",
                    "source": "book-stock-data",
                    "source_record_id": "ORPHAN",
                    "domain": "market",
                    "dataset": "stock_prices",
                    "schema_version": "1",
                    "received_at": "2026-08-07T00:00:00Z",
                    "payload_json": '{"id":"ORPHAN","symbol":"ORPHAN","price":2}',
                    "metadata_json": "{}",
                    "ingest_run_id": "orphan",
                    "privacy_class": "public",
                    "retention_class": "operational",
                }
            )
            with tempfile.TemporaryDirectory() as output:
                path = Path(output) / "orphan.parquet"
                write_bronze_parquet([orphan], path)
                ObjectStore(lake_uri).put_bytes(
                    "bronze/domain=market/dataset=stock_prices/schema_version=1/"
                    "ingest_date=2026-08-07/source=book-stock-data/part-orphan.parquet",
                    path.read_bytes(),
                )
            committed = read_bronze_rows(contract, "stock_prices", data_lake_uri=lake_uri)
            all_parts = read_bronze_rows(
                contract, "stock_prices", data_lake_uri=lake_uri, require_committed=False
            )
            self.assertEqual(len(committed), 1)
            self.assertEqual(len(all_parts), 2)


class _FakeRemoteStore:
    """In-memory stand-in for an s3:// ObjectStore that records reads and overlap."""

    manifests: dict[str, bytes] = {}
    missing: set[str] = set()
    broken: set[str] = set()
    reads: list[str] = []
    active = 0
    peak = 0
    lock = __import__("threading").Lock()

    def __init__(self, uri: str):
        self.uri = uri

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(key for key in self.manifests if key.startswith(prefix))

    def get_bytes(self, key: str) -> bytes:
        from data_lake.storage import ObjectNotFound, StorageError

        cls = type(self)
        with cls.lock:
            cls.reads.append(key)
            cls.active += 1
            cls.peak = max(cls.peak, cls.active)
        try:
            __import__("time").sleep(0.01)
            if key in cls.missing:
                raise ObjectNotFound(key)
            if key in cls.broken:
                raise StorageError(key)
            return cls.manifests[key]
        finally:
            with cls.lock:
                cls.active -= 1


class CommittedManifestReadTests(unittest.TestCase):
    """Remote lakes keep one manifest per ingest, so reads must not grow one process at a time."""

    contract = LakeProductContract(
        source="book-news-scraping", domain="news", product_schema_version="news.v1", project_root=ROOT
    )

    def setUp(self) -> None:
        from data_lake import product_adapter

        product_adapter._MANIFEST_CACHE.clear()
        _FakeRemoteStore.manifests = {}
        _FakeRemoteStore.missing = set()
        _FakeRemoteStore.broken = set()
        _FakeRemoteStore.reads = []
        _FakeRemoteStore.peak = 0
        for index in range(20):
            for dataset in ("articles", "articles_history"):
                key = f"control/manifests/source=book-news-scraping/{dataset}-{index}.json"
                _FakeRemoteStore.manifests[key] = json.dumps({
                    "source": "book-news-scraping", "domain": "news", "schema_version": "1", "dataset": dataset,
                    "bronze": {"key": f"bronze/domain=news/dataset={dataset}/schema_version=1/part-{index}.parquet"},
                }).encode()

    def committed(self, dataset: str, uri: str = "s3://bucket/lake") -> set[str]:
        from data_lake.product_adapter import _committed_bronze_keys

        with patch("data_lake.product_adapter.ObjectStore", _FakeRemoteStore):
            return _committed_bronze_keys(self.contract, dataset, data_lake_uri=uri)

    def test_reads_run_in_parallel_and_each_manifest_is_read_once_per_lake(self) -> None:
        articles = self.committed("articles")
        history = self.committed("articles_history")
        self.assertEqual(len(articles), 20)
        self.assertEqual(len(history), 20)
        self.assertTrue(all("dataset=articles/" in key for key in articles))
        self.assertEqual(len(_FakeRemoteStore.reads), 40)  # the second dataset reuses the cached manifests
        self.assertGreater(_FakeRemoteStore.peak, 1)

    def test_cache_is_per_lake(self) -> None:
        self.committed("articles", "s3://bucket/lake-a")
        self.committed("articles", "s3://bucket/lake-b")
        self.assertEqual(len(_FakeRemoteStore.reads), 80)

    def test_missing_manifest_is_skipped_and_broken_one_fails_closed(self) -> None:
        from data_lake.product_adapter import LakeIngestError

        first = "control/manifests/source=book-news-scraping/articles-0.json"
        _FakeRemoteStore.missing = {first}
        self.assertEqual(len(self.committed("articles")), 19)
        _FakeRemoteStore.broken = {"control/manifests/source=book-news-scraping/articles-1.json"}
        with self.assertRaises(LakeIngestError):
            self.committed("articles", "s3://bucket/other")


if __name__ == "__main__":
    unittest.main()
