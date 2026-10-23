#!/usr/bin/env python3
"""Contract tests for replaying existing scraper captures into Bronze."""

from __future__ import annotations

import json
import socket
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import run_data_product_stack as stack
from data_lake.workspace import OWNER_ROOT
from data_lake.ingest_scraping_captures import (
    CAPTURES,
    _deduplicate_exact_records,
    _map_records,
    _records,
    ingest_capture,
)


class ScrapingCaptureIngestTests(unittest.TestCase):
    @unittest.skipUnless(OWNER_ROOT.is_dir(), "product repositories not found under SOLO_BOOK_DEV_ROOT")
    def test_product_workspaces_resolve_to_declared_book_dev_paths(self) -> None:
        for product in stack.PRODUCTS:
            with self.subTest(product=product.product):
                self.assertTrue(stack.product_workspace(product).is_dir())

    def test_stack_can_select_a_bounded_product_subset(self) -> None:
        selected = stack.select_products(["news,discovery", "news"])
        self.assertEqual(
            [product.product for product in selected], ["news", "discovery"]
        )

    def test_stack_rejects_unknown_product_selection(self) -> None:
        with self.assertRaises(ValueError):
            stack.select_products(["news,missing"])

    def test_all_frozen_products_have_snapshot_and_history_contracts(self) -> None:
        products = {spec.product for spec in CAPTURES}
        self.assertEqual(products, {"crypto", "stocks", "fx", "defi", "seo", "news", "property"})
        for product in products:
            datasets = {spec.dataset for spec in CAPTURES if spec.product == product}
            self.assertTrue(any(not spec.history for spec in CAPTURES if spec.product == product))
            self.assertTrue(any(spec.history for spec in CAPTURES if spec.product == product))
            self.assertEqual(len(datasets), 3 if product == "property" else 1 if product == "crypto" else 2)

    def test_history_identity_includes_temporal_projection_field(self) -> None:
        temporal_fields = {"date", "observed_at", "scraped_at", "timestamp"}
        for spec in CAPTURES:
            if spec.product in {"crypto", "stocks", "fx", "defi", "seo"} and spec.history:
                self.assertTrue(temporal_fields.intersection(spec.id_fields), spec)

    def test_csv_rows_get_stable_source_ids_without_changing_payload_fields(self) -> None:
        raw = b"coin_id,currency,price,updated_at\nbitcoin,usd,1,2026-09-07T00:00:00Z\n"
        records = _records(raw, ("coin_id", "currency"))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "bitcoin|usd")
        self.assertEqual(records[0]["coin_id"], "bitcoin")
        self.assertEqual(records[0]["updated_at"], "2026-09-07T00:00:00Z")

    def test_legacy_projection_timestamp_is_rejected_without_timezone(self) -> None:
        raw = b"coin_id,currency,updated_at\nbitcoin,usd,2026-09-07 12:00:00\n"
        with self.assertRaises(ValueError):
            _records(raw, ("coin_id", "currency"), "updated_at")

    def test_projection_timestamp_is_preserved_when_not_declared_as_event_time(self) -> None:
        raw = b"coin_id,currency,updated_at\nbitcoin,usd,2026-09-07 12:00:00\n"
        records = _records(raw, ("coin_id", "currency"))
        self.assertEqual(records[0]["updated_at"], "2026-09-07 12:00:00")

    def test_exact_duplicate_rows_are_replayed_once(self) -> None:
        records = [
            {"id": "THB|EUR|2026-09-11", "rate": "0.02609"},
            {"id": "THB|EUR|2026-09-11", "rate": "0.02609"},
            {"id": "THB|USD|2026-09-11", "rate": "0.03024"},
        ]
        self.assertEqual(_deduplicate_exact_records(records), [records[0], records[2]])

    def test_paths_use_exported_capture_boundary(self) -> None:
        for spec in CAPTURES:
            self.assertIn("/data/exported/", spec.path)
            self.assertTrue(spec.path.endswith(".csv"))

    def test_malformed_captures_fail_closed(self) -> None:
        for raw in (
            b"name,time\na,2026-09-07\n",
            b"id,time\n,2026-09-07\n",
            b"id,time\na,invalid\n",
            b"id,time\na,2026-09-07T12:00:00\n",
            b"id,time\na,2026-09-07,extra\n",
            b"id,time\na\n",
            b"id,id,time\na,b,2026-09-07\n",
        ):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                _records(raw, ("id",), "time")

    def test_csv_multiline_is_preserved(self) -> None:
        rows = _records(b'id,description\na,"one\ntwo"\n', ("id",))
        self.assertEqual(rows[0]["description"], "one\ntwo")

    def test_seo_capture_maps_to_owned_page_provenance_without_a_serp_rank(self) -> None:
        spec = next(spec for spec in CAPTURES if spec.product == "seo" and not spec.history)
        raw = (
            b"scraped_at,keyword,target_domain,location,device,provider,found,best_rank,rank,best_domain,best_url,result_url,best_title,result_title,observed_at,total_results\n"
            b"2026-09-07T00:00:00Z,python developer,example.com,global,desktop,public_page_check,false,,,example.com,,,Example,Example,2026-09-07T00:00:00Z,2\n"
        )
        records = _map_records(spec, _records(raw, spec.id_fields, spec.event_time_field))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["id"], "python developer|example.com|global|desktop")
        self.assertEqual(records[0]["capture_kind"], "owned_page_provenance")
        self.assertEqual(records[0]["keyword"], "python developer")
        self.assertEqual(records[0]["target_domain"], "example.com")
        self.assertEqual(records[0]["result_url"], "")
        self.assertEqual(records[0]["rank"], "")
        self.assertEqual(records[0]["found"], "false")

    def test_news_capture_maps_to_attributed_rss_article(self) -> None:
        spec = next(spec for spec in CAPTURES if spec.product == "news" and not spec.history)
        raw = (
            b"captured_at,article_id,title,url,source,source_url,published_at\n"
            b"2026-09-07T00:00:00Z,a-1,Headline,https://publisher.example/a,Publisher,https://publisher.example/feed,2026-09-06T00:00:00Z\n"
        )
        records = _map_records(spec, _records(raw, spec.id_fields, spec.event_time_field))
        self.assertEqual(records[0]["id"], "Publisher|a-1")
        self.assertEqual(records[0]["headline"], "Headline")
        self.assertEqual(records[0]["canonical_url"], "https://publisher.example/a")
        self.assertEqual(records[0]["publisher"], "Publisher")
        self.assertEqual(records[0]["capture_kind"], "rss_article")

    def test_replay_preserves_source_time_raw_bytes_and_identity(self) -> None:
        import pyarrow.parquet as pq
        raw = b"coin_id,currency,price,updated_at\nbitcoin,usd,1,2026-09-07T00:00:00Z\n"
        with tempfile.TemporaryDirectory() as directory:
            lake = Path(directory)
            spec = replace(CAPTURES[0], path="capture.csv")
            with patch("data_lake.ingest_scraping_captures.Path.open", create=True) as opened:
                import io
                opened.return_value.__enter__.side_effect = lambda: io.BytesIO(raw)
                # Patch only the capture read; storage must use its real files.
                with patch("data_lake.ingest_scraping_captures.ingest_payload") as ingest:
                    ingest.return_value = {}
                    ingest_capture(spec, lake_uri=directory)
                    passed_raw, records = ingest.call_args.args
            from data_lake.ingest import ingest_payload
            kwargs = dict(ingest.call_args.kwargs)
            first = ingest_payload(passed_raw, records, **kwargs)
            second = ingest_payload(passed_raw, records, **kwargs)
            self.assertEqual(first["run_id"], second["run_id"])
            self.assertTrue(second["manifest_existed"])
            self.assertEqual((lake / first["raw_key"]).read_bytes(), raw)
            row = pq.ParquetFile(lake / first["bronze_key"]).read().to_pylist()[0]
            self.assertIsNone(row["event_time"])
            self.assertEqual(json.loads(row["payload_json"])["price"], "1")

    def test_delta_replay_keeps_full_landing_bytes_but_commits_only_new_records(self) -> None:
        spec = next(spec for spec in CAPTURES if spec.product == "news" and spec.history)
        full_raw = (
            b"captured_at,article_id,title,url,source,source_url,published_at\n"
            b"2026-09-07T00:00:00Z,a-1,Old,https://publisher.example/a,Publisher,https://publisher.example/feed,2026-09-06T00:00:00Z\n"
            b"2026-09-07T01:00:00Z,a-2,New,https://publisher.example/b,Publisher,https://publisher.example/feed,2026-09-06T01:00:00Z\n"
        )
        appended_raw = (
            b"captured_at,article_id,title,url,source,source_url,published_at\n"
            b"2026-09-07T01:00:00Z,a-2,New,https://publisher.example/b,Publisher,https://publisher.example/feed,2026-09-06T01:00:00Z\n"
        )
        records = _map_records(spec, _records(appended_raw, spec.id_fields, spec.event_time_field))
        with patch("data_lake.ingest_scraping_captures.ingest_payload", return_value={}) as ingest:
            ingest_capture(
                spec,
                lake_uri="/tmp/news-delta-lake",
                raw_override=full_raw,
                records_override=records,
                capture_mode="append_delta",
            )
        passed_raw, passed_records = ingest.call_args.args
        self.assertEqual(passed_raw, full_raw)
        self.assertEqual(len(passed_records), 1)
        self.assertEqual(ingest.call_args.kwargs["metadata"]["capture_mode"], "append_delta")

    def test_stack_refuses_an_occupied_port_without_starting_children(self) -> None:
        with socket.socket() as occupied, tempfile.TemporaryDirectory() as directory:
            occupied.bind(("127.0.0.1", 0))
            product = replace(stack.PRODUCTS[0], port=occupied.getsockname()[1])
            with patch.object(stack.subprocess, "Popen") as popen:
                with self.assertRaises(OSError):
                    stack.start_products([product], lake_uri=directory, log_dir=Path(directory))
                popen.assert_not_called()

    @unittest.skipUnless(OWNER_ROOT.is_dir(), "product repositories not found under SOLO_BOOK_DEV_ROOT")
    def test_stack_cleans_up_partial_start_and_overrides_inherited_port(self) -> None:
        child = Mock()
        child.poll.return_value = None
        with tempfile.TemporaryDirectory() as directory, patch.object(stack.socket, "socket"), patch.object(stack.subprocess, "Popen", side_effect=[child, OSError()]) as popen:
            with self.assertRaises(OSError):
                stack.start_products(stack.PRODUCTS[:2], lake_uri=directory, log_dir=Path(directory))
            child.send_signal.assert_called_once()
            self.assertEqual(popen.call_args_list[0].kwargs["env"]["PORT"], "8101")

    def test_stack_blocks_before_children_when_job_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(
                stack,
                "check_job_readiness",
                return_value={"status": "attention", "alerts": [{"code": "JOB_DATA_FRESHNESS_STALE"}]},
            ) as readiness, patch.object(stack, "start_products") as start:
                result = stack.main(
                    [
                        "--check",
                        "--require-job-readiness",
                        "--lake-uri",
                        directory,
                    ]
                )
            self.assertEqual(result, 2)
            readiness.assert_called_once_with(
                directory,
                datasets=None,
                max_age_hours=None,
            )
            start.assert_not_called()

    def test_stack_starts_only_after_job_readiness_passes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(
                stack,
                "check_job_readiness",
                return_value={"status": "ok", "alerts": []},
            ) as readiness, patch.object(stack, "start_products", return_value=[]) as start, patch.object(
                stack, "check_health", return_value=[]
            ) as health:
                result = stack.main(
                    [
                        "--check",
                        "--require-job-readiness",
                        "--lake-uri",
                        directory,
                    ]
                )
            self.assertEqual(result, 0)
            readiness.assert_called_once_with(
                directory,
                datasets=None,
                max_age_hours=None,
            )
            start.assert_called_once()
            health.assert_called_once_with(stack.PRODUCTS)

    def test_stack_smoke_can_run_news_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(stack, "start_products", return_value=[]) as start, patch.object(
                stack, "check_health", return_value=[]
            ) as health:
                result = stack.main(
                    [
                        "--check",
                        "--products",
                        "news",
                        "--lake-uri",
                        directory,
                        "--startup-seconds",
                        "0.1",
                    ]
                )
            self.assertEqual(result, 0)
            start.assert_called_once()
            selected = start.call_args.args[0]
            self.assertEqual([product.product for product in selected], ["news"])
            health.assert_called_once_with(selected)


if __name__ == "__main__":
    unittest.main()
