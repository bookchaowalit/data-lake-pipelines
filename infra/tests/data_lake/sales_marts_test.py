#!/usr/bin/env python3
"""Tests for the sales Bronze → dbt marts → Gold runner (docs/specs/sales-marts.md items 9–10)."""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_lake.query import query_parquet  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

NOW = datetime(2026, 11, 9, 14, 20, tzinfo=timezone.utc)
POINTERS = "control/pointers/gold/source=commerce-events/domain=sales/"


def _envelope(event_id, event_type, occurred_at, **data):
    return {"id": event_id, "type": event_type, "version": 1, "occurred_at": occurred_at,
            "producer": "platform-payment", "data": data}


def _land(lake, received_at, envelopes, offset=0):
    from data_lake.stream_sink import StreamMessage, convert_avro_file, sink_messages

    messages = [
        StreamMessage(topic="commerce.events.v1", partition=0, offset=offset + i, value=envelope)
        for i, envelope in enumerate(envelopes)
    ]
    landed = sink_messages(messages, data_lake_uri=lake, source="aiven-kafka", domain="commerce",
                           dataset="events", received_at=received_at)
    convert_avro_file(data_lake_uri=lake, avro_key=landed["avro_key"])


def _pay(intent, amount, at, ws="ws-a", currency="thb"):
    return _envelope(f"payment.intent.succeeded:{intent}", "payment.intent.succeeded", at,
                     workspace_id=ws, intent_id=intent, amount=amount, currency=currency, provider="stripe")


def _dbt_available() -> bool:
    from data_lake.jobs_marts import resolve_dbt_bin

    return resolve_dbt_bin() is not None


class SalesMartsTests(unittest.TestCase):
    def setUp(self):
        try:
            import duckdb  # noqa: F401
            import fastavro  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("duckdb/pyarrow/fastavro not installed")
        if not _dbt_available():
            if os.environ.get("SOLO_EMPIRE_CI") == "1":
                self.fail("dbt not found; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
            self.skipTest("dbt not installed")

    def _run(self, lake, **kwargs):
        from data_lake.sales_marts import run_sales_marts

        kwargs.setdefault("now", NOW)
        return run_sales_marts(data_lake_uri=lake, **kwargs)

    def _rows(self, lake, result, mart, sql):
        return [tuple(r.values()) for r in query_parquet(str(Path(lake) / result["marts"][mart]["key"]), sql)]

    def test_marts_are_built_from_committed_commerce_bronze(self):
        with tempfile.TemporaryDirectory() as lake:
            _land(lake, "2026-11-09T14:05:00Z", [_pay("i1", 2000, "2026-11-09T14:08:00Z"),
                                                  _pay("i2", 50_000, "2026-11-09T02:00:00Z", ws="ws-b")])
            # At-least-once: i1 again in the next hourly batch.
            _land(lake, "2026-11-09T15:05:00Z", [_pay("i1", 2000, "2026-11-09T14:08:00Z")], offset=2)
            result = self._run(lake)
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["marts"]["fact_sales"]["rows"], 2)
            self.assertEqual(result["marts"]["sales_rejected_events"]["rows"], 0)
            daily = self._rows(
                lake, result, "sales_daily_revenue",
                "SELECT CAST(business_date AS VARCHAR), currency, orders, gross_minor, net_minor FROM lake_table",
            )
            self.assertEqual(daily, [("2026-11-09", "THB", 2, 52_000, 52_000)])
            self.assertTrue(all(m["key"].startswith("gold/domain=sales/") for m in result["marts"].values()))
            manifest = json.loads(ObjectStore(lake).get_bytes(result["manifest_key"]))
            self.assertEqual(manifest["source"], "commerce-events")
            self.assertEqual({m["dataset"] for m in manifest["marts"]},
                             {"fact_sales", "dim_customer", "sales_daily_revenue", "sales_rejected_events"})

    def test_rerun_writes_nothing_new_even_with_a_later_clock(self):
        with tempfile.TemporaryDirectory() as lake:
            _land(lake, "2026-11-09T14:05:00Z", [_pay("i1", 2000, "2026-11-09T14:08:00Z")])
            first = self._run(lake)
            self.assertRegex(
                first["pointer_key"],
                rf"^{re.escape(POINTERS)}published_at=20261109T142000Z-{first['gold_run_id']}-[0-9a-f]{{12}}\.json$",
            )
            again = self._run(lake, now=datetime(2026, 11, 9, 15, 20, tzinfo=timezone.utc))
            self.assertEqual(again["gold_run_id"], first["gold_run_id"])
            self.assertTrue(all(m["existed"] for m in again["marts"].values()))
            self.assertTrue(again["manifest_existed"] and again["pointer_existed"])
            self.assertEqual(again["pointer_key"], first["pointer_key"])
            # New Bronze → new Gold run and a new pointer.
            _land(lake, "2026-11-09T16:05:00Z", [_pay("i3", 900, "2026-11-09T16:00:00Z")], offset=1)
            newer = self._run(lake, now=datetime(2026, 11, 9, 16, 20, tzinfo=timezone.utc))
            self.assertNotEqual(newer["gold_run_id"], first["gold_run_id"])
            self.assertEqual(max(ObjectStore(lake).list_keys(POINTERS)), newer["pointer_key"])

    def test_empty_lake_publishes_empty_marts(self):
        with tempfile.TemporaryDirectory() as lake:
            result = self._run(lake)
            self.assertEqual(result["status"], "success")
            self.assertEqual({m["rows"] for m in result["marts"].values()}, {0})

    def test_rejected_events_are_reported_as_a_contract_break(self):
        with tempfile.TemporaryDirectory() as lake:
            _land(lake, "2026-11-09T14:05:00Z", [_pay("i1", 2000, "2026-11-09T14:08:00Z"),
                                                  _pay("bad", 0, "2026-11-09T14:09:00Z")])
            result = self._run(lake)
            self.assertEqual(result["status"], "rejects")
            self.assertEqual(result["marts"]["sales_rejected_events"]["rows"], 1)
            self.assertTrue(result["manifest_key"])  # still published

    def test_result_carries_no_intent_or_workspace_ids(self):
        with tempfile.TemporaryDirectory() as lake:
            _land(lake, "2026-11-09T14:05:00Z", [_pay("intent-secret-1", 2000, "2026-11-09T14:08:00Z",
                                                       ws="ws-secret-1")])
            result = self._run(lake)
            text = json.dumps(result)
            self.assertNotIn("intent-secret-1", text)
            self.assertNotIn("ws-secret-1", text)


    def test_batch_committed_commerce_events_are_included(self):
        from data_lake.ingest import ingest_payload

        with tempfile.TemporaryDirectory() as lake:
            # A batch writer (not the stream sink): its parts are committed by
            # control/manifests/source=<src>/batch_id=*.json, which the model also accepts.
            envelope = _pay("batch-1", 700, "2026-11-09T05:00:00Z")
            ingest_payload(json.dumps([envelope]).encode(), [{**envelope, "received_at": "2026-11-09T06:00:00Z"}],
                           source="backfill", domain="commerce", dataset="events", data_lake_uri=lake,
                           input_format="text", event_time_field="occurred_at", privacy_class="internal")
            result = self._run(lake)
            self.assertEqual(result["marts"]["fact_sales"]["rows"], 1)

if __name__ == "__main__":
    unittest.main()
