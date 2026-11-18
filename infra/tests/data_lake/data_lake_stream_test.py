#!/usr/bin/env python3
"""Offline contract tests for the Redpanda/Kafka-compatible lake lane."""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.avro import read_avro  # noqa: E402
from data_lake.storage import ObjectStore, StorageError  # noqa: E402
import data_lake.stream_sink as stream_sink  # noqa: E402
from data_lake.stream_sink import (  # noqa: E402
    StreamMessage,
    convert_avro_file,
    convert_pending,
    run_once,
)


HAS_STREAM_RUNTIME = all(
    importlib.util.find_spec(name) is not None
    for name in ("fastavro", "pyarrow")
)


class FakeConsumer:
    def __init__(self, messages: list[StreamMessage]):
        self.messages = messages
        self.commits: list[object] = []

    def poll(self, *, timeout_ms: int, max_records: int):
        del timeout_ms
        messages, self.messages = self.messages[:max_records], self.messages[max_records:]
        return messages

    def commit(self, offsets=None):
        self.commits.append(offsets)


def _skip_unless_runtime(available: bool, reason: str):
    if not available and os.environ.get("SOLO_EMPIRE_CI") == "1":
        raise RuntimeError(f"{reason}; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
    return unittest.skipUnless(available, reason)


@_skip_unless_runtime(HAS_STREAM_RUNTIME, "install requirements-data-lake.txt for Avro/Parquet runtime")
class StreamLakeContractTests(unittest.TestCase):
    def test_sink_is_durable_before_commit_and_retry_is_idempotent(self):
        messages = [
            StreamMessage("events", 0, 10, {"id": "evt-10", "payload": {"value": 1}}),
            StreamMessage("events", 0, 11, {"id": "evt-11", "payload": {"value": 2}}),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            first_consumer = FakeConsumer(messages.copy())
            first = run_once(
                first_consumer,
                data_lake_uri=temp_dir,
                source="sensor",
                domain="operations",
                dataset="events",
                poll_ms=1,
                max_records=10,
            )
            self.assertEqual(first["status"], "success")
            self.assertTrue(first["committed"])
            self.assertEqual(len(first_consumer.commits), 1)

            store = ObjectStore(temp_dir)
            avro_rows = read_avro(__import__("io").BytesIO(store.get_bytes(first["avro_key"])))
            self.assertEqual(len(avro_rows), 2)
            self.assertEqual(avro_rows[0]["dataset"], "events")

            retry_consumer = FakeConsumer(messages.copy())
            retry = run_once(
                retry_consumer,
                data_lake_uri=temp_dir,
                source="sensor",
                domain="operations",
                dataset="events",
                poll_ms=1,
                max_records=10,
            )
            self.assertEqual(retry["batch_id"], first["batch_id"])
            self.assertTrue(retry["avro_existed"])
            self.assertTrue(retry["manifest_existed"])
            self.assertTrue(retry["checkpoint_existed"])
            self.assertEqual(len(retry_consumer.commits), 1)

            converted = convert_avro_file(data_lake_uri=temp_dir, avro_key=first["avro_key"])
            self.assertEqual(converted["status"], "success")
            self.assertEqual(converted["record_count"], 2)
            self.assertTrue(Path(temp_dir, converted["bronze_key"]).exists())
            converted_retry = convert_avro_file(data_lake_uri=temp_dir, avro_key=first["avro_key"])
            self.assertEqual(converted_retry["status"], "already_converted")
            pending = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual(pending["status"], "healthy")
            self.assertEqual(pending["already_converted"], 1)

    def test_stream_sink_resumes_after_avro_write_failure(self):
        messages = [StreamMessage("events", 0, 20, {"id": "evt-20", "payload": {"value": 1}})]
        with tempfile.TemporaryDirectory() as temp_dir:
            original_put = ObjectStore.put_bytes

            def fail_avro(store, key, data, **kwargs):
                if key.endswith(".avro"):
                    raise StorageError("synthetic Avro outage")
                return original_put(store, key, data, **kwargs)

            with patch.object(ObjectStore, "put_bytes", fail_avro):
                with self.assertRaises(StorageError):
                    run_once(
                        FakeConsumer(messages.copy()),
                        data_lake_uri=temp_dir,
                        source="sensor",
                        domain="operations",
                        dataset="events",
                        poll_ms=1,
                        max_records=10,
                    )
            retry = run_once(
                FakeConsumer(messages.copy()),
                data_lake_uri=temp_dir,
                source="sensor",
                domain="operations",
                dataset="events",
                poll_ms=1,
                max_records=10,
            )
            self.assertEqual(retry["status"], "success")
            self.assertTrue(retry["checkpoint_existed"])


    def test_convert_pending_skips_indexed_avro_without_reading_it(self):
        messages = [StreamMessage("events", 0, 30, {"id": "evt-30", "payload": {"value": 1}})]
        with tempfile.TemporaryDirectory() as temp_dir:
            sunk = run_once(
                FakeConsumer(messages.copy()),
                data_lake_uri=temp_dir,
                source="sensor",
                domain="operations",
                dataset="events",
                poll_ms=1,
                max_records=10,
            )
            first = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual(first["converted"], 1)
            self.assertEqual(first["indexed"], 0)
            index_keys = ObjectStore(temp_dir).list_keys("control/state/stream-converted")
            self.assertEqual(len(index_keys), 1)

            original_get = ObjectStore.get_bytes
            reads: list[str] = []

            def counting_get(store, key):
                reads.append(key)
                return original_get(store, key)

            with patch.object(ObjectStore, "get_bytes", counting_get):
                second = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual(second["status"], "healthy")
            self.assertEqual(second["scanned"], 1)
            self.assertEqual(second["already_converted"], 1)
            self.assertEqual(second["indexed"], 1)
            self.assertNotIn(sunk["avro_key"], reads)
            self.assertEqual(reads, [])

    def test_convert_pending_backfills_index_for_previously_converted_avro(self):
        messages = [StreamMessage("events", 0, 40, {"id": "evt-40", "payload": {"value": 1}})]
        with tempfile.TemporaryDirectory() as temp_dir:
            sunk = run_once(
                FakeConsumer(messages.copy()),
                data_lake_uri=temp_dir,
                source="sensor",
                domain="operations",
                dataset="events",
                poll_ms=1,
                max_records=10,
            )
            # Converted before the index existed (single-file path writes no index entry).
            convert_avro_file(data_lake_uri=temp_dir, avro_key=sunk["avro_key"])
            store = ObjectStore(temp_dir)
            self.assertEqual(store.list_keys("control/state/stream-converted"), [])
            backfill = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual(backfill["already_converted"], 1)
            self.assertEqual(backfill["indexed"], 0)
            self.assertEqual(len(store.list_keys("control/state/stream-converted")), 1)
            self.assertEqual(convert_pending(data_lake_uri=temp_dir)["indexed"], 1)

    def test_convert_pending_degrades_when_indexed_bronze_part_is_gone(self):
        messages = [StreamMessage("events", 0, 50, {"id": "evt-50", "payload": {"value": 1}})]
        with tempfile.TemporaryDirectory() as temp_dir:
            run_once(
                FakeConsumer(messages.copy()),
                data_lake_uri=temp_dir,
                source="sensor",
                domain="operations",
                dataset="events",
                poll_ms=1,
                max_records=10,
            )
            first = convert_pending(data_lake_uri=temp_dir)
            Path(temp_dir, first["results"][0]["bronze_key"]).unlink()
            after = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual(after["status"], "degraded")
            self.assertEqual(after["indexed"], 0)
            self.assertEqual(after["errors"][0]["error"], "IndexedBronzeMissing")

    def _converted_once(self, temp_dir, offset):
        messages = [StreamMessage("events", 0, offset, {"id": f"evt-{offset}", "payload": {"value": 1}})]
        run_once(FakeConsumer(messages.copy()), data_lake_uri=temp_dir, source="sensor", domain="operations",
                 dataset="events", poll_ms=1, max_records=10)
        return convert_pending(data_lake_uri=temp_dir)["results"][0]

    def test_convert_pending_degrades_when_indexed_bronze_part_moved(self):
        """Review P2: the same part file under another partition must not count as present."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self._converted_once(temp_dir, 60)
            source = Path(temp_dir, result["bronze_key"])
            moved = Path(str(source).replace("ingest_date=", "ingest_date=1999-01-01-was-"))
            moved.parent.mkdir(parents=True)
            source.rename(moved)
            after = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual(after["status"], "degraded")
            self.assertEqual(after["errors"][0]["error"], "IndexedBronzeMissing")

    def test_convert_pending_rewrites_a_missing_conversion_manifest(self):
        """Review P2: an indexed Avro whose manifest is gone goes back through the conversion path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self._converted_once(temp_dir, 70)
            Path(temp_dir, result["manifest_key"]).unlink()
            after = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual(after["status"], "healthy", after["errors"])
            self.assertEqual(after["indexed"], 0)
            self.assertTrue(Path(temp_dir, result["manifest_key"]).exists())
            self.assertEqual(convert_pending(data_lake_uri=temp_dir)["indexed"], 1)

    def test_convert_pending_upgrades_a_legacy_index_entry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self._converted_once(temp_dir, 80)
            store = ObjectStore(temp_dir)
            (current,) = store.list_keys("control/state/stream-converted")
            legacy = current[: current.index("/bronze=")] + ".json"
            store.put_bytes(legacy, store.get_bytes(current), content_type="application/json")
            Path(temp_dir, current).unlink()
            after = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual((after["status"], after["indexed"]), ("healthy", 1))
            self.assertIn(current, store.list_keys("control/state/stream-converted"))
            self.assertEqual(result["status"], "success")

    def test_legacy_upgrades_are_bounded_per_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self._converted_once(temp_dir, 90)
            store = ObjectStore(temp_dir)
            (current,) = store.list_keys("control/state/stream-converted")
            legacy = current[: current.index("/bronze=")] + ".json"
            store.put_bytes(legacy, store.get_bytes(current), content_type="application/json")
            Path(temp_dir, current).unlink()
            with patch.object(stream_sink, "LEGACY_UPGRADES_PER_RUN", 0):
                held = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual((held["status"], held["indexed"], held["legacy_pending"]), ("healthy", 1, 1))
            self.assertNotIn(current, store.list_keys("control/state/stream-converted"))
            upgraded = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual((upgraded["indexed"], upgraded["legacy_pending"]), (1, 0))
            self.assertIn(current, store.list_keys("control/state/stream-converted"))

    def test_index_keys_have_a_fixed_length_bronze_digest(self):
        """Review P2: the full Bronze key in the index key could pass the 1,024-byte S3/R2 limit."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self._converted_once(temp_dir, 100)
            (current,) = ObjectStore(temp_dir).list_keys("control/state/stream-converted")
            self.assertNotIn(result["bronze_key"], current)
            self.assertRegex(current, r"/bronze=[0-9a-f]{32}\.json$")

    def test_full_bronze_key_entries_from_2026_11_18_still_count(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self._converted_once(temp_dir, 110)
            store = ObjectStore(temp_dir)
            (current,) = store.list_keys("control/state/stream-converted")
            long_key = current[: current.index("/bronze=")] + "/" + result["bronze_key"] + ".json"
            store.put_bytes(long_key, store.get_bytes(current), content_type="application/json")
            Path(temp_dir, current).unlink()
            after = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual((after["status"], after["indexed"]), ("healthy", 1))

    def test_a_failed_legacy_upgrade_is_one_error_not_a_stopped_run(self):
        """Review P2: an unreadable legacy entry must not stop the other Avro files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            self._converted_once(temp_dir, 120)
            store = ObjectStore(temp_dir)
            (current,) = store.list_keys("control/state/stream-converted")
            legacy = current[: current.index("/bronze=")] + ".json"
            store.put_bytes(legacy, store.get_bytes(current), content_type="application/json")
            Path(temp_dir, current).unlink()
            messages = [StreamMessage("events", 0, 121, {"id": "evt-121", "payload": {"value": 1}})]
            run_once(FakeConsumer(messages), data_lake_uri=temp_dir, source="sensor", domain="operations",
                     dataset="events", poll_ms=1, max_records=10)
            original_get = ObjectStore.get_bytes

            def failing_get(obj, key):
                if key == legacy:
                    raise StorageError("unreadable")
                return original_get(obj, key)

            with patch.object(ObjectStore, "get_bytes", failing_get):
                after = convert_pending(data_lake_uri=temp_dir)
            self.assertEqual(after["status"], "degraded")
            self.assertEqual(after["converted"], 1)
            self.assertEqual([e["error"] for e in after["errors"]], ["StorageError"])

if __name__ == "__main__":
    unittest.main()
