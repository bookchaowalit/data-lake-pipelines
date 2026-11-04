#!/usr/bin/env python3
"""Offline tests for the confluent-kafka adapter and the event-plane heartbeat (ADR-0008)."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import event_heartbeat, stream_sink  # noqa: E402
from data_lake.stream_sink import BrokerError, ConfluentConsumer, StreamMessage, kafka_config  # noqa: E402

HAS_RUNTIME = all(importlib.util.find_spec(name) is not None for name in ("confluent_kafka", "fastavro", "pyarrow"))


def _skip_unless_runtime(available: bool, reason: str):
    if not available and os.environ.get("SOLO_EMPIRE_CI") == "1":
        raise RuntimeError(f"{reason}; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
    return unittest.skipUnless(available, reason)


class FakeError:
    def __init__(self, code: int, name: str):
        self._code, self._name = code, name

    def code(self):
        return self._code

    def name(self):
        return self._name


class FakeMessage:
    def __init__(self, offset: int, value: bytes | None = None, error: FakeError | None = None):
        self._offset, self._value, self._error = offset, value, error

    def error(self):
        return self._error

    def topic(self):
        return "ops.events.v1"

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def timestamp(self):
        return (1, 1_700_000_000_000)


class FakeConfluent:
    """Stands in for confluent_kafka.Consumer: assignment appears after the first consume."""

    def __init__(self, config):
        self.config = config
        self.batches: list[list[FakeMessage]] = []
        self.consumes = 0
        self.committed = None
        self.commit_result = None
        self.never_assigned = False
        self.closed = False

    def subscribe(self, topics):
        self.topics = topics

    def consume(self, num_messages, timeout):
        del num_messages, timeout
        self.consumes += 1
        return self.batches.pop(0) if self.batches else []

    def assignment(self):
        return ["tp"] if self.consumes and not self.never_assigned else []

    def commit(self, offsets, asynchronous):
        self.committed = (offsets, asynchronous)
        return offsets if self.commit_result is None else self.commit_result

    def close(self):
        self.closed = True


class FakeDrainConsumer:
    def __init__(self, messages):
        self.messages = messages
        self.commits = []
        self.closed = False

    def poll(self, *, timeout_ms, max_records):
        del timeout_ms
        batch, self.messages = self.messages[:max_records], self.messages[max_records:]
        return batch

    def commit(self, offsets=None):
        self.commits.append(offsets)

    def close(self):
        self.closed = True


def _envelope(n: int) -> dict:
    return event_heartbeat.heartbeat_envelope(f"run-{n}") | {"id": f"ops.heartbeat.emitted:{n}"}


class KafkaConfigTests(unittest.TestCase):
    def test_plaintext_without_credentials(self):
        config = kafka_config("localhost:19092", "lake-sink", "earliest", {})
        self.assertEqual(config["bootstrap.servers"], "localhost:19092")
        self.assertNotIn("security.protocol", config)
        self.assertEqual(config["auto.offset.reset"], "earliest")

    def test_sasl_ssl_with_credentials(self):
        config = kafka_config(
            "broker:23922",
            "lake-sink",
            "none",
            {"STREAM_KAFKA_USERNAME": "lake-sink", "STREAM_KAFKA_PASSWORD": "p", "STREAM_KAFKA_CA_PEM": "PEM"},
        )
        self.assertEqual(config["security.protocol"], "SASL_SSL")
        self.assertEqual(config["sasl.mechanism"], "SCRAM-SHA-256")
        self.assertEqual(config["ssl.ca.pem"], "PEM")
        self.assertEqual(config["auto.offset.reset"], "error")


@_skip_unless_runtime(HAS_RUNTIME, "install requirements-data-lake.txt for confluent-kafka")
class ConfluentConsumerTests(unittest.TestCase):
    def _consumer(self, join_timeout: float = 5) -> tuple[ConfluentConsumer, FakeConfluent]:
        with patch("confluent_kafka.Consumer", FakeConfluent):
            consumer = ConfluentConsumer({"group.id": "g"}, "ops.events.v1", join_timeout=join_timeout)
        fake = consumer._consumer
        self.assertFalse(fake.config["enable.auto.commit"])
        self.assertEqual(fake.topics, ["ops.events.v1"])
        return consumer, fake

    def test_first_poll_waits_for_join_and_skips_partition_eof(self):
        from confluent_kafka import KafkaError

        consumer, fake = self._consumer()
        eof = FakeMessage(0, error=FakeError(KafkaError._PARTITION_EOF, "_PARTITION_EOF"))
        fake.batches = [[], [eof, FakeMessage(4, json.dumps(_envelope(4)).encode())]]
        polled = consumer.poll(timeout_ms=1, max_records=10)
        self.assertEqual([m["offset"] for m in polled], [4])
        self.assertEqual(polled[0]["timestamp_ms"], 1_700_000_000_000)
        self.assertGreaterEqual(fake.consumes, 2)

    def test_group_join_timeout_is_an_error_not_an_empty_poll(self):
        consumer, fake = self._consumer(join_timeout=0.3)
        fake.never_assigned = True
        with self.assertRaisesRegex(BrokerError, "join timed out"):
            consumer.poll(timeout_ms=1, max_records=10)

    def test_empty_poll_after_join_is_empty(self):
        consumer, _ = self._consumer()
        self.assertEqual(consumer.poll(timeout_ms=1, max_records=10), [])

    def test_per_partition_commit_error_is_raised(self):
        consumer, fake = self._consumer()

        class Failed:
            error = FakeError(25, "UNKNOWN_MEMBER_ID")

        fake.commit_result = [Failed()]
        with self.assertRaisesRegex(BrokerError, "UNKNOWN_MEMBER_ID"):
            consumer.commit({"ops.events.v1/0": 3})

    def test_broker_error_is_raised(self):
        consumer, fake = self._consumer()
        fake.batches = [[FakeMessage(0, error=FakeError(-1, "_AUTHENTICATION"))]]
        with self.assertRaisesRegex(BrokerError, "_AUTHENTICATION"):
            consumer.poll(timeout_ms=1, max_records=10)

    def test_commit_sends_next_offsets_synchronously(self):
        consumer, fake = self._consumer()
        consumer.commit({"ops.events.v1/1": 8})
        partitions, asynchronous = fake.committed
        self.assertFalse(asynchronous)
        self.assertEqual([(p.topic, p.partition, p.offset) for p in partitions], [("ops.events.v1", 1, 8)])
        consumer.close()
        self.assertTrue(fake.closed)


@_skip_unless_runtime(HAS_RUNTIME, "install requirements-data-lake.txt for Avro/Parquet runtime")
class EnvelopeSinkTests(unittest.TestCase):
    def test_envelope_occurred_at_becomes_event_time(self):
        envelope = _envelope(1)
        with tempfile.TemporaryDirectory() as temp_dir:
            result = stream_sink.sink_messages(
                [StreamMessage("ops.events.v1", 0, 1, envelope)],
                data_lake_uri=temp_dir,
                source="aiven-kafka",
                domain="ops",
                dataset="events",
            )
            from data_lake.avro import read_avro
            from data_lake.storage import ObjectStore

            rows = read_avro(io.BytesIO(ObjectStore(temp_dir).get_bytes(result["avro_key"])))
        self.assertEqual(rows[0]["event_time"], envelope["occurred_at"])
        self.assertEqual(rows[0]["source_record_id"], envelope["id"])

    def test_until_empty_drains_every_batch_then_exits(self):
        messages = [StreamMessage("ops.events.v1", 0, n, _envelope(n)) for n in range(5)]
        drain = FakeDrainConsumer(messages)
        with tempfile.TemporaryDirectory() as temp_dir, patch.object(stream_sink, "_kafka_consumer", return_value=drain):
            argv = [
                "stream_sink.py", "sink", "--topic", "ops.events.v1", "--group-id", "lake-sink",
                "--data-lake-uri", temp_dir, "--source", "aiven-kafka", "--domain", "ops",
                "--dataset", "events", "--max-records", "2", "--poll-ms", "1", "--until-empty",
                "--expect-id", "ops.heartbeat.emitted:3", "--json",
            ]
            out = io.StringIO()
            with patch.object(sys, "argv", argv), redirect_stdout(out):
                code = stream_sink.main()
        self.assertEqual(code, 0)
        result = json.loads(out.getvalue())
        self.assertEqual((result["batches"], result["total_records"]), (3, 5))
        self.assertTrue(result["expected_id_found"])
        self.assertEqual(len(drain.commits), 3)
        self.assertTrue(drain.closed)


    def test_expect_id_is_false_when_the_event_did_not_land(self):
        drain = FakeDrainConsumer([StreamMessage("ops.events.v1", 0, 0, _envelope(0))])
        with tempfile.TemporaryDirectory() as temp_dir:
            result = stream_sink.run_once(
                drain, data_lake_uri=temp_dir, source="aiven-kafka", domain="ops", dataset="events",
                poll_ms=1, expect_id="ops.heartbeat.emitted:missing",
            )
        self.assertFalse(result["expected_id_found"])


class HeartbeatTests(unittest.TestCase):
    def test_envelope_has_every_adr_0008_field(self):
        envelope = event_heartbeat.heartbeat_envelope("123")
        self.assertEqual(
            set(envelope),
            {"id", "type", "version", "occurred_at", "producer", "correlation_id", "causation_id", "data"},
        )
        self.assertEqual(envelope["type"], "ops.heartbeat.emitted")
        self.assertTrue(envelope["occurred_at"].endswith("Z"))

    def test_publish_waits_for_ack_and_raises_on_error_or_timeout(self):
        class Producer:
            def __init__(self, err=None, deliver=True):
                self.err, self.deliver, self.produced = err, deliver, []

            def produce(self, topic, key, value, on_delivery):
                self.produced.append((topic, key, json.loads(value)))
                self.callback = on_delivery

            def poll(self, timeout):
                del timeout
                if self.deliver:
                    self.callback(self.err, None)

        ok = Producer()
        event_heartbeat.publish(ok, _envelope(1))
        self.assertEqual(ok.produced[0][0], "ops.events.v1")
        with self.assertRaisesRegex(RuntimeError, "TOPIC_AUTHORIZATION_FAILED"):
            event_heartbeat.publish(Producer(err=FakeError(29, "TOPIC_AUTHORIZATION_FAILED")), _envelope(2))
        with self.assertRaises(TimeoutError):
            event_heartbeat.publish(Producer(deliver=False), _envelope(3), timeout=0.3)

    def test_producer_config_uses_sasl_when_username_set(self):
        config = event_heartbeat.producer_config(
            {"STREAM_KAFKA_BOOTSTRAP": "b:1", "STREAM_KAFKA_USERNAME": "ops-heartbeat", "STREAM_KAFKA_PASSWORD": "p"}
        )
        self.assertEqual(config["security.protocol"], "SASL_SSL")
        self.assertTrue(config["enable.idempotence"])


if __name__ == "__main__":
    unittest.main()
