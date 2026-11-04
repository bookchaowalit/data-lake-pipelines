#!/usr/bin/env python3
"""Sink Kafka-compatible events to Avro, then convert them to Bronze Parquet.

The broker client is confluent-kafka (librdkafka): kafka-python-ng cannot
authenticate to Kafka 4.x (the Aiven event plane, ADR-0008). Local Redpanda
uses PLAINTEXT; ``STREAM_KAFKA_USERNAME`` switches the client to SASL_SSL.

The sink is deliberately small and at-least-once:

1. poll a bounded micro-batch;
2. write Avro, manifest, and checkpoint objects immutably;
3. commit Kafka offsets only after all three writes succeed.

Retries are safe because the batch ID is derived from topic/partition/offset
positions and the object store rejects conflicting writes.  The broker is a
replay buffer; Object Storage is the durable source of truth.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from data_lake.avro import (  # type: ignore
        AVRO_SCHEMA_VERSION,
        AvroUnavailable,
        read_avro,
        write_avro,
    )
    from data_lake.iceberg import IcebergUnavailable  # type: ignore
    from data_lake.ingest import canonical_json, slug, utc_now  # type: ignore
    from data_lake.parquet import BRONZE_COLUMNS, ParquetUnavailable, write_bronze_parquet  # type: ignore
    from data_lake.storage import ObjectNotFound, ObjectStore, StorageError  # type: ignore
else:
    from .avro import AVRO_SCHEMA_VERSION, AvroUnavailable, read_avro, write_avro
    from .iceberg import IcebergUnavailable
    from .ingest import canonical_json, slug, utc_now
    from .parquet import BRONZE_COLUMNS, ParquetUnavailable, write_bronze_parquet
    from .storage import ObjectNotFound, ObjectStore, StorageError


UTILS_DIR = Path(__file__).resolve().parents[1] / "utils"
if str(UTILS_DIR) not in sys.path:
    sys.path.insert(0, str(UTILS_DIR))
from _env import PROJECT_ROOT  # noqa: E402


DEFAULT_LAKE_URI = os.environ.get(
    "SOLO_EMPIRE_DATA_LAKE_URI",
    os.environ.get("DATA_LAKE_URI", str(PROJECT_ROOT / "data" / "lake")),
)


@dataclass(frozen=True)
class StreamMessage:
    """Small broker-neutral message shape used by the sink and offline tests."""

    topic: str
    partition: int
    offset: int
    value: Any
    timestamp_ms: int | None = None


def _message_field(message: Any, field: str, default: Any = None) -> Any:
    if isinstance(message, Mapping):
        return message.get(field, default)
    return getattr(message, field, default)


def _as_stream_message(message: Any) -> StreamMessage:
    topic = _message_field(message, "topic")
    partition = _message_field(message, "partition")
    offset = _message_field(message, "offset")
    if topic is None or partition is None or offset is None:
        raise ValueError("Kafka message must contain topic, partition, and offset")
    value = _message_field(message, "value")
    if isinstance(value, bytes):
        try:
            value = json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Streaming event values must be UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("Streaming event value must be a JSON object")
    timestamp_ms = _message_field(message, "timestamp", _message_field(message, "timestamp_ms"))
    return StreamMessage(
        topic=str(topic),
        partition=int(partition),
        offset=int(offset),
        value=value,
        timestamp_ms=None if timestamp_ms is None else int(timestamp_ms),
    )


def _flatten_poll(polled: Any, max_records: int) -> list[StreamMessage]:
    """Normalize KafkaConsumer.poll output and remove duplicate positions."""
    if isinstance(polled, Mapping):
        messages = [item for batch in polled.values() for item in batch]
    elif isinstance(polled, (list, tuple)):
        messages = list(polled)
    elif polled is None:
        messages = []
    else:
        messages = [polled]

    normalized: list[StreamMessage] = []
    seen: set[tuple[str, int, int]] = set()
    for message in messages:
        item = _as_stream_message(message)
        position = (item.topic, item.partition, item.offset)
        if position in seen:
            continue
        seen.add(position)
        normalized.append(item)
        if len(normalized) >= max_records:
            break
    return normalized


def _batch_id(messages: list[StreamMessage]) -> str:
    positions = "\n".join(
        f"{message.topic}:{message.partition}:{message.offset}"
        for message in sorted(messages, key=lambda item: (item.topic, item.partition, item.offset))
    )
    return hashlib.sha256(positions.encode("utf-8")).hexdigest()[:24]


def _payload_from_event(value: dict[str, Any]) -> Any:
    if "payload" in value:
        return value["payload"]
    if "payload_json" in value:
        return None
    return value


def _event_row(
    value: dict[str, Any],
    *,
    source: str,
    domain: str,
    dataset: str,
    schema_version: str,
    received_at: str,
    run_id: str,
    metadata: dict[str, Any],
    privacy_class: str,
    retention_class: str,
) -> dict[str, str | None]:
    """Normalize either a full Bronze envelope or a compact producer event."""
    payload = _payload_from_event(value)
    payload_json = value.get("payload_json")
    if payload_json is None and payload is not None:
        payload_json = canonical_json(payload)

    event_source = slug(str(value.get("source") or source))
    event_domain = slug(str(value.get("domain") or domain))
    event_dataset = slug(str(value.get("dataset") or dataset))
    event_schema_version = slug(str(value.get("schema_version") or schema_version))
    source_record_id = value.get("source_record_id", value.get("id"))
    source_record_id_text = None if source_record_id is None else str(source_record_id)
    identity = "\0".join(
        [
            event_source,
            source_record_id_text or str(payload_json or ""),
        ]
    )
    event_id = str(value.get("event_id") or hashlib.sha256(identity.encode("utf-8")).hexdigest())
    row_metadata = value.get("metadata", metadata)
    if isinstance(row_metadata, str):
        metadata_json = row_metadata
    else:
        metadata_json = canonical_json(row_metadata if isinstance(row_metadata, dict) else {})

    row: dict[str, str | None] = {
        "event_id": event_id,
        "source": event_source,
        "source_record_id": source_record_id_text,
        "domain": event_domain,
        "dataset": event_dataset,
        "schema_version": event_schema_version,
        "received_at": str(value.get("received_at") or received_at),
        # ADR-0008 envelopes carry occurred_at; Bronze envelopes carry event_time.
        "event_time": (
            None
            if value.get("event_time", value.get("occurred_at")) is None
            else str(value.get("event_time", value.get("occurred_at")))
        ),
        "content_type": str(value.get("content_type") or "application/json"),
        "raw_object_key": None if value.get("raw_object_key") is None else str(value.get("raw_object_key")),
        "raw_sha256": None if value.get("raw_sha256") is None else str(value.get("raw_sha256")),
        "payload_json": None if payload_json is None else str(payload_json),
        "metadata_json": metadata_json,
        "ingest_run_id": str(value.get("ingest_run_id") or run_id),
        "privacy_class": str(value.get("privacy_class") or privacy_class),
        "retention_class": str(value.get("retention_class") or retention_class),
    }
    return row


def _batch_positions(messages: list[StreamMessage]) -> list[dict[str, Any]]:
    return [
        {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        }
        for message in sorted(messages, key=lambda item: (item.topic, item.partition, item.offset))
    ]


def _next_offsets(messages: list[StreamMessage]) -> dict[str, int]:
    offsets: dict[str, int] = {}
    for message in messages:
        key = f"{message.topic}/{message.partition}"
        offsets[key] = max(offsets.get(key, 0), message.offset + 1)
    return dict(sorted(offsets.items()))


def sink_messages(
    messages: Iterable[Any],
    *,
    data_lake_uri: str,
    source: str,
    domain: str,
    dataset: str,
    schema_version: str = AVRO_SCHEMA_VERSION,
    metadata: dict[str, Any] | None = None,
    privacy_class: str = "private",
    retention_class: str = "operational",
    received_at: str | None = None,
) -> dict[str, Any]:
    """Persist one broker micro-batch; leave Kafka commit to ``run_once``."""
    normalized_messages = [_as_stream_message(message) for message in messages]
    if not normalized_messages:
        return {"status": "empty", "record_count": 0}
    if len({message.topic for message in normalized_messages}) != 1:
        raise ValueError("A stream micro-batch must contain one Kafka topic")

    source_name = slug(source)
    domain_name = slug(domain)
    dataset_name = slug(dataset)
    schema_name = slug(schema_version)
    batch_id = _batch_id(normalized_messages)
    manifest_key = (
        f"control/manifests/stream/source={source_name}/topic={slug(normalized_messages[0].topic)}/"
        f"batch_id={batch_id}.json"
    )
    store = ObjectStore(data_lake_uri)
    checkpoint_key = f"control/checkpoints/stream/source={source_name}/batch_id={batch_id}.json"

    existing_manifest: dict[str, Any] | None = None
    try:
        existing_manifest = json.loads(store.get_bytes(manifest_key).decode("utf-8"))
    except ObjectNotFound:
        pass
    existing_checkpoint: dict[str, Any] | None = None
    if existing_manifest is None:
        try:
            existing_checkpoint = json.loads(store.get_bytes(checkpoint_key).decode("utf-8"))
        except ObjectNotFound:
            pass
        except (StorageError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StorageError(f"Stream batch {batch_id} has an invalid checkpoint") from exc
        if existing_checkpoint is not None:
            if not isinstance(existing_checkpoint, dict):
                raise StorageError(f"Stream batch {batch_id} has an invalid checkpoint shape")
            for field, expected in {
                "stream": source_name,
                "batch_id": batch_id,
                "domain": domain_name,
                "dataset": dataset_name,
                "schema_version": schema_name,
                "privacy_class": privacy_class,
                "retention_class": retention_class,
                "metadata": metadata or {},
            }.items():
                if existing_checkpoint.get(field) != expected:
                    raise StorageError(f"Stream batch {batch_id} checkpoint does not match {field}")
            for field in ("updated_at", "run_id", "avro_key"):
                if field == "run_id":
                    continue
                if not isinstance(existing_checkpoint.get(field), str) or not existing_checkpoint[field]:
                    raise StorageError(f"Stream batch {batch_id} checkpoint is missing {field}")
    if existing_manifest is not None:
        for field, expected in {
            "source": source_name,
            "domain": domain_name,
            "dataset": dataset_name,
            "schema_version": schema_name,
            "privacy_class": privacy_class,
            "retention_class": retention_class,
            "metadata": metadata or {},
        }.items():
            if existing_manifest.get(field) != expected:
                raise StorageError(f"Stream batch {batch_id} already exists with a different {field}")
        received_at = str(existing_manifest["received_at"])
        run_id = str(existing_manifest["run_id"])
    elif existing_checkpoint is not None:
        received_at = str(existing_checkpoint["updated_at"])
        run_id = str(existing_checkpoint.get("run_id") or f"{received_at[:10]}-{batch_id}")
    else:
        received_at = received_at or utc_now()
        run_id = f"{received_at[:10]}-{batch_id}"

    rows = [
        _event_row(
            message.value,
            source=source_name,
            domain=domain_name,
            dataset=dataset_name,
            schema_version=schema_name,
            received_at=received_at,
            run_id=run_id,
            metadata=metadata or {},
            privacy_class=privacy_class,
            retention_class=retention_class,
        )
        for message in normalized_messages
    ]
    identity_fields = {
        "source": source_name,
        "domain": domain_name,
        "dataset": dataset_name,
        "schema_version": schema_name,
    }
    for field, expected in identity_fields.items():
        if any(row[field] != expected for row in rows):
            raise ValueError(f"Streaming event {field} does not match the sink contract")

    received_date = str(received_at)[:10]
    avro_key = (
        f"landing/stream/source={source_name}/domain={domain_name}/dataset={dataset_name}/"
        f"schema_version={schema_name}/ingest_date={received_date}/part-{batch_id}.avro"
    )
    if existing_checkpoint is not None:
        avro_key = str(existing_checkpoint["avro_key"])

    import io

    avro_buffer = io.BytesIO()
    write_avro(rows, avro_buffer)
    avro_bytes = avro_buffer.getvalue()
    checkpoint_result = None
    if existing_checkpoint is None:
        checkpoint = {
            "checkpoint_version": "1",
            "stream": source_name,
            "batch_id": batch_id,
            "domain": domain_name,
            "dataset": dataset_name,
            "schema_version": schema_name,
            "privacy_class": privacy_class,
            "retention_class": retention_class,
            "metadata": metadata or {},
            "updated_at": received_at,
            "run_id": run_id,
            "next_offsets": _next_offsets(normalized_messages),
            "manifest_key": manifest_key,
            "avro_key": avro_key,
        }
        # Persist the stable batch identity before Avro. A crash before the
        # final manifest can then resume with identical Avro bytes and keys.
        checkpoint_result = store.put_bytes(
            checkpoint_key,
            (canonical_json(checkpoint) + "\n").encode("utf-8"),
            content_type="application/json",
        )
    avro_result = store.put_bytes(
        avro_key,
        avro_bytes,
        content_type="avro/binary",
    )

    manifest = {
        "manifest_version": "1",
        "kind": "stream-avro-batch",
        "batch_id": batch_id,
        "run_id": run_id,
        "source": source_name,
        "domain": domain_name,
        "dataset": dataset_name,
        "schema_version": schema_name,
        "received_at": received_at,
        "record_count": len(rows),
        "avro": {"key": avro_key, "sha256": avro_result.sha256, "bytes": len(avro_bytes)},
        "positions": _batch_positions(normalized_messages),
        "next_offsets": _next_offsets(normalized_messages),
        "checkpoint_key": checkpoint_key,
        "privacy_class": privacy_class,
        "retention_class": retention_class,
        "metadata": metadata or {},
    }
    manifest_result = store.put_bytes(
        manifest_key,
        (canonical_json(manifest) + "\n").encode("utf-8"),
        content_type="application/json",
    )
    checkpoint_existed = (
        checkpoint_result.existed if checkpoint_result is not None else True
    )
    return {
        "status": "success",
        "data_lake": store.describe(),
        "batch_id": batch_id,
        "run_id": run_id,
        "record_count": len(rows),
        "avro_key": avro_result.key,
        "manifest_key": manifest_result.key,
        "checkpoint_key": checkpoint_key,
        "avro_existed": avro_result.existed,
        "manifest_existed": manifest_result.existed,
        "checkpoint_existed": checkpoint_existed,
        "next_offsets": _next_offsets(normalized_messages),
        "commit_positions": _batch_positions(normalized_messages),
    }


class BrokerError(RuntimeError):
    """The broker returned an error for a poll, commit, or join."""


def _commit_offsets(consumer: Any, messages: list[StreamMessage]) -> None:
    """Commit the next offset per partition after durable lake writes."""
    consumer.commit(offsets=_next_offsets(messages))


def _consumer_lag(consumer: Any) -> int | None:
    """Return the assigned Kafka lag for the local heartbeat, when available."""
    lag = getattr(consumer, "lag", None)
    return lag() if callable(lag) else None


class ConfluentConsumer:
    """confluent-kafka consumer behind the small poll/commit/close shape the sink uses."""

    def __init__(self, config: dict[str, str], topic: str, *, join_timeout: float = 30.0):
        from confluent_kafka import Consumer  # type: ignore

        self._consumer = Consumer({**config, "enable.auto.commit": False})
        self._consumer.subscribe([topic])
        self._join_timeout = join_timeout
        self._joined = False

    def poll(self, *, timeout_ms: int, max_records: int) -> list[dict[str, Any]]:
        # The first poll also waits for the group join, which takes seconds; an
        # empty answer before the join would read as "no events".
        deadline = time.monotonic() + timeout_ms / 1000
        if not self._joined:
            deadline = max(deadline, time.monotonic() + self._join_timeout)
        polled: list[dict[str, Any]] = []
        while not polled:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            for message in self._consumer.consume(num_messages=max_records, timeout=min(remaining, 1.0)):
                error = message.error()
                if error is not None:
                    if error.code() == self._partition_eof():
                        continue
                    raise BrokerError(error.name())
                _, timestamp_ms = message.timestamp()
                polled.append(
                    {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "value": message.value(),
                        "timestamp_ms": timestamp_ms if timestamp_ms and timestamp_ms > 0 else None,
                    }
                )
            if not self._joined and self._consumer.assignment():
                self._joined = True
                deadline = min(deadline, time.monotonic() + timeout_ms / 1000)
        if not polled and not self._joined:
            # No assignment means nothing was read at all; that is a failure, not an empty topic.
            raise BrokerError("consumer group join timed out")
        return polled

    @staticmethod
    def _partition_eof() -> int:
        from confluent_kafka import KafkaError  # type: ignore

        return KafkaError._PARTITION_EOF

    def commit(self, offsets: dict[str, int]) -> None:
        from confluent_kafka import KafkaException, TopicPartition  # type: ignore

        partitions = []
        for key, next_offset in offsets.items():
            topic, partition_text = key.rsplit("/", 1)
            partitions.append(TopicPartition(topic, int(partition_text), next_offset))
        try:
            committed = self._consumer.commit(offsets=partitions, asynchronous=False)
        except KafkaException as exc:
            raise BrokerError(exc.args[0].name() if exc.args else "commit failed") from exc
        # A synchronous commit reports per-partition failures on the returned partitions.
        for partition in committed or []:
            if partition.error is not None:
                raise BrokerError(f"commit failed: {partition.error.name()}")

    def lag(self) -> int | None:
        try:
            total = 0
            for position in self._consumer.position(self._consumer.assignment()):
                _, high = self._consumer.get_watermark_offsets(position, timeout=5.0)
                if position.offset >= 0:
                    total += max(high - position.offset, 0)
            return total
        except Exception:  # noqa: BLE001 - lag is best-effort heartbeat detail
            return None

    def close(self) -> None:
        self._consumer.close()


def kafka_config(bootstrap_servers: str, group_id: str, auto_offset_reset: str, environ: Mapping[str, str]) -> dict[str, str]:
    """librdkafka settings: PLAINTEXT locally, SASL_SSL when STREAM_KAFKA_USERNAME is set."""
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "error" if auto_offset_reset == "none" else auto_offset_reset,
    }
    if environ.get("STREAM_KAFKA_USERNAME"):
        config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": environ.get("STREAM_KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
                "sasl.username": environ["STREAM_KAFKA_USERNAME"],
                "sasl.password": environ["STREAM_KAFKA_PASSWORD"],
            }
        )
        if environ.get("STREAM_KAFKA_CA_PEM"):
            config["ssl.ca.pem"] = environ["STREAM_KAFKA_CA_PEM"]
    return config


def _collect_messages(consumer: Any, *, poll_ms: int, max_records: int, batch_seconds: float) -> list[StreamMessage]:
    deadline = time.monotonic() + max(batch_seconds, 0.0)
    collected: list[StreamMessage] = []
    seen: set[tuple[str, int, int]] = set()
    while len(collected) < max_records:
        polled = consumer.poll(timeout_ms=poll_ms, max_records=max_records - len(collected))
        batch = _flatten_poll(polled, max_records - len(collected))
        for message in batch:
            position = (message.topic, message.partition, message.offset)
            if position not in seen:
                seen.add(position)
                collected.append(message)
        if collected and (batch_seconds <= 0 or time.monotonic() >= deadline):
            break
        if not collected and batch_seconds <= 0:
            break
        if time.monotonic() >= deadline:
            break
    return collected


def run_once(
    consumer: Any,
    *,
    data_lake_uri: str,
    source: str,
    domain: str,
    dataset: str,
    schema_version: str = AVRO_SCHEMA_VERSION,
    metadata: dict[str, Any] | None = None,
    privacy_class: str = "private",
    retention_class: str = "operational",
    poll_ms: int = 5_000,
    max_records: int = 1_000,
    batch_seconds: float = 0.0,
    expect_id: str | None = None,
) -> dict[str, Any]:
    """Poll, durably land, and then commit one bounded micro-batch."""
    messages = _collect_messages(
        consumer,
        poll_ms=poll_ms,
        max_records=max_records,
        batch_seconds=batch_seconds,
    )
    if not messages:
        return {"status": "empty", "record_count": 0}
    result = sink_messages(
        messages,
        data_lake_uri=data_lake_uri,
        source=source,
        domain=domain,
        dataset=dataset,
        schema_version=schema_version,
        metadata=metadata,
        privacy_class=privacy_class,
        retention_class=retention_class,
    )
    _commit_offsets(consumer, messages)
    result["committed"] = True
    if expect_id is not None:
        result["expected_id_found"] = any(message.value.get("id") == expect_id for message in messages)
    return result


def convert_avro_file(
    *,
    data_lake_uri: str,
    avro_key: str,
    iceberg_catalog_uri: str | None = None,
    iceberg_warehouse_uri: str | None = None,
) -> dict[str, Any]:
    """Convert one immutable Avro object to Parquet and optionally register it."""
    store = ObjectStore(data_lake_uri)
    avro_bytes = store.get_bytes(avro_key)
    import io

    rows = read_avro(io.BytesIO(avro_bytes))
    if not rows:
        raise ValueError(f"Avro object contains no records: {avro_key}")
    for row in rows:
        missing = [column for column in BRONZE_COLUMNS if column not in row]
        if missing:
            raise ValueError(f"Avro object is missing Bronze columns: {', '.join(missing)}")

    source = slug(str(rows[0]["source"]))
    domain = slug(str(rows[0]["domain"]))
    dataset = slug(str(rows[0]["dataset"]))
    schema_version = slug(str(rows[0]["schema_version"]))
    received_date = str(rows[0]["received_at"])[:10]
    conversion_id = hashlib.sha256(avro_bytes).hexdigest()[:24]
    bronze_key = (
        f"bronze/domain={domain}/dataset={dataset}/schema_version={schema_version}/"
        f"ingest_date={received_date}/source={source}/part-{conversion_id}.parquet"
    )
    manifest_key = f"control/manifests/stream-parquet/source={source}/conversion_id={conversion_id}.json"

    try:
        existing = json.loads(store.get_bytes(manifest_key).decode("utf-8"))
    except ObjectNotFound:
        existing = None
    if existing is not None:
        existing_bronze = existing.get("bronze")
        if not isinstance(existing_bronze, dict) or not existing_bronze.get("key"):
            raise StorageError("Stream conversion manifest is missing Bronze lineage")
        try:
            existing_bytes = store.get_bytes(str(existing_bronze["key"]))
        except StorageError as exc:
            raise StorageError("Stream conversion manifest references a missing Bronze object") from exc
        expected_digest = existing_bronze.get("sha256")
        if expected_digest and hashlib.sha256(existing_bytes).hexdigest() != expected_digest:
            raise StorageError("Stream conversion Bronze checksum does not match its manifest")
        return {
            "status": "already_converted",
            "data_lake": store.describe(),
            "avro_key": avro_key,
            "bronze_key": existing_bronze["key"],
            "manifest_key": manifest_key,
            "record_count": existing["record_count"],
            "iceberg": existing.get("iceberg"),
        }

    import tempfile

    with tempfile.TemporaryDirectory(prefix="solo-empire-stream-convert-") as temp_dir:
        parquet_path = Path(temp_dir) / "part.parquet"
        write_bronze_parquet(rows, parquet_path)
        parquet_bytes = parquet_path.read_bytes()
    bronze_result = store.put_bytes(
        bronze_key,
        parquet_bytes,
        content_type="application/vnd.apache.parquet",
    )

    iceberg_result: dict[str, Any] | None = None
    if iceberg_catalog_uri:
        from data_lake.iceberg import register_bronze_file

        iceberg_result = register_bronze_file(
            data_lake_uri=data_lake_uri,
            bronze_key=bronze_key,
            domain=domain,
            dataset=dataset,
            catalog_uri=iceberg_catalog_uri,
            warehouse_uri=iceberg_warehouse_uri,
            run_id=str(rows[0].get("ingest_run_id") or conversion_id),
        )

    manifest = {
        "manifest_version": "1",
        "kind": "stream-parquet-conversion",
        "conversion_id": conversion_id,
        "source_avro": {"key": avro_key, "sha256": hashlib.sha256(avro_bytes).hexdigest()},
        "record_count": len(rows),
        "bronze": {"key": bronze_key, "sha256": bronze_result.sha256, "bytes": len(parquet_bytes)},
        "source": source,
        "domain": domain,
        "dataset": dataset,
        "schema_version": schema_version,
        "ingest_run_id": rows[0].get("ingest_run_id"),
    }
    if iceberg_result:
        manifest["iceberg"] = {key: value for key, value in iceberg_result.items() if key != "status"}
    manifest_result = store.put_bytes(
        manifest_key,
        (canonical_json(manifest) + "\n").encode("utf-8"),
        content_type="application/json",
    )
    return {
        "status": "success",
        "data_lake": store.describe(),
        "avro_key": avro_key,
        "bronze_key": bronze_result.key,
        "manifest_key": manifest_result.key,
        "record_count": len(rows),
        "bronze_existed": bronze_result.existed,
        "manifest_existed": manifest_result.existed,
        "iceberg": iceberg_result,
    }


def convert_pending(
    *,
    data_lake_uri: str,
    iceberg_catalog_uri: str | None = None,
    iceberg_warehouse_uri: str | None = None,
) -> dict[str, Any]:
    """Scan the immutable stream landing prefix and convert pending Avro files."""
    store = ObjectStore(data_lake_uri)
    avro_keys = [key for key in store.list_keys("landing/stream") if key.endswith(".avro")]
    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for avro_key in avro_keys:
        try:
            results.append(
                convert_avro_file(
                    data_lake_uri=data_lake_uri,
                    avro_key=avro_key,
                    iceberg_catalog_uri=iceberg_catalog_uri,
                    iceberg_warehouse_uri=iceberg_warehouse_uri,
                )
            )
        except (AvroUnavailable, IcebergUnavailable, ParquetUnavailable, StorageError, ValueError) as exc:
            # Keep polling other files, but expose a degraded status for the
            # healthcheck instead of silently dropping a poison object.
            errors.append({"avro_key": avro_key, "error": type(exc).__name__})
    return {
        "status": "degraded" if errors else "healthy",
        "data_lake": store.describe(),
        "scanned": len(avro_keys),
        "converted": sum(result["status"] == "success" for result in results),
        "already_converted": sum(result["status"] == "already_converted" for result in results),
        "errors": errors,
        "results": results,
    }


def _write_status(status_file: str | None, payload: dict[str, Any]) -> None:
    """Atomically publish a local runtime status file for Compose healthchecks."""
    if not status_file:
        return
    destination = Path(status_file)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    temporary.replace(destination)


def _status_age_seconds(value: str) -> float:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - timestamp).total_seconds()


def _check_status(status_file: str, max_age_seconds: float, as_json: bool) -> int:
    path = Path(status_file)
    try:
        status = json.loads(path.read_text(encoding="utf-8"))
        age_seconds = _status_age_seconds(str(status["last_heartbeat"]))
        status["age_seconds"] = round(age_seconds, 3)
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError) as exc:
        if as_json:
            print(canonical_json({"status": "unhealthy", "error": type(exc).__name__}))
        else:
            print(f"Status unavailable: {type(exc).__name__}", file=sys.stderr)
        return 1
    if as_json:
        print(canonical_json(status))
    else:
        print(f"{status.get('status', 'unknown')} age={status['age_seconds']:.1f}s")
    return 0 if status.get("status") == "healthy" and age_seconds <= max_age_seconds else 1


def _kafka_consumer(args: argparse.Namespace) -> Any:
    try:
        import confluent_kafka  # type: ignore  # noqa: F401
    except ImportError as exc:
        raise AvroUnavailable(
            "The Kafka-compatible sink requires confluent-kafka; install with "
            "python3 -m pip install -r infra/requirements-data-lake.txt"
        ) from exc
    config = kafka_config(args.bootstrap_servers, args.group_id, args.auto_offset_reset, os.environ)
    return ConfluentConsumer(config, args.topic)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sink = subparsers.add_parser("sink", help="consume Kafka-compatible JSON events into Avro")
    sink.add_argument("--bootstrap-servers", default=os.environ.get("STREAM_KAFKA_BOOTSTRAP", "localhost:19092"))
    sink.add_argument("--topic", required=True)
    sink.add_argument("--group-id", required=True)
    sink.add_argument("--data-lake-uri", default=DEFAULT_LAKE_URI)
    sink.add_argument("--source", required=True)
    sink.add_argument("--domain", required=True)
    sink.add_argument("--dataset", required=True)
    sink.add_argument("--schema-version", default=AVRO_SCHEMA_VERSION)
    sink.add_argument("--metadata", help="JSON object attached to every event")
    sink.add_argument("--privacy-class", choices=["public", "internal", "private", "restricted"], default="private")
    sink.add_argument("--retention-class", default="operational")
    sink.add_argument("--poll-ms", type=int, default=5_000)
    sink.add_argument("--max-records", type=int, default=1_000)
    sink.add_argument("--batch-seconds", type=float, default=60.0)
    sink.add_argument("--auto-offset-reset", choices=["earliest", "latest", "none"], default="latest")
    sink.add_argument("--once", action="store_true", help="process one poll window and exit")
    sink.add_argument(
        "--until-empty",
        action="store_true",
        help="process batches until a poll comes back empty, then exit (scheduled runs)",
    )
    sink.add_argument("--max-batches", type=int, default=50, help="upper bound for --until-empty")
    sink.add_argument("--expect-id", help="report whether an event with this envelope id was landed")
    sink.add_argument("--status-file", help="local JSON heartbeat path for healthchecks")
    sink.add_argument("--json", action="store_true", dest="as_json")

    convert = subparsers.add_parser("convert", help="convert one Avro object into Bronze Parquet")
    convert.add_argument("--data-lake-uri", default=DEFAULT_LAKE_URI)
    convert.add_argument("--avro-key", help="one immutable Avro object key")
    convert.add_argument("--watch", action="store_true", help="poll landing/stream for pending Avro objects")
    convert.add_argument("--poll-seconds", type=float, default=60.0)
    convert.add_argument("--once", action="store_true", help="scan once and exit when used with --watch")
    convert.add_argument("--iceberg-catalog-uri")
    convert.add_argument("--iceberg-warehouse-uri")
    convert.add_argument("--status-file", help="local JSON heartbeat path for healthchecks")
    convert.add_argument("--json", action="store_true", dest="as_json")

    status = subparsers.add_parser("status", help="check a sink/converter heartbeat file")
    status.add_argument("--status-file", required=True)
    status.add_argument("--max-age-seconds", type=float, default=180.0)
    status.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "status":
        return _check_status(args.status_file, args.max_age_seconds, args.as_json)
    try:
        if args.command == "convert":
            if args.watch:
                last_success_at: str | None = None
                while True:
                    result = convert_pending(
                        data_lake_uri=args.data_lake_uri,
                        iceberg_catalog_uri=args.iceberg_catalog_uri,
                        iceberg_warehouse_uri=args.iceberg_warehouse_uri,
                    )
                    now = utc_now()
                    if result["status"] == "healthy":
                        last_success_at = now
                    _write_status(
                        args.status_file,
                        {
                            "component": "stream-converter",
                            "status": result["status"],
                            "last_heartbeat": now,
                            "last_success_at": last_success_at,
                            "scanned": result["scanned"],
                            "converted": result["converted"],
                            "already_converted": result["already_converted"],
                            "error_count": len(result["errors"]),
                        },
                    )
                    if args.once:
                        break
                    time.sleep(max(args.poll_seconds, 1.0))
            else:
                if not args.avro_key:
                    raise ValueError("convert requires --avro-key or --watch")
                result = convert_avro_file(
                    data_lake_uri=args.data_lake_uri,
                    avro_key=args.avro_key,
                    iceberg_catalog_uri=args.iceberg_catalog_uri,
                    iceberg_warehouse_uri=args.iceberg_warehouse_uri,
                )
        else:
            metadata = json.loads(args.metadata) if args.metadata else {}
            if not isinstance(metadata, dict):
                raise ValueError("--metadata must be a JSON object")
            consumer = _kafka_consumer(args)
            try:
                _write_status(
                    args.status_file,
                    {
                        "component": "stream-sink",
                        "status": "healthy",
                        "phase": "starting",
                        "last_heartbeat": utc_now(),
                        "last_success_at": None,
                        "record_count": 0,
                        "committed": False,
                        "broker_lag": None,
                    },
                )
                batches = 0
                total_records = 0
                expected_found = False
                while True:
                    result = run_once(
                        consumer,
                        data_lake_uri=args.data_lake_uri,
                        source=args.source,
                        domain=args.domain,
                        dataset=args.dataset,
                        schema_version=args.schema_version,
                        metadata=metadata,
                        privacy_class=args.privacy_class,
                        retention_class=args.retention_class,
                        poll_ms=args.poll_ms,
                        max_records=args.max_records,
                        batch_seconds=0.0 if args.once or args.until_empty else args.batch_seconds,
                        expect_id=args.expect_id,
                    )
                    expected_found = expected_found or bool(result.get("expected_id_found"))
                    batches += result["status"] == "success"
                    total_records += result.get("record_count", 0)
                    now = utc_now()
                    _write_status(
                        args.status_file,
                        {
                            "component": "stream-sink",
                            "status": "healthy",
                            "last_heartbeat": now,
                        "last_success_at": now if result["status"] == "success" else None,
                        "batch_id": result.get("batch_id"),
                        "checkpoint_key": result.get("checkpoint_key"),
                        "record_count": result.get("record_count", 0),
                        "committed": result.get("committed", False),
                        "broker_lag": _consumer_lag(consumer),
                    },
                )
                    if args.once:
                        break
                    if args.until_empty and (result["status"] == "empty" or batches >= args.max_batches):
                        result = {**result, "batches": batches, "total_records": total_records}
                        if args.expect_id is not None:
                            result["expected_id_found"] = expected_found
                        break
            finally:
                consumer.close()
    except (AvroUnavailable, BrokerError, IcebergUnavailable, ParquetUnavailable, StorageError, ValueError) as exc:
        if getattr(args, "status_file", None):
            _write_status(
                args.status_file,
                {
                    "component": f"stream-{args.command}",
                    "status": "failed",
                    "last_heartbeat": utc_now(),
                    "error": type(exc).__name__,
                },
            )
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    if args.as_json:
        print(canonical_json(result))
    elif args.command == "convert":
        if args.watch:
            print(f"Stream converter: scanned={result['scanned']} converted={result['converted']}")
        else:
            print(f"Avro conversion complete: {result['record_count']} record(s)")
            print(f"  Bronze: {result['bronze_key']}")
            print(f"  Manifest: {result['manifest_key']}")
    elif "batches" in result:
        print(f"Stream sink drained: {result['batches']} batch(es), {result['total_records']} record(s)")
    elif result["status"] == "empty":
        print("No streaming events available")
    else:
        print(f"Stream sink complete: {result['record_count']} record(s)")
        print(f"  Avro: {result['avro_key']}")
        print(f"  Manifest: {result['manifest_key']}")
        print(f"  Checkpoint: {result['checkpoint_key']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
