#!/usr/bin/env python3
"""Publish one ``ops.heartbeat.emitted`` envelope to the event plane (ADR-0008).

The hourly event-sink workflow runs this before draining the topics, so every
run is also an end-to-end check (produce -> broker -> lake) and the free Aiven
service never sits idle. Credentials: the ``ops-heartbeat`` SASL user, which
may only write ``ops.events.v1``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Mapping

TOPIC = "ops.events.v1"
PRODUCER = "data-lake-pipelines/event-sink"
PUBLISH_TIMEOUT_SECONDS = 15.0


def heartbeat_envelope(run_id: str, *, now: datetime | None = None) -> dict[str, Any]:
    occurred_at = (now or datetime.now(timezone.utc)).isoformat().replace("+00:00", "Z")
    return {
        "id": f"ops.heartbeat.emitted:{uuid.uuid4()}",
        "type": "ops.heartbeat.emitted",
        "version": 1,
        "occurred_at": occurred_at,
        "producer": PRODUCER,
        "correlation_id": run_id,
        "causation_id": None,
        "data": {"run_id": run_id},
    }


def producer_config(environ: Mapping[str, str]) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": environ["STREAM_KAFKA_BOOTSTRAP"],
        "acks": "all",
        "enable.idempotence": True,
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


def publish(producer: Any, envelope: dict[str, Any], *, timeout: float = PUBLISH_TIMEOUT_SECONDS) -> None:
    """Produce one envelope and wait for the broker ack; raise on error or timeout."""
    result: dict[str, Any] = {}
    producer.produce(
        TOPIC,
        key=envelope["producer"].encode(),
        value=json.dumps(envelope, separators=(",", ":")).encode(),
        on_delivery=lambda err, _message: result.setdefault("err", err),
    )
    deadline = time.monotonic() + timeout
    while "err" not in result and time.monotonic() < deadline:
        producer.poll(0.2)
    if "err" not in result:
        raise TimeoutError("broker did not acknowledge the heartbeat")
    if result["err"] is not None:
        raise RuntimeError(result["err"].name())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID") or f"local-{uuid.uuid4().hex[:12]}")
    args = parser.parse_args(argv)
    try:
        from confluent_kafka import Producer  # type: ignore
    except ImportError:
        print("ERROR: confluent-kafka is not installed", file=sys.stderr)
        return 2
    envelope = heartbeat_envelope(args.run_id)
    try:
        producer = Producer(producer_config(os.environ))
        publish(producer, envelope)
        producer.flush(5)
    except (KeyError, RuntimeError, TimeoutError) as exc:
        # Exception type only: the public workflow log must not carry broker detail.
        print(f"ERROR: heartbeat not published ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(json.dumps({"status": "published", "topic": TOPIC, "id": envelope["id"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
