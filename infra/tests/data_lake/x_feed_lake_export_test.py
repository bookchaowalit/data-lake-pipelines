#!/usr/bin/env python3
"""Regression tests for the X feed lake export (intake-api -> Bronze social/x_feed).

The intake API is a stateful fake served through httpx.MockTransport; the lake writer is the real
``ingest_records`` into a temporary file:// lake, read back with DuckDB. Rows are fake test data.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts" / "utils"))
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(ROOT / "infra" / "tests" / "fixtures"))

from sync.x_feed_lake_export import (  # noqa: E402
    ExportError,
    bearer,
    checked_url,
    export_pending,
)
from x_feed_rows import x_feed_rows  # noqa: E402


class FakeIntake:
    """Landed x_feed intakes and their rows; records every mark."""

    def __init__(self, pulls: dict[str, list[dict[str, Any]]]) -> None:
        self.pulls = pulls
        self.status = {pid: "landed" for pid in pulls}
        self.marks: list[dict[str, Any]] = []
        self.grow_on_mark: str | None = None

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path == "/v1/x-feed/pulls":
            assert request.url.params["status"] == "landed"
            items = [
                {"intake_id": 1000000 + i, "pull_id": pid, "status": status}
                for i, (pid, status) in enumerate(self.status.items())
                if status == "landed"
            ]
            return httpx.Response(200, json={"items": items})
        if request.method == "GET" and path.endswith("/rows"):
            pid = path.split("/")[4]
            rows = self.pulls.get(pid)
            if rows is None:
                return httpx.Response(404, json={"detail": {"error": "not_found"}})
            return httpx.Response(200, json={"pull_id": pid, "record_count": len(rows), "items": rows})
        if request.method == "POST" and path.endswith("/exported"):
            pid = path.split("/")[4]
            body = json.loads(request.content)
            self.marks.append({"pull_id": pid, **body})
            if self.grow_on_mark == pid:
                return httpx.Response(409, json={"detail": {"error": "pull_changed"}})
            self.status[pid] = "verified"
            return httpx.Response(200, json={"pull_id": pid, "status": "verified"})
        return httpx.Response(404, json={"detail": {"error": "not_found"}})


def client_for(fake: FakeIntake) -> httpx.Client:
    return httpx.Client(base_url="http://127.0.0.1:3624", transport=httpx.MockTransport(fake.handle))


def read_bronze(lake: Path) -> list[dict[str, Any]]:
    import duckdb

    parts = sorted((lake / "bronze/domain=social/dataset=x_feed").rglob("part-*.parquet"))
    assert parts, "no Bronze part written"
    con = duckdb.connect()
    rows = con.execute(
        "SELECT source, source_record_id, event_time, payload_json, privacy_class FROM read_parquet(?)",
        [[str(p) for p in parts]],
    ).fetchall()
    return [
        {"source": r[0], "sighting_key": r[1], "event_time": r[2], "payload": json.loads(r[3]), "privacy": r[4]}
        for r in rows
    ]


def test_exports_every_landed_pull_and_marks_it() -> None:
    fake = FakeIntake({"test-pull-a": x_feed_rows("test-pull-a"), "test-pull-b": x_feed_rows("test-pull-b", 3)})
    with tempfile.TemporaryDirectory() as tmp:
        lake = Path(tmp) / "lake"
        result = export_pending(client_for(fake), data_lake_uri=str(lake))
        assert result["status"] == "success", result
        assert result["pending"] == 2 and result["exported"] == 2
        assert fake.status == {"test-pull-a": "verified", "test-pull-b": "verified"}
        mark = fake.marks[0]
        assert mark["record_count"] == 20
        assert mark["lake_bronze_object_key"].startswith(
            "bronze/domain=social/dataset=x_feed/schema_version=1/ingest_date="
        )
        assert mark["lake_manifest_key"].startswith("control/manifests/source=x-feed/")
        bronze = read_bronze(lake)
        assert len(bronze) == 23
        first = next(r for r in bronze if r["sighting_key"].startswith("test-pull-a:1990000000000000000:"))
        assert first["source"] == "x-feed"
        assert first["privacy"] == "private"
        assert first["event_time"] == "2026-10-25T23:30:00+00:00"
        assert first["payload"]["text"] == "TEST DATA 0: fake post about agents"
        assert (lake / "landing/source=x-feed").is_dir()

        again = export_pending(client_for(fake), data_lake_uri=str(lake))
        assert again["pending"] == 0 and again["exported"] == 0


def test_re_export_of_the_same_rows_is_idempotent_in_the_lake() -> None:
    rows = x_feed_rows("test-pull-c", 5)
    with tempfile.TemporaryDirectory() as tmp:
        lake = Path(tmp) / "lake"
        first = export_pending(client_for(FakeIntake({"test-pull-c": rows})), data_lake_uri=str(lake))
        second = export_pending(client_for(FakeIntake({"test-pull-c": rows})), data_lake_uri=str(lake))
        assert first["runs"][0]["bronze_key"] == second["runs"][0]["bronze_key"]
        assert second["runs"][0]["bronze_existed"] is True
        assert len(read_bronze(lake)) == 5


def test_a_pull_that_changed_is_left_for_the_next_run() -> None:
    fake = FakeIntake({"test-pull-d": x_feed_rows("test-pull-d", 2)})
    fake.grow_on_mark = "test-pull-d"
    with tempfile.TemporaryDirectory() as tmp:
        result = export_pending(client_for(fake), data_lake_uri=str(Path(tmp) / "lake"))
        assert result["runs"][0]["status"] == "changed_retry_next_run"
        assert result["exported"] == 0
        assert fake.status["test-pull-d"] == "landed"


def test_a_failing_pull_does_not_stop_the_others() -> None:
    fake = FakeIntake({"test-pull-e": x_feed_rows("test-pull-e", 2)})
    fake.status["test-pull-gone"] = "landed"  # listed, but its rows answer 404
    with tempfile.TemporaryDirectory() as tmp:
        result = export_pending(client_for(fake), data_lake_uri=str(Path(tmp) / "lake"))
        assert result["status"] == "partial"
        statuses = {r["pull_id"]: r["status"] for r in result["runs"]}
        assert statuses == {"test-pull-e": "verified", "test-pull-gone": "failed"}
        failed = next(r for r in result["runs"] if r["status"] == "failed")
        assert failed["error"] == "read pull test-pull-gone: HTTP 404 not_found"


def test_dry_run_writes_nothing() -> None:
    fake = FakeIntake({"test-pull-f": x_feed_rows("test-pull-f", 2)})
    with tempfile.TemporaryDirectory() as tmp:
        lake = Path(tmp) / "lake"
        result = export_pending(client_for(fake), data_lake_uri=str(lake), dry_run=True)
        assert result == {
            "pipeline": "x_feed_lake_export",
            "status": "dry_run",
            "pending": 1,
            "lake_table": "bronze/domain=social/dataset=x_feed/schema_version=1",
            "pull_ids": ["test-pull-f"],
        }
        assert not lake.exists()
        assert fake.marks == []


def test_configuration_is_checked() -> None:
    assert checked_url("https://intake.example/") == "https://intake.example"
    assert checked_url("http://127.0.0.1:3624") == "http://127.0.0.1:3624"
    for bad in ("http://intake.example", "ftp://x", ""):
        try:
            checked_url(bad)
        except ExportError:
            continue
        raise AssertionError(f"accepted {bad!r}")
    assert bearer({"EMPIRE_TOKEN": " abc.def.ghi \n"}) == "abc.def.ghi"
    try:
        bearer({})
    except ExportError:
        pass
    else:
        raise AssertionError("missing token accepted")


def main() -> int:
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
    print(f"x_feed_lake_export_test: ok ({len(tests)} tests)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
