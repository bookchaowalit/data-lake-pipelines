#!/usr/bin/env python3
"""Offline tests for the bounded News collection-to-lake runner."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import run_news_capture as runner  # noqa: E402


class FakeNewsStore:
    def __init__(self, current: dict, history: dict) -> None:
        self.current = current
        self.history = history
        self.min_ingest_dates: list[str | None] = []

    def load_records(self, *, data_lake_uri: str, min_ingest_date: str | None = None) -> dict:
        self.min_ingest_dates.append(min_ingest_date)
        return self.current

    def load_history(self, *, data_lake_uri: str, min_ingest_date: str | None = None) -> dict:
        self.min_ingest_dates.append(min_ingest_date)
        return self.history


def _fresh_item() -> dict[str, str]:
    return {"updated_at": runner.utc_now_iso()}


def _healthy_store() -> FakeNewsStore:
    now = runner.utc_now_iso()
    return FakeNewsStore(
        {
            "items": [
                {"updated_at": now, "source": source}
                for source in runner.NEWS_FEEDS
            ],
            "data_status": "ok",
            "read_mode": "parquet",
            "read_fallback": "error",
            "source_kind": "bronze_parquet",
        },
        {
            "items": [_fresh_item()],
            "data_status": "ok",
        },
    )


class NewsCaptureRuntimeTests(unittest.TestCase):
    def test_cycle_retries_a_transient_failure_and_then_reports_health(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(
            runner,
            "_run_collector",
            side_effect=[runner.NewsCycleError("collector_timeout"), None],
        ) as collector, patch.object(
            runner,
            "_validate_captures",
            return_value=[{"dataset": "articles", "history": False, "record_count": 1}],
        ), patch.object(
            runner,
            "_commit_captures",
            return_value=[{"dataset": "articles", "history": False, "record_count": 1}],
        ), patch.object(
            runner,
            "check_news_health",
            return_value={"data_status": "ok", "record_count": 1, "history_count": 1},
        ), patch.object(runner.time, "sleep") as sleep:
            report = runner.run_news_cycle(
                lake_uri=directory,
                lock_path=Path(directory) / "news.lock",
                attempts=2,
                retry_backoff_seconds=0.25,
            )
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["attempts"], 2)
        self.assertEqual(collector.call_count, 2)
        sleep.assert_called_once_with(0.25)

    def test_ingest_retry_replays_the_same_capture_without_collecting_again(self) -> None:
        prepared = (
            runner.PreparedCapture(
                spec=runner.NEWS_CAPTURES[0],
                raw=b"capture",
                records=tuple(),
                capture_mode="snapshot",
            ),
        )
        with tempfile.TemporaryDirectory() as directory, patch.object(
            runner,
            "_run_collector",
        ) as collector, patch.object(
            runner,
            "_history_baselines",
            return_value={},
        ), patch.object(
            runner,
            "_validate_captures",
            return_value=(prepared, [{"dataset": "articles", "record_count": 1}]),
        ), patch.object(
            runner,
            "_commit_captures",
            side_effect=[runner.NewsCycleError("ingest_StorageError"), [{"dataset": "articles", "record_count": 1}]],
        ) as commit, patch.object(
            runner,
            "check_news_health",
            return_value={"data_status": "ok", "record_count": 1, "history_count": 1},
        ), patch.object(runner.time, "sleep"):
            report = runner.run_news_cycle(
                lake_uri=directory,
                lock_path=Path(directory) / "news.lock",
                attempts=2,
                retry_backoff_seconds=0,
            )
        self.assertEqual(report["status"], "ok")
        self.assertEqual(collector.call_count, 1)
        self.assertEqual(commit.call_count, 2)
        self.assertIs(commit.call_args_list[0].args[1], prepared)
        self.assertIs(commit.call_args_list[1].args[1], prepared)

    def test_health_retry_replays_the_same_capture_without_collecting_again(self) -> None:
        prepared = (
            runner.PreparedCapture(
                spec=runner.NEWS_CAPTURES[0],
                raw=b"capture",
                records=tuple(),
                capture_mode="snapshot",
            ),
        )
        with tempfile.TemporaryDirectory() as directory, patch.object(
            runner, "_run_collector"
        ) as collector, patch.object(
            runner, "_history_baselines", return_value={}
        ), patch.object(
            runner,
            "_validate_captures",
            return_value=(prepared, [{"dataset": "articles", "record_count": 1}]),
        ), patch.object(
            runner,
            "_commit_captures",
            return_value=[{"dataset": "articles", "record_count": 1}],
        ) as commit, patch.object(
            runner,
            "check_news_health",
            side_effect=[runner.NewsCycleError("health_feed_stale_matichon"), {"data_status": "ok"}],
        ), patch.object(runner.time, "sleep"):
            report = runner.run_news_cycle(
                lake_uri=directory,
                lock_path=Path(directory) / "news.lock",
                attempts=2,
                retry_backoff_seconds=0,
            )
        self.assertEqual(report["status"], "ok")
        self.assertEqual(collector.call_count, 1)
        self.assertEqual(commit.call_count, 2)
        self.assertIs(commit.call_args_list[0].args[1], prepared)
        self.assertIs(commit.call_args_list[1].args[1], prepared)

    def test_collector_failure_does_not_touch_lake_ingest(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(
            runner, "_run_collector", side_effect=runner.NewsCycleError("collector_failed")
        ), patch.object(runner, "_validate_captures") as validate, patch.object(
            runner, "_commit_captures"
        ) as commit:
            report = runner.run_news_cycle(
                lake_uri=directory,
                lock_path=Path(directory) / "news.lock",
                attempts=1,
            )
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error"], "collector_failed")
        validate.assert_not_called()
        commit.assert_not_called()

    def test_lock_contention_returns_a_retryable_locked_status(self) -> None:
        with patch.object(
            runner, "acquire_lock", side_effect=runner.LockBusyError("lock_busy")
        ):
            report = runner.run_news_cycle(lake_uri="/tmp/news-test-lake")
        self.assertEqual(report["status"], "locked")
        self.assertEqual(report["error"], "lock_busy")

    def test_collector_timeout_is_redacted_to_a_stable_error_class(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector = Path(directory) / "run_feeds.py"
            collector.write_text("", encoding="utf-8")
            with patch.object(runner, "NEWS_COLLECTOR", collector), patch.object(
                runner, "NEWS_REPO", Path(directory)
            ), patch.object(
                runner.subprocess,
                "run",
                side_effect=subprocess.TimeoutExpired(["python", "run_feeds.py"], 1),
            ):
                with self.assertRaises(runner.NewsCycleError) as raised:
                    runner._run_collector(limit=1, timeout_seconds=1)
        self.assertEqual(raised.exception.code, "collector_timeout")

    def test_health_gate_rejects_stale_current_records(self) -> None:
        old = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        store = FakeNewsStore(
            {
                "items": [{"updated_at": old}],
                "data_status": "stale",
                "read_mode": "parquet",
                "read_fallback": "error",
            },
            {"items": [{"updated_at": old}], "data_status": "stale"},
        )
        with self.assertRaises(runner.NewsCycleError) as raised:
            runner.check_news_health("/tmp/news-test-lake", store_module=store)
        self.assertEqual(raised.exception.code, "health_current_stale")

    def test_health_gate_reports_counts_and_latest_timestamp(self) -> None:
        report = runner.check_news_health(
            "/tmp/news-test-lake", store_module=_healthy_store()
        )
        self.assertEqual(report["data_status"], "ok")
        self.assertEqual(report["record_count"], len(runner.NEWS_FEEDS))
        self.assertEqual(report["history_count"], 1)
        self.assertEqual(report["read_mode"], "parquet")
        self.assertTrue(report["latest_observed_at"].endswith("Z"))
        self.assertEqual(set(report["feeds"]), set(runner.NEWS_FEEDS))
        self.assertTrue(all(item["status"] == "ok" for item in report["feeds"].values()))

    def test_health_gate_reads_only_recent_ingest_dates(self) -> None:
        # Reading every Bronze part ever written grew past the 900 s cron cap (2026-11-02);
        # freshness only needs the partitions inside the freshness window plus a day.
        store = _healthy_store()
        runner.check_news_health("/tmp/news-test-lake", max_age_hours=24.0, store_module=store)
        expected = (datetime.now(timezone.utc) - timedelta(hours=48)).date().isoformat()
        self.assertEqual(store.min_ingest_dates, [expected, expected])

    def test_health_gate_rejects_a_missing_feed_even_when_another_feed_is_fresh(self) -> None:
        now = runner.utc_now_iso()
        store = FakeNewsStore(
            {
                "items": [{"updated_at": now, "source": source} for source in runner.NEWS_FEEDS[:-1]],
                "data_status": "ok",
                "read_mode": "parquet",
                "read_fallback": "error",
            },
            {"items": [{"updated_at": now}], "data_status": "ok"},
        )
        with self.assertRaises(runner.NewsCycleError) as raised:
            runner.check_news_health("/tmp/news-test-lake", store_module=store)
        self.assertEqual(raised.exception.code, "health_feed_empty_notebookspec")

    def test_health_gate_rejects_one_stale_feed_when_aggregate_is_fresh(self) -> None:
        fresh = runner.utc_now_iso()
        stale = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        items = [
            {"updated_at": stale if source == "Blognone" else fresh, "source": source}
            for source in runner.NEWS_FEEDS
        ]
        store = FakeNewsStore(
            {
                "items": items,
                "data_status": "ok",
                "read_mode": "parquet",
                "read_fallback": "error",
            },
            {"items": [{"updated_at": fresh}], "data_status": "ok"},
        )
        with self.assertRaises(runner.NewsCycleError) as raised:
            runner.check_news_health("/tmp/news-test-lake", store_module=store)
        self.assertEqual(raised.exception.code, "health_feed_stale_blognone")

    def test_history_delta_accepts_append_and_rejects_rewrite(self) -> None:
        before = b"captured_at,article_id\n2026-09-12T00:00:00Z,a-1\n"
        after = before + b"2026-09-12T01:00:00Z,a-2\n"
        delta, mode = runner._history_delta(before, after)
        self.assertEqual(mode, "append_delta")
        self.assertEqual(delta, b"captured_at,article_id\n2026-09-12T01:00:00Z,a-2\n")
        with self.assertRaises(runner.NewsCycleError) as raised:
            runner._history_delta(before, b"captured_at,article_id\nrewritten\n")
        self.assertEqual(raised.exception.code, "capture_history_rewrite")


if __name__ == "__main__":
    unittest.main()
