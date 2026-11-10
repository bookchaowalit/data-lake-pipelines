#!/usr/bin/env python3
"""Tests for the jobs Silver → dbt marts → Gold runner."""

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

from data_lake.ingest import ingest_payload  # noqa: E402
from data_lake.query import query_parquet  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)


def _job(index, **overrides):
    record = {
        "id": f"job-{index}",
        "title": f"Engineer {index}",
        "company": "Acme",
        "url": f"https://example.com/jobs/{index}",
        "event_time": "2026-10-22T06:00:00Z",
        "posted": "2026-10-20",
        "provider": "book-job-scraping",
        "source": "Himalayas",
        "tags": "",
        "salary": "",
        "keyword": "python",
    }
    record.update(overrides)
    return record


FIXTURE_JOBS = [
    _job(1, tags="Python, SQL", salary="$5k"),
    _job(2, tags="python"),
    _job(3, source="Jobicy", company="Beta", posted="2026-10-21", tags="Go", salary="$1"),
    _job(4, source="Jobicy", company=" beta ", posted="2026-09-01"),
    _job(5, url=""),  # quarantined
    _job(6, provider="offline_fixture"),  # excluded
]


def _ingest(root, records, raw=b"jobs-marts-fixture", dataset="job_postings"):
    ingest_payload(
        raw,
        records,
        source="book-job-data",
        domain="jobs",
        dataset=dataset,
        data_lake_uri=root,
        input_format="text",
        event_time_field="event_time",
        privacy_class="internal",
    )


def _dbt_available() -> bool:
    from data_lake.jobs_marts import resolve_dbt_bin

    return resolve_dbt_bin() is not None


class JobsMartsTests(unittest.TestCase):
    def setUp(self):
        try:
            import duckdb  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("duckdb/pyarrow not installed")
        if not _dbt_available():
            if os.environ.get("SOLO_EMPIRE_CI") == "1":
                self.fail("dbt not found; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
            self.skipTest("dbt not installed")

    def _run(self, root, **kwargs):
        from data_lake.jobs_marts import run_jobs_marts

        kwargs.setdefault("now", NOW)
        # The fixture quarantines 1 of 5 rows on purpose; the real default
        # (0.02) is covered by test_quality_gate_failure_writes_no_gold.
        kwargs.setdefault("max_invalid_ratio", 0.25)
        return run_jobs_marts(data_lake_uri=root, **kwargs)

    def _rows(self, root, result, mart, sql):
        return query_parquet(str(Path(root) / result["marts"][mart]["key"]), sql)

    def test_marts_are_built_from_the_current_silver_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            result = self._run(root)
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["silver"]["record_count"], 4)
            self.assertEqual(result["silver"]["invalid_count"], 1)
            self.assertEqual(result["silver"]["excluded_count"], 1)

            daily = self._rows(
                root, result, "jobs_postings_daily",
                "SELECT CAST(posted_date AS VARCHAR) d, board, postings, companies, with_salary "
                "FROM lake_table ORDER BY d, board",
            )
            self.assertEqual(
                [tuple(row.values()) for row in daily],
                [
                    ("2026-09-01", "Jobicy", 1, 1, 0),
                    ("2026-10-20", "Himalayas", 2, 1, 1),
                    ("2026-10-21", "Jobicy", 1, 1, 1),
                ],
            )

            health = self._rows(
                root, result, "jobs_board_health",
                "SELECT board, postings_total, CAST(last_posted_date AS VARCHAR) last_posted, postings_7d "
                "FROM lake_table ORDER BY board",
            )
            self.assertEqual(
                [tuple(row.values()) for row in health],
                [("Himalayas", 2, "2026-10-20", 2), ("Jobicy", 2, "2026-10-21", 1)],
            )

            skills = self._rows(
                root, result, "jobs_skills_demand",
                "SELECT CAST(posted_week AS VARCHAR) w, tag, postings, demand_rank "
                "FROM lake_table ORDER BY w, demand_rank, tag",
            )
            self.assertEqual(
                [tuple(row.values()) for row in skills],
                [("2026-10-19", "python", 2, 1), ("2026-10-19", "go", 1, 2), ("2026-10-19", "sql", 1, 2)],
            )

            companies = self._rows(
                root, result, "jobs_company_hiring",
                "SELECT company_key, postings_total, postings_30d, boards FROM lake_table ORDER BY company_key",
            )
            self.assertEqual(
                [tuple(row.values()) for row in companies],
                [("acme", 2, 2, ["Himalayas"]), ("beta", 2, 1, ["Jobicy"])],
            )

    def test_offsets_missing_companies_and_repeated_tags(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(
                root,
                [
                    # 18:00 at +07:00 is 11:00 UTC; it must not read as 18:00.
                    _job(1, tags="Python, python, SQL", event_time="2026-10-22T18:00:00+07:00"),
                    _job(2, company="", tags="Go", event_time="2026-10-22T06:00:00Z"),
                ],
            )
            result = self._run(root, now=datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc))
            self.assertEqual(result["freshness"]["max_seen_at"], "2026-10-22T11:00:00+00:00")
            self.assertEqual(result["freshness"]["age_hours"], 1.0)
            health = self._rows(
                root, result, "jobs_board_health",
                "SELECT companies, CAST(last_seen_at AS VARCHAR) seen FROM lake_table",
            )
            self.assertEqual((health[0]["companies"], health[0]["seen"]), (1, "2026-10-22 11:00:00"))
            daily = self._rows(root, result, "jobs_postings_daily", "SELECT companies FROM lake_table")
            self.assertEqual(daily[0]["companies"], 1)
            skills = self._rows(
                root, result, "jobs_skills_demand",
                "SELECT tag, postings FROM lake_table ORDER BY tag",
            )
            self.assertEqual([tuple(row.values()) for row in skills], [("go", 1), ("python", 1), ("sql", 1)])
            companies = self._rows(root, result, "jobs_company_hiring", "SELECT company_key FROM lake_table")
            self.assertEqual([row["company_key"] for row in companies], ["acme"])

    def test_windows_cover_exactly_n_dates_ending_on_as_of(self):
        with tempfile.TemporaryDirectory() as root:
            days = [f"2026-10-{d:02d}" for d in range(1, 23)]  # one posting per date to 10-22
            records = [_job(i, posted=day) for i, day in enumerate(days, start=1)]
            records.append(_job(99, posted="2026-12-31"))  # future-dated: never "recent"
            _ingest(root, records)
            result = self._run(root)
            health = self._rows(root, result, "jobs_board_health", "SELECT postings_7d FROM lake_table")
            self.assertEqual(health[0]["postings_7d"], 7)
            company = self._rows(root, result, "jobs_company_hiring", "SELECT postings_30d FROM lake_table")
            self.assertEqual(company[0]["postings_30d"], 22)

    def test_skills_keep_at_most_50_tags_per_week_even_with_ties(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, [_job(i, tags=f"tag{i:03d}") for i in range(1, 61)])
            result = self._run(root)
            skills = self._rows(
                root, result, "jobs_skills_demand",
                "SELECT count(*) n, min(tag) first_tag, max(tag) last_tag, max(demand_rank) top_rank FROM lake_table",
            )
            self.assertEqual(
                tuple(skills[0].values()), (50, "tag001", "tag050", 1)
            )

    def test_dbt_build_without_an_explicit_silver_part_fails(self):
        import subprocess

        from data_lake.jobs_marts import DBT_PROJECT_DIR, resolve_dbt_bin

        with tempfile.TemporaryDirectory() as temp_dir:
            result = subprocess.run(
                [
                    str(resolve_dbt_bin()), "build", "--select", "stg_job_postings",
                    "--profiles-dir", str(DBT_PROJECT_DIR), "--project-dir", str(DBT_PROJECT_DIR),
                    "--target-path", str(Path(temp_dir) / "target"), "--log-path", str(Path(temp_dir) / "logs"),
                ],
                cwd=str(DBT_PROJECT_DIR),
                env={**os.environ, "DBT_DUCKDB_PATH": str(Path(temp_dir) / "x.duckdb")},
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("jobs_silver_part", result.stdout + result.stderr)

    def test_rerun_is_idempotent_and_manifest_links_silver(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            first = self._run(root)
            second = self._run(root)
            self.assertEqual(first["gold_run_id"], second["gold_run_id"])
            self.assertTrue(second["manifest_existed"])
            self.assertTrue(all(mart["existed"] for mart in second["marts"].values()))
            self.assertTrue(
                all(
                    "/schema_version=1/" in mart["key"] and mart["key"].startswith("gold/domain=jobs/")
                    for mart in first["marts"].values()
                )
            )
            manifest = json.loads(ObjectStore(root).get_bytes(first["manifest_key"]))
            self.assertEqual(manifest["layer"], "gold")
            self.assertEqual(manifest["input"]["silver_run_id"], first["silver_run_id"])
            self.assertEqual(
                {mart["dataset"]: mart["key"] for mart in manifest["marts"]},
                {name: mart["key"] for name, mart in first["marts"].items()},
            )

    def test_each_run_publishes_a_sortable_pointer_to_its_gold_and_silver(self):
        prefix = "control/pointers/gold/source=book-job-data/domain=jobs/"
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            first = self._run(root)
            self.assertRegex(
                first["pointer_key"],
                rf"^{re.escape(prefix)}published_at=20261022T120000Z-{first['gold_run_id']}-[0-9a-f]{{12}}\.json$",
            )
            self.assertFalse(first["pointer_existed"])
            pointer = json.loads(ObjectStore(root).get_bytes(first["pointer_key"]))
            self.assertEqual(
                pointer,
                {
                    "pointer_version": "1",
                    "layer": "gold",
                    "source": "book-job-data",
                    "domain": "jobs",
                    "published_at": "2026-10-22T12:00:00Z",
                    "gold_run_id": first["gold_run_id"],
                    "manifest_key": first["manifest_key"],
                    "silver_run_id": first["silver_run_id"],
                    "silver_key": first["silver"]["key"],
                    "silver_record_count": 4,
                    "datasets": {
                        "job_postings": {
                            "silver_key": first["silver"]["key"],
                            "silver_run_id": first["silver_run_id"],
                            "record_count": 4,
                        }
                    },
                },
            )

            # Same `now`: identical object, a no-op. Later `now`: a new, greater key
            # even though the Gold run is the same.
            self.assertTrue(self._run(root)["pointer_existed"])
            later = self._run(root, now=datetime(2026, 10, 22, 14, 0, tzinfo=timezone.utc))
            self.assertEqual(later["gold_run_id"], first["gold_run_id"])
            keys = ObjectStore(root).list_keys(prefix)
            self.assertEqual(len(keys), 2)
            self.assertEqual(max(keys), later["pointer_key"])

    def test_matches_and_leads_get_envelope_silver_listed_in_the_pointer(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            # Leads often lack title/url; only source_record_id is required.
            _ingest(
                root,
                [_job(1, score="9"), _job(1, score="10", event_time="2026-10-22T07:00:00Z"), _job(2, title="", url="")],
                raw=b"matches",
                dataset="job_matches",
            )
            result = self._run(root)
            store = ObjectStore(root)
            pointer = json.loads(store.get_bytes(result["pointer_key"]))
            self.assertEqual(set(pointer["datasets"]), {"job_postings", "job_matches"})
            self.assertEqual(pointer["datasets"]["job_postings"]["silver_key"], result["silver"]["key"])
            matches = pointer["datasets"]["job_matches"]
            self.assertEqual(matches["record_count"], 2)
            self.assertTrue(
                matches["silver_key"].startswith(
                    "silver/domain=jobs/dataset=job_matches/schema_version=1/event_date="
                )
            )
            rows = query_parquet(
                str(Path(root) / matches["silver_key"]),
                "SELECT source_record_id, payload_json FROM lake_table ORDER BY source_record_id",
            )
            self.assertEqual([row["source_record_id"] for row in rows], ["job-1", "job-2"])
            self.assertEqual(json.loads(rows[0]["payload_json"])["score"], "10")
            # job_leads has no Bronze here: reported, not fatal, and absent from the pointer.
            self.assertEqual(result["status"], "success")
            self.assertIn("error", result["envelope_silver"]["job_leads"])
            self.assertEqual(result["envelope_silver"]["job_matches"]["record_count"], 2)

    def test_history_datasets_get_all_versions_silver_listed_in_the_pointer(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            _ingest(
                root,
                [_job(1, score="9"), _job(1, score="10", event_time="2026-10-22T07:00:00Z"), _job(2)],
                raw=b"matches-history",
                dataset="job_matches_history",
            )
            result = self._run(root)
            pointer = json.loads(ObjectStore(root).get_bytes(result["pointer_key"]))
            history = pointer["datasets"]["job_matches_history"]
            self.assertEqual(history["record_count"], 3)
            self.assertTrue(
                history["silver_key"].startswith(
                    "silver/domain=jobs/dataset=job_matches_history/schema_version=1/event_date="
                )
            )
            self.assertEqual(result["envelope_silver"]["job_matches_history"]["record_count"], 3)
            # No Bronze for the other history datasets: reported, not fatal, absent from the pointer.
            self.assertIn("error", result["envelope_silver"]["job_postings_history"])
            self.assertNotIn("job_postings_history", pointer["datasets"])
            self.assertEqual(result["status"], "success")

    def test_same_time_retry_after_an_envelope_recovers_does_not_conflict(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            first = self._run(root)
            self.assertIn("error", first["envelope_silver"]["job_matches"])
            _ingest(root, [_job(1)], raw=b"matches", dataset="job_matches")
            second = self._run(root)  # same `now`, same Gold, new `datasets`
            self.assertEqual(second["gold_run_id"], first["gold_run_id"])
            self.assertNotEqual(second["pointer_key"], first["pointer_key"])
            pointer = json.loads(ObjectStore(root).get_bytes(second["pointer_key"]))
            self.assertIn("job_matches", pointer["datasets"])

    def test_new_bronze_batch_gets_new_gold_run(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            first = self._run(root)
            _ingest(root, [_job(7, source="Jobicy", company="Gamma")], raw=b"second-batch")
            second = self._run(root)
            self.assertNotEqual(first["gold_run_id"], second["gold_run_id"])
            health = self._rows(
                root, second, "jobs_board_health",
                "SELECT postings_total FROM lake_table WHERE board = 'Jobicy'",
            )
            self.assertEqual(health[0]["postings_total"], 3)

    def test_stale_silver_is_reported_but_gold_still_written(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            result = self._run(root, now=datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc), stale_hours=24)
            self.assertEqual(result["status"], "stale")
            self.assertTrue(result["freshness"]["stale"])
            self.assertGreater(result["freshness"]["age_hours"], 24)
            self.assertTrue(ObjectStore(root).list_keys("gold/domain=jobs/"))

    def test_quality_gate_failure_writes_no_gold(self):
        from data_lake.silver import SilverQualityError

        with tempfile.TemporaryDirectory() as root:
            _ingest(root, [_job(1), _job(2, url=""), _job(3, title="")])
            with self.assertRaises(SilverQualityError):
                self._run(root, max_invalid_ratio=0.02)
            self.assertEqual(ObjectStore(root).list_keys("gold/"), [])
            self.assertEqual(ObjectStore(root).list_keys("control/pointers/"), [])


class ExportTimezoneTest(unittest.TestCase):
    """Gold parts must be byte-identical whatever the host timezone, or a replay of the same
    gold_run_id under another TZ fails with StorageConflict. The export pins DuckDB to UTC."""

    def test_timestamptz_columns_export_as_utc_under_any_host_timezone(self):
        import duckdb  # type: ignore

        from data_lake.jobs_marts import _export_mart

        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "marts.duckdb"
            with duckdb.connect(str(database)) as con:
                con.execute("set TimeZone = 'UTC'")
                con.execute("create table m as select 1 as id, timestamptz '2026-11-01 03:00:00+00' as at")
            outputs = []
            for zone in ("UTC", "Asia/Bangkok"):
                previous = os.environ.get("TZ")
                os.environ["TZ"] = zone
                try:
                    destination = Path(tmp) / f"{zone.replace('/', '-')}.parquet"
                    _export_mart(database, "m", ("id",), destination)
                    outputs.append(destination.read_bytes())
                finally:
                    if previous is None:
                        os.environ.pop("TZ", None)
                    else:
                        os.environ["TZ"] = previous
            self.assertEqual(outputs[0], outputs[1])
            import pyarrow.parquet as pq  # type: ignore

            self.assertEqual(str(pq.read_schema(Path(tmp) / "UTC.parquet").field("at").type.tz), "UTC")


if __name__ == "__main__":
    unittest.main()
