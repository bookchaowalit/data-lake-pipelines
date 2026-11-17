#!/usr/bin/env python3
"""Jobs Dagster assets (ADR-0016 P1): graph, checks, lineage JSON and the jobs_marts.py CLI contract."""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_lake.storage import ObjectStore  # noqa: E402
from jobs_marts_test import FIXTURE_JOBS, NOW, _dbt_available, _ingest, _job  # noqa: E402


def _require_runtime(test: unittest.TestCase, *, dbt: bool) -> None:
    missing = []
    try:
        import dagster  # noqa: F401
    except ImportError:
        missing.append("dagster (uv sync --extra orchestration)")
    if dbt and not _dbt_available():
        missing.append("dbt")
    if missing:
        if os.environ.get("SOLO_EMPIRE_CI") == "1":
            test.fail(
                f"{', '.join(missing)} not installed; SOLO_EMPIRE_CI=1 requires them"
            )
        test.skipTest(f"{', '.join(missing)} not installed")


class JobsAssetGraphTests(unittest.TestCase):
    def setUp(self):
        _require_runtime(self, dbt=False)

    def test_definitions_load(self):
        import dagster as dg
        from data_lake.dagster_jobs import defs

        dg.Definitions.validate_loadable(defs)

    def test_lineage_follows_bronze_silver_gold(self):
        from data_lake.dagster_jobs import lineage_document
        from data_lake.jobs_marts import JOBS_MARTS

        doc = lineage_document()
        assets = {node["key"]: node for node in doc["assets"]}
        self.assertEqual(doc["domain"], "jobs")
        self.assertEqual(
            assets["jobs/silver/job_postings"]["upstream"], ["jobs/bronze/job_postings"]
        )
        self.assertTrue(assets["jobs/bronze/job_postings"]["external"])
        self.assertFalse(assets["jobs/silver/job_postings"]["external"])
        for mart in JOBS_MARTS:
            self.assertEqual(
                assets[f"jobs/gold/{mart}"]["upstream"], ["jobs/silver/job_postings"]
            )
            self.assertEqual(assets[f"jobs/gold/{mart}"]["checks"], ["has_rows"])
        self.assertEqual(
            assets["jobs/gold/manifest"]["upstream"],
            sorted(f"jobs/gold/{mart}" for mart in JOBS_MARTS),
        )
        self.assertIn("jobs/gold/manifest", assets["jobs/gold/pointer"]["upstream"])
        self.assertIn(
            "jobs/silver/job_matches", assets["jobs/gold/pointer"]["upstream"]
        )
        self.assertEqual(
            assets["jobs/gold/pointer"]["checks"], ["fresh", "side_silver_built"]
        )
        self.assertEqual(
            assets["jobs/silver/job_postings"]["checks"], ["quarantine_ratio"]
        )

    def test_lineage_is_stable(self):
        from data_lake.dagster_jobs import lineage_document

        self.assertEqual(
            json.dumps(lineage_document(), sort_keys=True),
            json.dumps(lineage_document(), sort_keys=True),
        )


class JobsAssetRunTests(unittest.TestCase):
    def setUp(self):
        _require_runtime(self, dbt=True)

    def _run(self, root, **kwargs):
        from data_lake.dagster_jobs import run

        kwargs.setdefault("now", NOW)
        kwargs.setdefault("max_invalid_ratio", 0.25)
        return run(data_lake_uri=root, **kwargs)

    def _main(self, argv):
        from data_lake import dagster_jobs

        out = io.StringIO()
        with (
            mock.patch.object(sys, "argv", ["dagster_jobs.py", *argv]),
            contextlib.redirect_stdout(out),
        ):
            code = dagster_jobs.main()
        return code, json.loads(out.getvalue())

    def test_run_returns_the_jobs_marts_result_with_checks_and_lineage(self):
        from data_lake.jobs_marts import run_jobs_marts

        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            result = self._run(root)
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["silver"]["record_count"], 4)
            self.assertEqual(result["orchestrator"]["name"], "dagster")

            checks = result["checks"]
            self.assertTrue(checks["jobs/silver/job_postings:quarantine_ratio"])
            self.assertTrue(checks["jobs/gold/jobs_postings_daily:has_rows"])
            self.assertTrue(checks["jobs/gold/manifest:dbt_build_ok"])
            self.assertTrue(checks["jobs/gold/pointer:fresh"])
            # The fixture lands no matches/leads Bronze, so the API datasets are missing.
            self.assertFalse(checks["jobs/gold/pointer:side_silver_built"])

            lineage_key = result["lineage"]["key"]
            self.assertTrue(
                lineage_key.startswith("control/lineage/dagster/domain=jobs/graph-")
            )
            lineage = json.loads((Path(root) / lineage_key).read_bytes())
            self.assertEqual(lineage["producer"], "dagster")

            # Same Bronze, models and writers: the plain runner finds the same Gold run,
            # so the Dagster path writes exactly what jobs_marts.py writes.
            plain = run_jobs_marts(data_lake_uri=root, max_invalid_ratio=0.25, now=NOW)
            self.assertEqual(plain["gold_run_id"], result["gold_run_id"])
            self.assertTrue(plain["manifest_existed"])

            again = self._run(root)
            self.assertTrue(again["lineage"]["existed"])
            self.assertTrue(again["manifest_existed"])

    def test_cli_exit_codes_match_jobs_marts(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            code, payload = self._main(
                ["--data-lake-uri", root, "--max-invalid-ratio", "0.25", "--json"]
            )
            # Real clock: the fixture postings are weeks old, so the run is stale (exit 3).
            self.assertEqual(code, 3)
            self.assertEqual(payload["status"], "stale")
            self.assertFalse(payload["checks"]["jobs/gold/pointer:fresh"])
            self.assertTrue(ObjectStore(root).list_keys("gold/domain=jobs/"))

    def test_stale_run_fails_the_fresh_check_but_publishes(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            result = self._run(
                root,
                now=datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc),
                stale_hours=24,
            )
            self.assertEqual(result["status"], "stale")
            self.assertFalse(result["checks"]["jobs/gold/pointer:fresh"])
            self.assertTrue(result["pointer_key"])

    def test_quality_gate_failure_is_a_failed_run_without_gold(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, [_job(1), _job(2, url=""), _job(3, title="")])
            result = self._run(root, max_invalid_ratio=0.02)
            self.assertEqual(result["status"], "failed")
            self.assertTrue(
                result["error"].startswith("SilverQualityError"), result["error"]
            )
            self.assertEqual(ObjectStore(root).list_keys("gold/"), [])
            self.assertEqual(ObjectStore(root).list_keys("control/lineage/"), [])

            code, payload = self._main(["--data-lake-uri", root, "--json"])
            self.assertEqual(code, 2)
            self.assertEqual(payload["status"], "failed")


if __name__ == "__main__":
    unittest.main()
