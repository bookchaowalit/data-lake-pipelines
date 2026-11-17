# Jobs Dagster assets (ADR-0016 P1)

Status: accepted 2026-11-17 (owner approved ADR-0016 and asked to start P1).
Decision: architecture-docs `ADR-0016-managed-analytics-stack-on-free-tiers.md`, Rollout P1.

## Goal

The jobs domain becomes a set of Dagster software-defined assets with asset checks, run by the
existing scheduled workflow. Data, keys, Gold run ids, the workflow's JSON and its exit codes stay
the same, so the heartbeat and the BI job do not change.

## Behaviour

1. `infra/scripts/data_lake/dagster_jobs.py` declares the asset graph:
   - six external Bronze assets `jobs/bronze/<dataset>` (owned by career-job-scraper);
   - `jobs/silver/job_postings`, and the five best-effort API Silver datasets (skippable);
   - `jobs/gold/<mart>` for each dbt mart in `JOBS_MARTS`;
   - `jobs/gold/manifest` and `jobs/gold/pointer`.
2. One multi-asset (`jobs_lake`) calls `jobs_marts.run_jobs_marts` once and reports a
   materialization per produced asset. It never re-implements the run.
3. Asset checks, all severity WARN so they never change the exit code:
   `quarantine_ratio` (Silver), `has_rows` (each mart), `dbt_build_ok` (manifest),
   `fresh` and `side_silver_built` (pointer).
4. The CLI runs the assets in-process on an ephemeral Dagster instance (no webserver, daemon or
   stored run history) and prints the `jobs_marts.py` JSON plus `checks`, `orchestrator` and
   `lineage`. Exit codes: 0 success, 3 stale (Gold still published), 2 failed.
5. After a successful run the asset graph is written as lineage JSON to
   `control/lineage/dagster/domain=jobs/graph-<digest>.json`. The document has no run data, so
   the key changes only when the graph changes; a rerun finds it (`existed: true`). The write is
   best-effort: Gold is already published, so a failure is reported as `lineage.error`, not exit 2.
6. `defs` is a normal `Definitions`, so `dagster asset materialize -m data_lake.dagster_jobs
   --select '*'` (with `infra/scripts` on `PYTHONPATH`) works for local use. The UI needs the
   webserver, which is not locked and never installed by the scheduled job:
   `uv run --extra orchestration --with dagster-webserver==1.13.28 dagster dev -m data_lake.dagster_jobs`.
7. Dagster is the `orchestration` extra in `pyproject.toml`, pinned to a release at least two
   weeks old. Telemetry is off (`DAGSTER_DISABLE_TELEMETRY=1`).

## Public log

The repository is public. Dagster's own log goes to stderr, which the workflow keeps in a runner
temp file. The step summary adds only the names of failed checks and the lineage key.

## Tests

`infra/tests/data_lake/dagster_jobs_test.py`: the definitions load; lineage follows
Bronze → Silver → Gold with the checks attached; a run matches the plain runner's Gold run id and
is idempotent; stale and quality-gate failures give the same statuses and exit codes as
`jobs_marts.py`.

## Done when (ADR-0016 P1)

A scheduled run is green, the `jobs-marts` heartbeat is unchanged, and the lineage JSON exists
in the lake.
