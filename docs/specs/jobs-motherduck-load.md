# Jobs Gold → MotherDuck load (ADR-0016 P2)

Status: built 2026-11-18, waiting for the owner's MotherDuck sign-up (ADR-0016 owner item O2).
Decision: architecture-docs `ADR-0016-managed-analytics-stack-on-free-tiers.md`, Rollout P2.

## Goal

After each jobs Gold publish, the warehouse `empire_gold` on MotherDuck holds the same jobs marts
as R2 Gold. Done when the row counts in MotherDuck equal the Gold manifest counts.

## Behaviour

1. `infra/scripts/data_lake/motherduck_load.py` takes one jobs Gold manifest (the build job's
   `manifest_key`, or the newest jobs Gold pointer when none is given). The pointer must pass the
   reader contract (`pointer_version == "1"`, `layer == "gold"`) or the load fails.
2. Only Gold leaves R2 (ADR-0016 §2), and only marts in `EXPORT_APPROVED`, a list kept apart
   from the publisher's `JOBS_MARTS`: a mart new to Gold is reported in `not_exported` and never
   loaded until it is classified as holding no personal data and added there. Approval covers
   the columns too: a part whose columns differ from the approved list is refused, and only the
   approved columns are selected; `unchanged` is never reported while a warehouse table's
   columns differ from the approved list. A mart taken off the list is dropped in the next load's
   transaction (`dropped`), but only tables the loader's own `_gold_loads` log shows it wrote;
   other tables in the schema are never touched.
   Classification (owner, ADR-0016 §2, 2026-11-17): jobs marts are public postings and
   aggregates; `company` is the employer named in a public job ad (a business, not a person).
   A board that lets individuals post under their own name would need a review of that column.
   The load refuses a key outside
   `control/manifests/gold/source=book-job-data/domain=jobs/`, a manifest that is not jobs Gold,
   one that lists a mart twice or does not list exactly the published marts (`JOBS_MARTS`), a part key other than
   its own mart's part of that Gold run
   (`gold/domain=jobs/dataset=<mart>/schema_version=1/part-<gold_run_id>.parquet`), and any part
   whose sha256 differs from the manifest.
3. All `jobs.<mart>` tables are replaced in one transaction. Row counts are compared with the
   manifest inside that transaction; a mismatch rolls back, so readers never see a partial or
   wrong load.
4. Every load appends a row to `jobs._gold_loads` (seq, gold_run_id, manifest key, counts, time)
   in the same transaction; the highest `seq` is the run the tables hold.
   When the newest row already has this Gold run and the tables still match, the run reports
   `unchanged` and writes nothing.
5. The target is `md:empire_gold` when `MOTHERDUCK_TOKEN` is set (the read-write service token;
   passed to DuckDB in the connection config, never in a connection string or output). `--target <file>` or `JOBS_WAREHOUSE_TARGET` loads into a local DuckDB file instead,
   which the tests use.
6. Without a target the CLI reports `skipped_no_token` and exits 0. Exit codes: 0 loaded,
   unchanged or skipped; 2 failed.
7. `--require-latest` skips a manifest that is no longer the newest jobs Gold run
   (`skipped_not_latest`). The scheduled job sets it, so re-running an old workflow run cannot
   put older data back. The run's pointer key (keys sort by publish time) is stored in
   `_gold_loads.published_key` whenever the run is chosen through the pointer (with or without
   the flag) and checked again inside the load transaction, so a slow or
   concurrent load of an older run is refused too; two loads racing on the same tables conflict
   in the warehouse and one fails. Restoring an older run on purpose is `--manifest-key` without
   the flag, which never reads the pointer.
   Tables are addressed as `"<catalog>"."jobs"."<mart>"`, so an attached database named `jobs`
   cannot shadow the schema. When the same Gold run is re-published, the `unchanged` path still
   appends a log row with the newer pointer key (no mart is rewritten), so the watermark moves.
   The held-run check and the watermark move run in one transaction.
   The workflow step also skips (`skipped_stale_revision`) unless its commit is main's head (and
   fails if main's head cannot be read), so
   "Re-run all jobs" on an old run, which rebuilds Gold with old code and a new publish time,
   cannot write the warehouse.
8. The `warehouse` job in `jobs-marts.yml` runs after `build` (same gate as `bi`: exit 0 or 3).
   A failed or cancelled load adds `jobs-warehouse` to the `jobs-marts` heartbeat. Gold and the
   BI site do not depend on it. Without the secret the job reports `skipped_no_token` before
   installing anything.

## Public log

The job prints status, gold_run_id, warehouse kind and row counts only; an error shows its
exception type. stderr stays in a runner temp file.

## Owner steps to switch it on

1. Sign up MotherDuck Lite (EU region if offered) at https://app.motherduck.com/. Lite has no
   read-only token (ADR-0016 1.1 §3), so create two service accounts, each with a Read/Write
   token: `loader` (this load step) and `reader` (Preset). `reader` gets data only through a
   share (see `jobs-preset-dashboard.md`).
2. Store both in Infisical `/motherduck` (`MOTHERDUCK_TOKEN` for `loader`,
   `MOTHERDUCK_READER_TOKEN` for `reader`).
3. Copy `MOTHERDUCK_TOKEN` to this repository's Actions secrets
   (https://github.com/bookchaowalit/data-lake-pipelines/settings/secrets/actions). The next
   scheduled run loads; check its summary for `status: loaded` and the counts.

## Tests

`infra/tests/data_lake/motherduck_load_test.py`: counts match and a rerun is `unchanged`; a
tampered part is refused and the tables keep the previous load; non-jobs or extra datasets are
refused; the CLI skips without a token and loads the newest pointer with `--target`.
