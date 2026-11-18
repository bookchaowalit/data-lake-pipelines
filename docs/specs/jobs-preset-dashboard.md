# Jobs dashboard on Preset (ADR-0016 P3)

Status: repository side built 2026-11-18. The dashboard itself waits for the MotherDuck load to
run (owner item O2, P2) and the Preset sign-up (owner item O3).
Decision: architecture-docs `ADR-0016-managed-analytics-stack-on-free-tiers.md`, Rollout P3.

## Goal

A Preset dashboard for jobs on the MotherDuck warehouse, with its export ZIP in this repository.
Done when its numbers match jobs-insights for 2 weeks.

Preset Starter has no API, so the dashboard is built in the Preset UI. This repository holds
everything that can be checked:

| Piece | Path | What it does |
|---|---|---|
| Dataset SQL | `infra/bi/preset/jobs/datasets/*.sql` | The only SQL the dashboard uses. Paste it into Preset; do not edit it there |
| Parity check | `infra/scripts/data_lake/preset_parity.py` | After each MotherDuck load, runs that SQL and compares it with what jobs-insights computes from the same Gold run |
| Export check | `infra/scripts/data_lake/preset_export_check.py` | Refuses an export ZIP with a credential or data outside the ADR-0016 §2 allow-list |
| Export ZIP | `infra/bi/preset/jobs/*.zip` | The dashboard as Preset exports it; `preset_jobs_test.py` checks every committed ZIP in CI |

## Datasets

All four read `empire_gold.jobs.<mart>` (three-part names, so another MotherDuck database cannot
shadow the schema). Windows end on the newest capture date (`max(as_of_date)`), as on
jobs-insights, not on today.

| Dataset | Rows | jobs-insights page |
|---|---|---|
| `jobs_kpis` | one: `as_of_date`, `postings`, `postings_90d`, `postings_7d`, `boards`, `companies` | Overview tiles |
| `jobs_daily_90d` | `posted_date`, `board`, `postings` for the 90-day window | Overview chart and per-board table |
| `jobs_skills_last_week` | top 20 `demand_rank`, `tag`, `postings` of the last complete week (Monday start) | Skills, first table |
| `jobs_companies_30d` | `company`, `postings_30d`, `postings_total`, `boards`, `last_posted_date`, in page order | Companies |

The physical marts (`jobs.jobs_board_health` and the others) may also be used as datasets, with
their approved columns only.

## Charts (suggested layout)

1. Five Big Number charts on `jobs_kpis`, one per tile, with the `as_of_date` in the subtitle.
2. Stacked bar on `jobs_daily_90d`: x `posted_date`, metric `sum(postings)`, dimension `board`.
3. Table on `jobs_skills_last_week`, ordered by `demand_rank`.
4. Table on `jobs_companies_30d` with a filter on `company`.
5. Table on `jobs.jobs_board_health` (board, postings_total, postings_7d, companies, last_posted_date, last_seen_at).

Dashboard title: `Jobs insights`.

## Parity check

`preset_parity.py` runs inside the `warehouse` job of `jobs-marts.yml`, after a load that reports
`loaded` or `unchanged` (any other load status skips it), with the read-write token:

- It builds the jobs-insights snapshot from the same Gold manifest and takes the site's own
  numbers (`jobs_bi_site.overview_kpis`, `skills_last_week`, the daily rows and the company rows).
- In one transaction it reads the warehouse's newest `_gold_loads` run, runs the four dataset SQL
  files and writes the result, so a load from another session cannot mix two Gold runs. A
  different run (the load was skipped or another session loaded) gives `skipped_run_mismatch`
  and nothing is recorded.
- It compares each tile, `as_of_date`, postings per `(posted_date, board)` in the 90-day window
  (the chart's own points, so a posting moved to another day is a mismatch), the skills week and
  its top 20, and every company row in page order (name, 30-day and all-time postings, boards as
  the page prints them, last posting date). Daily rows are summed per key as the chart sums them,
  so a repeated row is a mismatch.
- Each check appends a row to `jobs._preset_parity` (`seq`, `gold_run_id`, `checked_at`, `ok`,
  `mismatches_json`, `export_sha256`). `export_sha256` fingerprints the dashboard export ZIPs
  committed at that revision, and is empty when none is committed or one fails the export check.
- Two streaks, both broken by a mismatch or by more than 6 h between two checks (jobs-marts runs
  every 2 h; time nobody checked is not evidence):
  - `dataset_matching_days`: the dataset SQL alone matched. This says nothing about a dashboard.
  - `dashboard_matching_days`: only checks made with the same committed export count, so it starts
    when the export is first committed and restarts after every re-export.
  `p3_parity_met` is true once the dashboard streak is 14 days long.
- The log shows status, run id, the names of the comparisons that differ, the export state and the
  streaks, never values. A mismatch exits 2, fails the job and the heartbeat reports
  `jobs-warehouse`.

What the check proves: the committed dashboard reads only the repository's dataset SQL (the export
check refuses anything else), and that SQL returned the jobs-insights numbers on every check for
14 days. It does not see how a chart aggregates or filters a dataset in the Preset UI; the owner's
look at the dashboard (step 7) covers that.

Locally, against a DuckDB file named `empire_gold.duckdb` that `motherduck_load.py` loaded:

```bash
.venv/bin/python infra/scripts/data_lake/preset_parity.py --manifest-key <key> --target /tmp/empire_gold.duckdb
```

## Owner steps

1. Finish P2 first: `MOTHERDUCK_TOKEN` in the repository secrets, and a jobs-marts run whose
   warehouse step reports `status: loaded`.
2. Sign up for Preset Starter at https://preset.io/pricing/ with the personal owner identity (EU
   region if offered). Store the login in Infisical `/preset`.
3. Add a database: Settings → Database Connections → + Database → MotherDuck (or DuckDB).
   - SQLAlchemy URI: `duckdb:///md:empire_gold`
   - Advanced → Security → Secure Extra:
     `{"connect_args": {"config": {"motherduck_token": "<MOTHERDUCK_TOKEN_RO from Infisical /motherduck>"}}}`
   - Use the **read-only** token. If Preset only accepts the token in the URI
     (`?motherduck_token=…`; not confirmed for Preset), that works too, but the export will then
     carry the token: the export check refuses it, and the token must be removed from
     `databases/*.yaml` before the ZIP is committed.
4. SQL Lab → paste each file from `infra/bi/preset/jobs/datasets/` unchanged → Save → Save
   dataset, named after the file (`jobs_kpis` and so on).
5. Build the charts and the `Jobs insights` dashboard above.
6. Dashboards → ⋯ → Export. Save the ZIP as `infra/bi/preset/jobs/jobs-dashboard.zip`, then:

   ```bash
   .venv/bin/python infra/scripts/data_lake/preset_export_check.py
   ```

   Commit the ZIP only when it prints `ok`. Export again and commit after every dashboard change.
7. Right after committing the first export, compare each dashboard chart with
   https://jobs-insights.pages.dev once by eye (same tiles, chart points, skills and companies).
   P3 is done when a jobs-marts run summary shows `p3_parity_met: true` (14 days with that same
   export) and a second look at the end still matches. jobs-insights keeps running until the
   owner retires it (ADR-0016 Rollout).

## Export check rules

`preset_export_check.py` fails a ZIP when:

- a credential is found. Every YAML file is parsed and walked, including JSON held in string
  values (chart `params`, `query_context`, database `extra`), and also scanned as raw text: a
  `motherduck_token` value, a JWT, or a key named like a password, secret, token, API key,
  private key, credential, access key or `encrypted_extra` that holds a value (Preset's masked
  `XXXXXXXXXX` is allowed);
- a database URI is not exactly `duckdb:///md:empire_gold` (no query parameters);
- a physical dataset is not an `EXPORT_APPROVED` mart in schema `jobs`, or lists a column outside
  that mart's approved columns;
- a SQL dataset's SQL differs from every file in `infra/bi/preset/jobs/datasets/` (comments,
  whitespace and case are ignored outside quoted text; string literals must match exactly);
- a SQL fragment Preset builds queries from (a calculated column, a dataset metric, or any
  `sqlExpression`, `expression`, `where` or `having` in a chart, including `query_context`
  extras) names anything other than its own dataset's columns
  and a fixed list of SQL keywords and functions, or uses a quoted or dotted name, a comment or
  `;`; a chart points at a dataset that is not in the export;
- a dashboard holds SQL, shows a chart that is not in the export, or does not show at least one
  chart on each of the four SQL datasets (the parity check proves only what they return);
- an entry is not YAML, has an absolute or `..` path, or is over 5 MB; or there is no dashboard.

Problems name the file and the rule, never the matched text.
