# data-lake-pipelines

Status as of 2026-10-23. Describes only what is in this repository today.

## Purpose

Lake-first pipelines for Solo Empire data products: landing → Bronze Parquet → Silver,
scheduled captures (news, market, SEO, jobs), scraping coverage/health audit, discovery
portfolio, Telegram/Brave collectors, diary intake and a dbt projection (`job.v1` adapter for
`book-job-data` / job-radar).

## Users

The owner and the owner's own data products (`book-*-data`, job-radar). No external users.

## What works

- `infra/scripts/data_lake/`: ingest (JSON/CSV → Bronze Parquet + manifests), object store
  (local `file://` or S3-compatible), DuckDB query, Silver transform, Iceberg catalog helpers,
  maintenance/restore drill, capture runners `run_{jobs,market,seo,news}_capture.py`,
  `run_collection_monitor.py`, and `setup_*_cron.sh` to install them in the owner's crontab.
- `infra/dbt/`: dbt-duckdb projection, built in CI against a synthetic Bronze fixture.
- Tests: every `infra/tests/data_lake/*_test.py` runs in CI (`SOLO_EMPIRE_CI=1` turns
  runtime-dependent skips into failures). Tests that need a product repo skip when
  `SOLO_BOOK_DEV_ROOT` is unset.
- GitHub workflows: shared CI v1.1.1 (Ruff, mypy, tests, security), and offline contract
  workflows (`data-lake-cloud-contract`, `free-cloud-e2e`, weekly schedule) that never write to
  the cloud; `hosted-integrations-preflight`, `silver-parity`.

## How to run

```bash
uv sync --all-extras          # from uv.lock (pyproject mirrors infra/requirements-data-lake.txt)
uv run mypy                   # scope in [tool.mypy] files=
for t in infra/tests/data_lake/*_test.py; do uv run python "$t" || echo "FAIL $t"; done
```

Captures run on the owner's machine via cron (`setup_*_cron.sh plan|install|status|remove`);
lanes still marked `bounded` need the explicit approval env var the script names.

## Observability

Opt-in Sentry for the cron runners (`infra/scripts/data_lake/observability.py`): install the
`sentry` extra and set `SENTRY_DSN`. A run that exits 1 sends one static "capture run failed"
message tagged with the runner and exit code; exit 2 (lock held / lane blocked) is not reported;
an uncaught exception is captured with its type and frame locations only. The allowlist
scrubber drops messages, values, request, user, breadcrumbs and extras. Off without a DSN.

## AI

No LLM provider is called. `run_jobs_capture.py` clears `OPENROUTER_API_KEY` (and other paid
provider keys) in the child environment so the jobs scraper stays on free paths.

## Limits

- No hosted runtime or live URL; data lives outside the repo (local lake or owner's bucket).
- The GitHub `schedule:` workflows are offline contract checks, not production captures.
- mypy covers `data_lake`, `discovery`, `opportunities`; gap: `infra/scripts/utils` (vendored,
  15 errors), `infra/scripts/sync` (2 errors), `infra/scripts/cli` and test scripts are not
  type-checked yet.
- Ruff runs pyflakes + syntax rules only (`ruff.toml`); wider rules are a recorded follow-up.
- `news_scheduler_contract_test`: two tests skip until `book-news-scraping/setup_cron.sh`
  accepts the runner root from the environment.

## Disposition

Keep: active internal data platform for the owner's data products.
