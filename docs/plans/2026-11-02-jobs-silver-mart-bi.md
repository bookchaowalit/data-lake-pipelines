# Jobs: Bronze → Silver → Mart → BI Implementation Plan

> **For agentic workers:** steps use checkbox (`- [ ]`) syntax. TDD per ADR-0004: red test first, then the smallest change.

**Goal:** Turn the jobs Bronze data that already lands in R2 every 6 h into a governed Silver table, a small set of Gold marts, and one BI view, so the full layered architecture (sources → ingestion → platform → mart → BI) runs end to end for one domain.

**Architecture:** Keep the scheduler we already have. The Track B workflow in `career-job-scraper` (GitHub-hosted, public repo) captures and writes Bronze to `s3://solo-empire-portfolio/portfolio-demo/book-job-data-001`. A new job in this repo runs right after it: `silver.py` (jobs normalizer) → `dbt build` against Silver Parquet in R2 (dbt-duckdb with httpfs) → write Gold marts as Parquet to `gold/domain=jobs/` → BI reads Gold only. DuckDB stays a compute layer; it is never the source of truth.

**Tech Stack:** Python 3.13, existing `silver.py` / `product_adapter.py` / `storage.py`, dbt-duckdb, DuckDB httpfs (R2 S3 API), GitHub Actions, BI tool picked in decision D2.

**Spec:** Owner request 2026-11-02: "data source หลายที่ → ingestion → data platform → data mart → BI"; jobs domain first.

## Current state (measured 2026-11-02)

| Layer | State |
|---|---|
| Sources | 9 boards via `book-job-scraping`: Himalayas 2476, WorkingNomads 1041, Jobicy 400, RemoteOK 240, HN_WhoIsHiring 78, TheMuse 25, Remotive 25, Landing.jobs 6, JobsDB-TH 1 (unique jobs) |
| Ingestion | Track B "Scheduled capture and deploy to R2" in `career-job-scraper`, green every ~6 h (last 2026-11-02 06:15 UTC) |
| Bronze | `book-job-data-001/bronze/domain=jobs/`: `job_postings` 43 parts / 17,428 rows / 4,292 unique ids since 2026-08-07; plus `job_matches`, `job_leads` (+ `_history`) |
| Silver | `silver.py` jobs normalizer + tests exist; **never run on R2** (no `silver/` prefix) |
| Gold / mart | none for jobs (only `email_activity_daily` + metadata models, local DuckDB in CI) |
| BI | none |

Known data issues found while sampling:

1. `normalize_job_row` takes `provider` first, so every row gets `book-job-scraping`. The board name sits in payload `source`. Silver needs a `board` column from `source`.
2. 129 Bronze rows have no URL. Today `transform_bronze_to_silver()` is **fail-closed**: any invalid row raises `SilverQualityError` before a part or manifest is written (enforced by `test_quality_failure_is_fail_closed_before_silver_write`). So the first R2 run would abort. Task 1 adds an explicit quarantine policy for jobs; until then Task 2 cannot run.
3. 2 rows with `provider=offline_fixture` reached prod Bronze → exclude in Silver and find the leak.
4. There is a second jobs path: `free-cloud-e2e-001/bronze/domain=income/dataset=remote_job_feeds` (702 parts, last write 2026-10-29). Two lakes for one domain → decision D3.
5. Payload carries `tags` and `keyword` → enough for a skills-demand mart.

## Decisions for the owner

- **D1 Scope of v1 marts** (recommend: postings only). `job_matches` holds personal fit scores → keep it out of anything shared.
- **D2 BI tool** (recommend: Evidence.dev static site on Cloudflare Pages behind Cloudflare Access). Zero server cost, SQL + Markdown in git, reads Gold Parquet at build time. Alternatives: Metabase on Railway (needs an always-on service + Postgres, plus the deploy-approval gate), or a page in an existing app (tracking / life-dashboard).
- **D3 `remote_job_feeds` in `domain=income`**: fold into `book-job-data-001` and retire, or keep as a separate product. Recommend retire after confirming no consumer reads it.

## Global Constraints

- Gold and BI read **Silver/Gold only**, never Bronze or landing.
- Writes are limited to `silver/domain=jobs/`, `gold/domain=jobs/`, `control/manifests/{silver,gold}/source=book-job-data/`.
- R2 key from Infisical `/job-data`; scoped to the `solo-empire-portfolio` bucket. Never print values; list names as JSON keys only.
- Public-repo CI → GitHub-hosted runners (CI hosting policy).
- Idempotent: rerunning on the same Bronze manifest writes nothing new.
- Every run sends a heartbeat (`jobs-silver-gold`) to the uptime Worker; alert if > 8 h.

### Task 1: Fix the jobs Silver contract ✅ (branch `feat/jobs-silver-v2-quarantine`)

**Files:** `infra/scripts/data_lake/silver.py`, `infra/tests/data_lake/silver_test.py`

- [x] Red → green: `board` comes from payload `source` (`provider` stays the scraper name).
- [x] Red → green: `provider=offline_fixture` rows are excluded (`SilverProductContract.excluded_providers`), counted as `quality.excluded_count`.
- [x] `board`, `tags` (comma string or list → JSON array), `keyword` added to `JOB_SILVER_COLUMNS`; `job.silver.v2` / `job-postings-silver.v2`. `silver_schema_version` stays `1` (additive columns, nothing on R2 yet).
- [x] Quarantine policy `SilverProductContract.max_invalid_ratio` (CLI `--max-invalid-ratio`), default `0.0` = fail-closed for every domain; existing fail-closed test unchanged and green.
  - [x] Within limit: valid rows → `dataset=job_postings`, invalid rows (with `quality_errors`) → `dataset=job_postings_rejects`; manifest `quality.status=passed_with_rejects` + `rejects` block; rerun is a no-op.
  - [x] Above limit, or no valid rows: raises `SilverQualityError`, writes nothing.
  - [x] A different ratio over the same input gets its own run id (objects are write-once; without this the manifest raised `StorageConflict`).
- [x] Full suite 51/51 files green; ruff clean.
- [x] Dry run on a local copy of the real R2 Bronze (2026-11-02): 17,428 rows → 4,292 latest → 2 fixture excluded → **7 invalid (0.16 %)** → 4,283 valid across 8 boards. With `0.0` it fails closed as intended.
- [ ] Jobs scheduled run uses `--max-invalid-ratio 0.02` (≈ 12× today's rate; a jump past 2 % means a board broke and should stop the run).

### Task 2: First Silver run on R2 ✅ (2026-11-02, main `a5f1453`)

- [x] Run inside the Infisical env so the secrets expand there, not in the outer shell (single quotes matter). `ObjectStore` shells out to the `aws` CLI for S3, so it must be on `PATH` (GitHub-hosted Ubuntu has it; locally use a throwaway `uv venv` + `uv pip install awscli`):
  ```bash
  infisical run --path=/job-data -- bash -c 'AWS_ACCESS_KEY_ID="$JOB_DATA_R2_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$JOB_DATA_R2_SECRET_ACCESS_KEY" DATA_LAKE_S3_ENDPOINT="$JOB_DATA_S3_ENDPOINT" AWS_ENDPOINT_URL="$JOB_DATA_S3_ENDPOINT" AWS_DEFAULT_REGION=auto DATA_LAKE_CLOUD_WRITE_ENABLED=true .venv/bin/python infra/scripts/data_lake/silver.py --data-lake-uri "$JOB_DATA_LAKE_URI" --max-invalid-ratio 0.02 --json'
  ```
- [x] Evidence (run id `silver-c822249f3a893e42114cc727`):
  - Bronze 17,428 rows → 13,136 duplicates → 2 fixture rows excluded → 7 invalid (0.16 %) → **4,283 valid**, `quality.status=passed_with_rejects`.
  - `silver/domain=jobs/dataset=job_postings/schema_version=1/event_date=mixed/part-silver-c822249f3a893e42114cc727.parquet`
  - `silver/domain=jobs/dataset=job_postings_rejects/schema_version=1/event_date=mixed/part-silver-c822249f3a893e42114cc727.parquet`
  - `control/manifests/silver/source=book-job-data/dataset=job_postings/transform_id=silver-c822249f3a893e42114cc727.json`
  - Rerun: same run id, `silver_existed`/`rejects_existed`/`manifest_existed` all `true` (no new objects).
  - Read back from R2: 8 boards, `count(*) = count(distinct record_id)` per board; all 7 rejects are `HN_WhoIsHiring` (posts without a URL).

Findings that shape the next tasks:

1. **Each run is a full snapshot.** When Bronze changes, the next run writes a new complete part under `event_date=mixed`. `read_silver_rows()` globs every part, so after the second scheduled run it would double-count. Task 3 must read only the part named by the **latest Silver manifest** (or Task 4 must prune older parts — prefer reading latest; parts are immutable evidence).
2. HN_WhoIsHiring rows have no URL by nature; fix in `book-job-scraping` (fall back to the HN item URL) rather than loosening Silver.

### Task 3: dbt marts → Gold ✅ (2026-11-02)

Design: one runner, `infra/scripts/data_lake/jobs_marts.py`, does Silver → dbt → Gold in one bounded run. It builds (or re-finds) the Silver snapshot of the current Bronze and hands **that exact part** to dbt (`--vars jobs_silver_part`), which solves Task 2 finding 1 by construction. dbt runs in a scratch DuckDB file (`DBT_DUCKDB_PATH`) that is thrown away afterwards. Marts are exported as deterministic Parquet (pyarrow, ordered rows) and published write-once with a Gold manifest. No DuckDB S3 config needed: the Silver part is copied through `ObjectStore`.

- [x] `stg_job_postings` (valid rows only, one row per `record_id`; glob default only for local dev).
- [x] `jobs_postings_daily` — posted date × board: postings, companies, with_salary.
- [x] `jobs_board_health` — board: total, companies, last posted/seen, postings in last 7 days.
- [x] `jobs_skills_demand` — ISO week × tag (lower-cased): postings, rank, top 50 per week.
- [x] `jobs_company_hiring` — company (case-insensitive): total, last 30 days, boards, first/last posted.
- [x] "Recent" windows are anchored on the newest capture in the snapshot, not the wall clock, so a rebuild of the same Silver run gives identical rows.
- [x] dbt tests: `unique`/`not_null` on keys, local `unique_combination_of_columns` generic test (no package deps), `tests/jobs_marts_quality.sql` consistency checks. `dbt_lake_test.py` builds a jobs Silver fixture so the full project (100 nodes) still passes.
- [x] Freshness: runner reports age of the newest capture; `status=stale` and exit 3 above `--stale-hours` (default 24), Gold still written.
- [x] Gold layout: `gold/domain=jobs/dataset=<mart>/schema_version=1/part-<gold_run_id>.parquet`; manifest `control/manifests/gold/source=book-job-data/domain=jobs/gold_run_id=<id>.json`. `gold_run_id` = hash(Silver run, jobs model files, duckdb + pyarrow versions).
- [x] Tests `infra/tests/data_lake/jobs_marts_test.py` (5): mart values, idempotent rerun, new Bronze → new Gold run, stale reporting, quality gate writes no Gold. Suite 52/52 files, mypy 57 files, ruff clean.
- [x] Real run on R2: Track B landed a new batch at 16:18 UTC, so Silver made a new snapshot `silver-7f9d98ed92c6d18910bf61f2` (4,410 rows, 9 boards); Gold `gold-b903ed14085599696ebb7f8b`: board_health 9, postings_daily 207, skills_demand 1,402, company_hiring 2,346 rows; dbt 5 models + 12 tests pass; age 0.26 h. Rerun: every object `existed`.
- [x] Review fixes (Codex): capture times normalised to UTC (`timestamptz … at time zone 'UTC'` + profile `TimeZone: UTC`), missing companies are `NULL` keys (not one extra "company"), a posting counts once per tag. Gold rebuilt as `gold-9c9ac59b1a26ba75726e4d72` from the same Silver run; `gold-b903…` stays as superseded evidence, so readers must take the newest Gold manifest.

Findings:

1. New board **PeoplePerHour** (16 jobs): `posted_at` does not parse as a date, so it is missing from `jobs_postings_daily` / `jobs_skills_demand`. Fix the date at the source (`book-job-scraping`) or add a parser in `stg_job_postings`.
2. Board health already shows value: RemoteOK last posted 2026-08-18 → its scraper is likely broken.
3. BI / Task 5 needs the newest Gold run: read the newest Gold manifest, or take `marts.*.key` from the runner output in the same workflow.

### Task 4: Schedule it

**Files:** `.github/workflows/jobs-silver-gold.yml`

- [ ] Trigger: `workflow_run` is cross-repo-unsafe, so use `schedule` at :30 after each Track B slot (`30 */6 * * *`) + `workflow_dispatch`.
- [ ] Steps: Silver → `dbt build --target r2 --select +marts.jobs` → Gold export → heartbeat.
- [ ] Secrets: R2 key (repo secret from Infisical `/job-data`), `HEARTBEAT_TOKEN`.
- [ ] Concurrency group so two runs never overlap.

### Task 5: BI (per D2; Evidence.dev assumed)

**Files:** new public repo `jobs-insights` (or `infra/bi/` here)

- [ ] Pages: Overview (postings/day by board), Boards health, Skills demand, Companies.
- [ ] Build reads Gold Parquet only; rebuild after Task 4 succeeds (deploy hook).
- [ ] Cloudflare Access in front; ADR-0007 design floor (WCAG 2.2); screenshots at 375/768/1440 before and after deploy.

### Task 6: Clean up and register

- [ ] D3 action on `remote_job_feeds`.
- [ ] Find and stop the `offline_fixture` leak in Track B.
- [ ] Add Silver/Gold datasets + BI consumer to `catalog_assets` / `lineage_edges` (architecture-docs) so `meta_source_lineage` shows the full chain.
- [ ] Update README "Metadata ใน lake" and the data-sources registry.

## Done when

- Silver and Gold prefixes for jobs exist in R2 and refresh every 6 h without manual steps.
- 4 marts pass dbt tests on R2 data; freshness alerts route to Telegram via the uptime Worker.
- The BI site shows today's numbers and reads nothing but Gold.
- Lineage view shows sources → Bronze → Silver → Gold → BI for jobs.

## Estimate

Tasks 1–4 ≈ 1–1.5 days AI-paced; Task 5 ≈ 0.5–1 day; Task 6 ≈ 0.5 day. Running cost: $0 (GH-hosted runners, R2 free tier, ~3 MB Bronze/quarter, Cloudflare Pages + Access free).
