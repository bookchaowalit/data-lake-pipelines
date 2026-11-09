# Spec: news Silver → marts → Gold → BI

Status: proposed (2026-11-09). Standard: ADR-0004 (spec, then failing test, then code).
Code: `infra/scripts/data_lake/news_marts.py`, `silver.py` (`news_articles` normaliser),
`infra/dbt/models/staging/stg_news_articles.sql`, `infra/dbt/models/marts/news/`,
`infra/scripts/data_lake/news_bi_snapshot.py`, `news_bi_site.py`, `.github/workflows/news-marts.yml`.
Tests: `infra/tests/data_lake/news_marts_test.py`, `news_bi_test.py`, `infra/dbt/tests/news_marts_quality.sql`.
Platform rules: ADR-0013 (one R2 lake, DuckDB + dbt, no warehouse). Pattern: the jobs runner
(`docs/plans/2026-11-02-jobs-silver-mart-bi.md`, `docs/specs/jobs-gold-pointer.md`).

## Problem

`news-capture` (Railway `data-schedules`, every 2 h at :15 UTC) lands four approved RSS feeds
(Matichon, Bangkok Post Business, Blognone, Notebookspec) in Bronze
`domain=news/dataset=articles` of the `free-cloud-e2e-001` lake prefix. Nothing reads it: no
Silver, no marts, no consumer. Jobs is the only domain that reaches BI.

## Source contract

Bronze `source=book-news-scraping`, `domain=news`, dataset `articles`, product schema `news.v1`,
manifest-committed parts only. Every capture re-emits the items still in each feed, so one
article has many Bronze rows; `source_record_id` is `<publisher>|<article_id>`. Payload fields
used: `source` (publisher label), `article_id`, `headline`, `canonical_url`, `published_at`
(ISO 8601), `section`, `author`, `source_feed_url`, `image_url`, `captured_at`. `summary` stays in
`payload_json` only and is never published to Gold or BI (article text belongs to the publisher).

Day-0 profile (2026-11-09): 17,669 Bronze rows, 3,482 articles, 0 missing headline / URL /
`published_at`; `section` is empty for Matichon and Notebookspec.

## Behaviour

1. **Silver.** `SilverProductContract(source="book-news-scraping", domain="news",
   bronze_dataset="articles", silver_dataset="articles", bronze_product_schema_version="news.v1",
   normalizer="news_articles", privacy_class="public")`: newest Bronze row per article. Columns =
   common Silver columns + `publisher, article_id, headline, canonical_url, published_at, section,
   author, feed_url, image_url`. A row is invalid when `source_record_id`, `headline`,
   `canonical_url` or a parseable `published_at` is missing; invalid rows go to `articles_rejects`
   under the runner's `--max-invalid-ratio` (default 0.02), otherwise the run fails and writes no Gold.
   `canonical_url` keeps its query (it can identify the article, e.g. `?p=123`); only `utm_*`, click-id
   parameters and the fragment are dropped (transform `news-articles-silver.v2`).
   A valid `published_at` is stored as canonical UTC ISO 8601 (`...Z`; no offset = UTC), because
   `fromisoformat` accepts forms DuckDB cannot cast.
2. **Staging.** `stg_news_articles` reads exactly one Silver part (`news_silver_part` var, set by
   the runner; a bare `dbt build` without it fails). `published_at` and `seen_at` (= `event_time`,
   the capture time) are UTC instants; `published_date` is the date in `news_timezone`
   (`Asia/Bangkok`). Empty `section` becomes `unspecified`.
3. **As-of date.** Every mart uses `as_of_date` = the Bangkok date of the newest `seen_at` in the
   snapshot, never the wall clock, so a rebuild of one Silver run gives the same rows. Articles
   whose `published_date` is after `as_of_date` are left out of all marts.
4. **Marts** (Gold, one Parquet part each):
   - `news_articles_daily` — one row per (`published_date`, `publisher`, `section`): `articles`,
     `with_image`.
   - `news_feed_health` — one row per publisher: `articles_total`, `articles_7d` (published in the
     7 dates ending on `as_of_date`), `last_published_at`, `last_seen_at`, `as_of_date`.
   - `news_latest_articles` — the newest 30 articles per publisher by `published_at` (ties by
     `record_id`): `publisher, section, published_at, headline, canonical_url, rank`. No summary,
     author or image.
5. **Gold.** Like jobs: `gold_run_id` = hash(silver run, model files incl. `dbt_project.yml` (which holds
   `news_timezone`), duckdb/pyarrow versions);
   parts at `gold/domain=news/dataset=<mart>/schema_version=1/part-<gold_run_id>.parquet`, manifest
   at `control/manifests/gold/source=book-news-scraping/domain=news/gold_run_id=<id>.json`, and a
   publish pointer at `control/pointers/gold/source=book-news-scraping/domain=news/published_at=<ts>-<id>-<sha12>.json`.
   A rerun over unchanged Bronze writes nothing new (every put reports `existed`); unlike jobs, the
   pointer is reused when the newest one already names this Gold run, since no news reader uses
   pointer age as a freshness signal (the `news-marts` heartbeat covers liveness).
6. **Freshness.** The run is `stale` (exit 3, still published) when any of the four expected
   publishers is missing from `news_feed_health` or its `last_seen_at` is older than
   `--stale-hours` (default 6: three missed captures). The result lists the stale publishers.
7. **BI.** `news_bi_snapshot.py` loads one Gold manifest into DuckDB after checking every part's
   sha256; `news_bi_site.py` renders a static, no-JS page (feed health table, 14-day articles per
   publisher chart, latest headlines linking out with `rel="noopener noreferrer"`) under the same
   strict CSP `_headers` and ADR-0007 floor as the jobs site, deployed to Cloudflare Pages project
   `news-insights` only when the live deployment's mark differs from `<gold_run_id>@<commit>`.
8. **Schedule and alerting.** `news-marts.yml` (public repo: counts, run ids and keys only in the
   log) is started by the jobs queue (`platform-jobs-worker` job `news-marts`, `35 */2 * * *`,
   20 min after news-capture). Heartbeat `news-marts` in the uptime Worker, 5 h; `ok:false` with
   `failed` = `news-marts` (build failed), `stale`, or `news-bi` (deploy failed).

## Out of scope

Topic or keyword extraction (Thai needs a tokenizer; revisit with a written use case), article
text, sentiment, Telegram alerts, new feeds (each needs owner approval).
