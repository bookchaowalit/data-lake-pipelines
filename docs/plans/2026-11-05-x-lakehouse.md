# X-intelligence lakehouse: Sheet → R2 raw + Supabase curated + small Sheet view

Status: **Day 0 done 2026-11-05.** Backfill executed and validated (see §0 "Day 0"). Parallel run
(days 1–7) is next; the bots still write the Sheet, which was only read. Code: `infra/scripts/x_lakehouse/`. Tests: `infra/tests/x_lakehouse/`.

## 0. Decisions (2026-11-05)

1. **Timezone: ICT confirmed.** Checked against the time encoded in each X post id (Snowflake):
   reading `posted_at` as Asia/Bangkok puts all 27,950 X Feed rows and all 2,048 `ref_posted_at`
   values within 2 minutes of the id time (the Sheet drops seconds); none fit UTC.
2. **Keep raw `x_signals` and `claims`.** The curated tables stay rebuildable from R2, and every
   claim version is kept.
3. **Intake overlap:** this `raw/` layer is the X Feed store from now on. When Grok Bot pushes to
   intake-api, `x_feed_lake_export` stays as the intake cell's export, and one writer for analysis
   is chosen then. Nothing changes in the intake path now.
4. **Writer bridge:** the bots keep writing the Sheet; a scheduled catch-up `backfill --execute`
   (idempotent) runs 3×/day as a Railway cron in `data-schedules`, next to `sheet-summaries`, which
   already has the Sheet credentials. A FastAPI wrapper around `append_rows` is built only when a bot
   is ready to call it.
5. **Targets:** R2 bucket `x-intel-lake` with a token scoped to that bucket; a new free Supabase
   project `x-intel` (ap-southeast-1; all 5 existing projects are paused, so it is the only active
   one); variables in Infisical `/x-lakehouse`.

**Day 0 (2026-11-05, owner said go):** R2 bucket `x-intel-lake` (APAC) + account token
`x-intel-lake-rw` (Object Read/Write on that bucket only); Supabase project `x-intel`
(ref `qdfnqfrlhlargihrpqgc`, org personal, ap-southeast-1) with role `x_lake` that owns only schema
`x_intel` (no CREATE on the database); `X_LAKE_URI`, `R2_*`, `X_LAKE_DB_URL`, `SUPABASE_X_INTEL_REF`
in Infisical `/x-lakehouse`. The project's `postgres` password was never stored (reset it in the
dashboard if needed; the pipeline does not use it). `backfill --execute` wrote 49,417 raw rows
(81 parts) and inserted 4,301 curated rows. `validate` was ok on all 12 datasets, and a second
`--execute` wrote 0 rows and updated 0.
Run: `infisical run --path /sheet-summaries -- infisical run --path /x-lakehouse -- python -m x_lakehouse.<cmd>`
from `infra/scripts`.

**Parallel run started (2026-11-05 evening):** Railway project `data-schedules`, cron service
`x-lakehouse`, `20 1,9,15 * * *` UTC (08:20 / 16:20 / 22:20 ICT), restart NEVER, CLI-uploaded
(`railway up` of a `git archive` of main, `RAILWAY_DOCKERFILE_PATH=deploy/x-lakehouse/Dockerfile`,
so a push to main does not redeploy it). Each run is `python -m x_lakehouse.scheduled`: catch-up
`backfill --execute`, then `validate --since <yesterday ICT>` on all 12 datasets, then the serving
view as a dry run (counts only). Logs carry counts only. Uptime heartbeat `x-lakehouse` (11 h,
platform-uptime-worker f973686): `ok:false` alerts at once on Telegram, a missed run after 11 h.
Railway variables: `GOOGLE_SERVICE_ACCOUNT_JSON`, `HEARTBEAT_TOKEN`, `R2_ACCOUNT_ID`,
`R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `X_LAKE_URI`, `X_LAKE_DB_URL` (Supabase pooler, IPv4),
`SENTRY_ENVIRONMENT` (no `SENTRY_DSN` yet, so Sentry is off). First runs: local 12:40 UTC wrote 505
X Feed / 317 context / 20 trends TH / 174 signals rows, validate ok; Railway one-shot 12:50 UTC wrote
0 (already caught up), validate ok, 130 s. Day 1 of the 7-day clock is 2026-11-06.

**Cutover design (2026-11-08, owner: "ปรับไปแค่ Pipeline ใหม่เลย"):**
6. **Bots write SQL, the cron stays the only R2 writer.** The bots cannot run this package and hold
   no R2/DB credentials, but they can run SQL through the Supabase connector (role `postgres`). So the
   writer is `x_intel.ingest(dataset, rows jsonb, run_id)` (SECURITY DEFINER, owned by `x_lake`,
   EXECUTE for `postgres` only) into `x_intel.ingest_inbox`, plus an immediate upsert of the curated
   tables. The Railway cron drains the inbox through `append_rows` (`X_LAKE_SOURCE=inbox|both`).
   Rejected: an always-on FastAPI service (new fixed cost), R2/DB secrets on the shared bot box
   (credential rule), Postgres-only writes (no raw history). Code: `inbox.py`, `bot_sql.py`.
7. **Two more datasets:** `x_analyses` ("X Analyses", A:N) and `x_triage_audit` ("X Triage Audit",
   A:J), raw + curated, so no X-pipeline tab is left without a home. Headers read 2026-11-08
   (`infra/tests/fixtures/x_lakehouse_sheet_headers_2026-11-08.json`); a read-only dry run that day:
   60 + 1 rows, 0 coercion errors, 0 duplicate keys. `x_analyses` repeats `post_id` by design, so it
   is left out of the duplicate-`post_id` checks.
8. **Claims Book checked are protected:** a stored claim with `checked_by = 'book'` is replaced only
   by a row that also says `book` (`Curated.protect`, in both the Python upsert and `ingest()`).
   Once Book checked a claim (stored, or any row of the batch), only Book's rows are candidates
   when a batch's newest row is picked, so a later bot row cannot hide Book's update. `validate`
   compares Postgres with the newest *eligible* version and reports skipped bot versions as
   `postgres_protected_held`. On 2026-11-08
   all 20 claims had `checked_by = 'rule'`, so the parallel run is unaffected.

**Bot-only datasets (raw only), private inbox (2026-11-10, raw-only since the owner's decision of
2026-11-12; spec `docs/specs/x-lake-private-datasets.md`; not applied or deployed yet):**
9. **Seven datasets without a Sheet tab**, sent through `ingest()` only: the owner's own account
   data `x_own_account_daily`, `x_own_mentions`, `x_bookmarks`, `x_home_timeline` (**private**:
   door `x_intel_private.ingest()`, inbox in schema `x_intel_private`, raw under
   `<prefix>/private/`, never served; `x_intel.ingest()` refuses them) and `x_spaces`,
   `x_communities`, `x_list_sources` (public inbox, normal raw path, not served). All seven are raw
   only: no curated table. Phone numbers in their text become `[phone]`. Four more, raw only too
   (2026-11-14): `x_post_engagers`, `x_news` (public) and `x_own_likes`, `x_bookmark_folders`
   (private). Limit: bots on the
   `postgres` role may still bypass RLS on the private inbox (spec §2).
10. **Section 8: dropped (owner 2026-11-12, raw only).** The prompt's small curated tables
    (`x_trends_th`, `x_trends_intl`, `x_topic_counts`, `x_account_snapshots`, `alpha_feed`) and the
    two optional serve tabs are not built: these datasets stay in raw on R2 like `x_feed` /
    `x_post_context`; tables over raw are built later by whoever needs them.
    `x_intel.drain_log` records each drain run (counts only).
11. **Digest:** `digest_log.ai_status` / `fallback_reason` / `telegram_message_ids`; `error` is
    delivery only; an alert after 3 `provider_auth` fallbacks in a row.

## 1. Context

The X pipeline's bots append to the "Chief Findings Warehouse" Google Sheet
(`1zajA2iEfa9TS5JNjV1Y7LOurrTmiqrP-8ygn9clvBdI`) 2–3 times a day. On 2026-11-05 the Sheet held
31 tabs and 6.6 MB; X Feed alone had 27,950 rows after 11 days. The goal is to move storage to
a lakehouse layout and keep the Sheet only as a small serving view, with no new fixed cost.

The headers below were **read from the live Sheet** on 2026-11-05 (read-only scope) and every
column was profiled (Python type of every cell, blank rate, duplicate keys). The header snapshot is
the test fixture `infra/tests/fixtures/x_lakehouse_sheet_headers_2026-11-05.json`; a contract test
fails if `schemas.py` drifts from it.

Findings that shaped the schema:

- Every X id (`post_id`, `ref_post_id`, `first_post_id`, `user_id`, `last_since_id`) is already
  stored as **text** in the Sheet. No digits were lost. The code refuses a numeric id ≥ 2^53 rather
  than storing it (it would already be wrong).
- Timestamps are `YYYY-MM-DD HH:MM` wall time with no offset; the column names (`*_ict`) say ICT.
  They are stored as UTC `timestamp[us, tz=UTC]`, converted from Asia/Bangkok.
- `post_id` is unique in X Feed (27,950) and X Signals (2,930), and every X Signals post is in X Feed.
- Columns that mix numbers and words stay `string`: Alpha Feed `metric_1`/`metric_2`
  (`Resolved`, `fed_due:2026-11-01`), X Query Plan `result_cap`/`max_pages` (`n/a`, `25/seed`),
  X Post Context `video_duration_s`/`video_views` (`8.0; 8.0` lists for multi-video posts).
- X Post Context: 85 % of rows are media-only posts with no `ref_type`/`ref_post_id`; only `post_id`
  is required.

Out of scope (never read or written): `AI Summaries`, `AI Summaries Latest`, every `Chief *` tab,
`README`, `All Findings`, `Datasets`. (`X Analyses` and `X Triage Audit` joined on 2026-11-08, §0.7.)

## 2. Layout and naming

```
r2://<bucket>/raw/<dataset>/date=YYYY-MM-DD/part-NNNN.parquet   immutable, zstd level 6, explicit Arrow schema
r2://<bucket>/_manifests/<dataset>/<batch_id>/date=D-part-N.json  one entry per part, written before the part
postgres  schema x_intel                                        curated tables
sheet     "Serve · X Signals 7d", "Serve · Open Claims"         the only tabs this code may write
```

- Dataset names are snake_case of the tab name (`X Trends TH` → `x_trends_th`). Columns keep the
  Sheet header names exactly, so a bot's row dict needs no mapping.
- `date=` is the **ingest date** in ICT: the row's own capture timestamp (`collected_at`,
  `snapshot_at`, `pulled_at_ict`, `last_checked`). X Post Context has no timestamp, so it uses the
  post's X Feed `collected_at` (falling back to the run date).
- Parts are numbered per partition; a new batch writes the next number. X Post Context has no
  timestamp of its own (its date comes from a feed lookup that can change between runs), so its keys
  are always checked against every partition before writing; so is any row with a blank timestamp. No file is ever overwritten
  (local: `open(..., "xb")`; R2: existence check before write — one writer at a time).
- `batch_id` = `backfill-<UTC ts>-<rand>` or `append-<UTC ts>-<rand>`.
- Contract name = `<dataset>.v1` (Parquet schema metadata). A breaking change means `v2` and a new
  column set, never an in-place rewrite.

Every raw row also carries lineage columns: `_row_key` (sha256 of the natural key's raw text),
`_batch_id`, `_source` (`sheet-backfill` | `append`), `_sheet_row` (row number at backfill time),
`_ingested_at` (UTC), `_coerce_errors` (JSON `{column: {raw, reason}}`, null when clean),
`_extra` (JSON of Sheet columns the contract does not know yet — kept, never dropped).

## 3. Raw datasets (Parquet on R2)

Types: `str`=string, `int`=int64, `float`=float64, `bool`, `ts`=timestamp(us, UTC), `date`=date32.
Rows = Sheet rows on 2026-11-05.

| dataset | tab | rows | natural key (`_row_key`) | partition from |
|---|---|---:|---|---|
| x_feed | X Feed | 27,950 | post_id, requesting_chief, query_id, watch_type, watch_value, collected_at | collected_at |
| x_post_context | X Post Context | 14,662 | post_id, ref_type, ref_post_id | X Feed collected_at of post_id |
| x_trends_th | X Trends TH | 355 | snapshot_at, rank, trend_name | snapshot_at |
| x_trends_intl | X Trends Intl | 795 | snapshot_at, woeid, rank, trend_name | snapshot_at |
| x_topic_counts | X Topic Counts | 102 | snapshot_at, query_id | snapshot_at |
| x_account_snapshots | X Account Snapshots | 953 | snapshot_at, list, handle | snapshot_at |
| alpha_feed | Alpha Feed | 479 | pulled_at_ict, source, sub_source, url, title | pulled_at_ict |
| x_signals ★ | X Signals | 2,930 | post_id, collected_at | collected_at |
| claims ★ | Claims | 18 | claim_id, last_checked | last_checked |

★ Not in the requested list: added so the curated tables can always be rebuilt from R2, and so
claims keep every observed version (Postgres keeps only the latest). Drop them if you disagree.

Columns (Sheet order):

- **x_feed**: collected_at ts, requesting_chief str, watch_type str, watch_value str, post_id str,
  post_url str, author_handle str, posted_at ts, text str, likes int, reposts int, replies int,
  views int, tag str, query_id str, score float, top bool
- **x_post_context**: post_id str, ref_type str, ref_post_id str, ref_post_url str,
  ref_author_handle str, ref_author_type str, ref_text str, ref_likes int, ref_posted_at ts,
  media_count int, media_types str, media_urls str, media_alt_text str, video_duration_s str,
  video_views str
- **x_trends_th**: snapshot_date date, snapshot_at ts, rank int, trend_name str, trend_type str,
  post_count int, search_url str, picked_for_posts bool, note str
- **x_trends_intl**: snapshot_date date, snapshot_at ts, woeid int, country str, rank int,
  trend_name str, trend_type str, post_count int, search_url str, note str
- **x_topic_counts**: snapshot_date date, snapshot_at ts, query_id str, topic str, buckets int,
  total_24h int, max_hour_24h int, max_hour_start_ict ts, median_hour_prior6d float, ratio float,
  spike bool, drilldown_query_id str, note str
- **x_account_snapshots**: snapshot_date date, snapshot_at ts, list str, watchlist_row int,
  handle str, user_id str, followers_count int, following_count int, tweet_count int,
  listed_count int, like_count int, media_count int, verified bool, protected bool,
  account_status str, followers_delta_vs_prev int, note str
- **alpha_feed**: pulled_at_ict ts, source str, sub_source str, date date, title str,
  description str, metric_1 str, metric_2 str, extra str, url str, time_bound_date date, tags str,
  requesting_chief str, notes str
- **x_signals**: post_id str, collected_at ts, query_id str, category str, subcategory str,
  signal_type str, author_class str, expanded_link str, novelty str, opportunity_score int,
  verification_status str, suggested_action str, life_cat str, domain str, topic str, tags str,
  tax_version str, cred_tier str, manip_flags str, content_label str, route str, alert_tier str,
  triage_version str
- **claims**: claim_id str, claim str, first_seen_link str, spread_count int,
  verification_status str, verification_source str, first_post_id str, first_seen ts, topic str,
  tags str, supporting_links str, contradicting_links str, last_checked ts, priority float,
  queue_state str, checked_by str, next_check ts

## 4. Curated tables (Supabase Postgres, schema `x_intel`)

DDL: `python -m x_lakehouse.schemas` prints it; it is idempotent (`create … if not exists`) and runs
at the start of every `--execute`. Each table also has `created_at`, `updated_at` (timestamptz) and
`_batch_id`. Column types follow the raw mapping (`str`→text, `int`→bigint, `float`→double
precision, `ts`→timestamptz, `date`→date).

| table | primary key | from | extra | indexes |
|---|---|---|---|---|
| x_signals | post_id (unique) | X Signals, latest row per post_id | joined from X Feed by post_id: author_handle, post_url, posted_at, post_text, likes, reposts, replies, views, feed_score, feed_tag | collected_at; created_at; (domain, collected_at); cred_tier; (alert_tier, collected_at); route; author_handle |
| claims | claim_id | Claims, latest row per claim_id | — | first_post_id; first_seen; created_at; (queue_state, next_check); topic |
| x_watchlist | (requesting_chief, type, value) | X Watchlist | — | (type, value); tag |
| x_query_plan | query_id | X Query Plan | — | status; last_run_at |
| taxonomy_map | (section, v3_topic_or_type) | Taxonomy Map | — | — |

"tier" in the brief maps to two columns, `cred_tier` (T1–T5) and `alert_tier` (none / digest /
urgent_candidate); both are indexed.

**Upsert rule** (`curated.py`): COPY the batch into a temp table, then
`insert … on conflict (pk) do update set … where <some column is distinct>`. Reruns change nothing
and `updated_at` moves only on a real change (proved: a second full backfill updated 0 rows).
`claims` (by `last_checked`) and `x_signals` (by `collected_at`) also refuse an update from an older
version, so replaying an old batch cannot roll a row back. The joined X Feed columns use
`coalesce(excluded.c, t.c)`, so a later batch that lacks the feed row keeps the stored author and
text. A signal or post-context row appended in a later call than its post looks the post up in the
last 7 days of raw `x_feed` partitions (context uses the post's capture date as its partition).

## 5. Serving layer (Sheet)

`serve_sheet.py` reads Postgres and writes two **new** tabs it owns:
"Serve · X Signals 7d" (`collected_at` within 7 days, 23 columns including author and text) and
"Serve · Open Claims" (`queue_state` not in closed/done/resolved/archived/dismissed).
It creates a tab with its header once; each refresh clears and rewrites rows 2+ only. A changed
column list is skipped with an error and exit 1 (never a header rewrite), so a scheduler sees it. `assert_writable()` refuses every
other tab, including all existing data tabs, `AI Summaries*` and `Chief *`. Dry run by default.
The existing X tabs are left as they are; trimming or archiving them is a separate owner decision.

## 6. Query layer (DuckDB on R2)

`python -m x_lakehouse.run_queries` creates an in-memory DuckDB R2 secret
(`type r2, key_id, secret, account_id`) from env vars and runs `queries/*.sql` (`{lake}` is
replaced by `r2://<bucket>/<prefix>`):

- `01_daily_volume_per_domain` — signals and alerts per ICT day per domain, 30 days
- `02_top_accounts_by_signals` — authors by signal count (X Signals ⨝ X Feed), with alert count,
  main domain, best cred tier, views
- `03_topic_count_spikes` — the bot's own `spike` flag plus a z-score of `total_24h` against the
  topic's previous ≤ 14 snapshots (flag at z ≥ 2 with ≥ 3 prior snapshots)
- `04_rows_per_partition` — ops view: rows, parts, coercion-error rows per dataset per date
- `05_account_follower_growth` — follower gain per watched account over 14 days

All five were run against the dry-run Parquet output (real data); results are in the review notes.
`alpha_feed` has its own `date` column, which clashes with the `date=` partition key. Today the two
are equal on all 479 rows. To be safe, read alpha_feed with `hive_partitioning = false`.

## 7. Retention

| layer | keep | how |
|---|---|---|
| R2 raw | indefinitely (review at 2 years) | append-only; no lifecycle rule needed at this volume |
| Postgres x_signals | 180 days of rows (post text is X content; matches intake's 180-day text rule) | follow-up job: `delete from x_intel.x_signals where collected_at < now() - interval '180 days'` — not built yet |
| Postgres claims, config tables | all (tiny) | — |
| R2 private raw (bot-only private datasets) | per dataset (spec `x-lake-private-datasets.md` §2) | R2 lifecycle rules on `private/raw/<dataset>/` — follow-up after merge |
| Sheet serving tabs | 7 days / open claims only | rebuilt by serve_sheet.py |
| Sheet X source tabs | untouched by this work | owner decides after cutover |

## 8. Cost (free tiers only)

Measured on the 2026-11-05 dry run: all raw data = 8.0 MB for 11 days (x_feed 5.7 MB), so about
0.75 MB/day, about 275 MB/year. Curated x_signals = 3.1 MB for 2,930 rows (about 1.1 KB/row with
7 indexes), at about 900 signals/day.

| service | free limit | expected use | headroom |
|---|---|---|---|
| R2 storage | 10 GB-month | ~0.3 GB after 1 year | ~30 years at today's rate |
| R2 Class A (write/list) | 1 M/month | ~3 runs/day × ~25 ops ≈ 2–3 k/month | large |
| R2 Class B (read) | 10 M/month | daily `validate --since` + ad-hoc DuckDB ≈ 10–50 k/month | large |
| R2 egress | free | — | — |
| Supabase DB | 500 MB | x_signals ~170 MB at 180-day retention; ~350 MB/year without it | needs the retention job within ~12 months |
| DuckDB | local | — | — |
| Sheets API | 300 reads/min/project | 1 batchGet per run | large |

Total new fixed cost: **$0**. Supabase free projects pause after 7 days without activity; the
daily upserts keep the project active.

## 9. Configuration (env vars only; no secret in code)

| variable | used by | notes |
|---|---|---|
| `GOOGLE_SERVICE_ACCOUNT_JSON` | backfill, validate, serve_sheet | the existing `sheet-summaries@` key in Infisical `/sheet-summaries` works (it has Editor on the Sheet; backfill asks only for the read-only scope) |
| `X_LAKE_SHEET_ID` | all | optional; defaults to the Chief Findings Warehouse id |
| `X_LAKE_URI` | backfill, append, validate, run_queries | `r2://<bucket>[/prefix]`; `file:///dir` for tests |
| `R2_ACCOUNT_ID` | R2 store, DuckDB | |
| `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | R2 store, DuckDB | R2 API token, Object Read & Write, **scoped to the one bucket** |
| `X_LAKE_DB_URL` | backfill, append, validate, serve_sheet | Supabase session-pooler URL with `sslmode=require`; a role that owns schema `x_intel` only |

| `X_LAKE_SOURCE` | scheduled | `sheet` (default: the parallel run as before) \| `both` (drain inbox + Sheet catch-up + compare) \| `inbox` (lake only) |
| `X_LAKE_COMPARE_SINCE` | scheduled (`both`) | optional ISO time of the first dual-write run; the inbox/Sheet comparison starts there |
| `X_LAKE_SERVE` | scheduled | `execute` refreshes the serving tabs after an ok run; anything else = dry run (counts only) |

Proposed home: Infisical folder `/x-lakehouse`, run with `infisical run --path /x-lakehouse`.

## 10. Validation

Built into every run and into `validate.py` (read-only, exit 1 on failure):

- per-dataset row counts: Sheet vs raw Parquet (rows, distinct `_row_key`) vs Postgres
- Sheet rows missing from raw (with sample row numbers)
- duplicate `post_id` in the Sheet, in raw and in Postgres; duplicate natural keys in the Sheet and raw
- null rate per column
- coercion errors: count per column + samples; the row is kept, the bad field is null, and the raw
  value and reason are kept in `_coerce_errors`
- schema mismatch: missing / unexpected / reordered Sheet columns. A missing column or tab makes
  `--execute` refuse **before** opening R2 or Postgres (exit 2); `--execute` also refuses without
  `X_LAKE_DB_URL`, so raw and curated are always written together
- curated vs raw: every key in the scoped raw rows (and Sheet rows) must exist in Postgres, so
  lake-only rows after cutover are covered too; for claims and x_signals the stored version
  (`last_checked` / `collected_at`) must also be the newest one seen, which catches an upsert that
  failed after its raw write

`validate` fails, listing the reasons, on: a missing tab or column, any coercion error, duplicate
natural keys or post_ids, Sheet rows missing from raw, and keys missing from Postgres.
Coercion errors count whether they are in the Sheet or only in raw (post-cutover appends).
`validate` refuses to run without `X_LAKE_DB_URL` unless `--raw-only` is passed (which prints that
Postgres was not checked). `validate --since <date>` checks only recent partitions (the daily parallel-run check); X Post
Context rows are scoped by the partition they were stored in.

## 11. Cutover plan

**Day 0 — approve and backfill**
1. Owner approves this doc and the dry run.
2. Create the R2 bucket (proposed `x-intel-lake`) and a bucket-scoped R2 token; create the Supabase
   project (or schema `x_intel` in an existing free project) and a role limited to it. Store the six
   variables in Infisical `/x-lakehouse`.
3. `backfill --check-targets` (read-only: confirms credentials, shows empty targets).
4. `backfill --execute`, then `validate` → must print `ok` for every dataset. Rerunning is safe.

**Days 1–7 — parallel run (old writer unchanged)**
- The bots keep appending to the Sheet exactly as today. The new path catches up 3×/day by
  re-running `backfill --execute`. It is idempotent, so it writes only rows it has not seen
  (an `append-…` caller can be added to a bot in parallel where that is easy).
- Every run validates (`validate --since <yesterday>` inside `x_lakehouse.scheduled`) and posts
  the result to heartbeat `x-lakehouse`; the monitor's `/status` and the Railway logs are the log
  (date, per-dataset sheet rows, ok). `run_queries 04_rows_per_partition` stays a manual spot check. Any `FAIL`, duplicate or unexplained coercion
  error resets the 7-day clock.
- `serve_sheet` runs in **dry run** only during this week.

**Parallel-run log (Railway `event=validate`, read 2026-11-08):** 11-06 08:23, 16:20, 22:23;
11-07 16:20, 22:21; 11-08 08:21, 16:22: all `ok=true`, `failed={}`, backfill exit 0. The
11-07 08:20 slot left no logs (its deployment was replaced at 02:29 UTC that morning); a missing
run is not a FAIL, but the owner decides whether day 2 counts.

**Day 8+ — switch writers through the bot inbox (each phase only on the owner's go)**

*Phase 1 — dual write (≈ 13 → 15 Nov, at least 3 runs).*
1. Deploy this code to the cron (CLI upload, as before). With `X_LAKE_SOURCE` unset nothing changes
   except the two new datasets (§0.7) being caught up from the Sheet.
2. Apply the inbox migration: `python -m x_lakehouse.inbox --apply` (or the first run in
   `both`/`inbox` mode applies it). Review first with `--print-ddl`.
3. Set `X_LAKE_SOURCE=both` and `X_LAKE_COMPARE_SINCE=<ISO time of the first dual-write bot run>`.
4. Chief Data keeps every Sheet append and also sends the same rows: `bot_sql.py <dataset> <run_id>
   rows.json out/` → one `execute_sql` call per file (≤ 500 rows, ≤ 900 KB), x_feed before
   x_signals (the immediate x_signals upsert takes post facts from the inbox's X Feed rows). Claim
   updates go as full claim rows. A failed lake write never blocks the Sheet write or the run.
5. Each cron run logs `event=compare`: `missing_in_inbox` (Sheet rows dated in the window, older than
   60 min, that never reached the inbox; `x_analyses`, which has no timestamp, by the day the row
   first landed in raw) and `missing_in_sheet` (inbox rows received before the cron read the Sheet
   that the Sheet lacks; claims by `claim_id`). A drained row with a coercion error fails the run
   even when it is dated before the validate window. Exit: 3+ runs with no mismatch and 0 coercion errors.

*Phase 2 — lake only (≈ from 16 Nov).*
Prepared 2026-11-10: runbook `2026-11-10-x-sheet-cutover-runbook.md` (watchlist row map, run record, Chief Data brief).
1. Chief Data stops the X tab appends and the X Query Plan G:P run-state write; config reads come from
   `x_intel.v_query_plan_sheet`, `x_intel.v_watchlist_sheet` (Sheet column order, Sheet cell format)
   and `x_intel.v_claims_open`; config edits go through `ingest('x_query_plan' | 'x_watchlist', …)`.
2. `X_LAKE_SOURCE=inbox`: drain, validate raw vs Postgres, and check every drained row reached R2
   (`drained_missing_from_raw`). Config rows (query plan, watchlist, taxonomy) are applied by
   `ingest()` only; the drain types them but never re-upserts a snapshot (that could roll back an
   edit made during the drain). A config row with a coercion error stays in the inbox and fails
   every run (`config_coerce_errors`) until a corrected row replaces it. After an ok run, inbox rows drained more than 3 days ago are deleted.
3. `X_LAKE_SERVE=execute`: "Serve · X Signals 7d", "Serve · Open Claims", "Serve · X Query Plan".
4. The old X tabs stay in the Sheet, frozen. Nothing is deleted.

*Phase 3 — cleanup (≥ 30 days later, owner approval):* remove the `both` path, decide on the frozen
tabs (copy first), build the 180-day x_signals retention job before ~month 9.

**Rollback**
- *Before the switch:* nothing to undo for the Sheet, which was never changed. To reset the new
  path, drop schema `x_intel` and/or empty the bucket, then run the backfill again.
- *A bad batch in raw:* `_manifests/<dataset>/<batch_id>/` lists the exact part keys (each entry is
  written before its part, so a crashed batch is fully listed; every row also carries `_batch_id`; a part number named by any manifest entry is never reused, even when that part was never published, so a crashed batch's entry can never point at a later batch's part).
  Delete only those parts (owner approval), then rerun the backfill or append. The two Day 0
  batches predate this layout and have one `<batch_id>.json` per dataset; both completed.
- *Curated corrupted:* `drop schema x_intel cascade`, then `backfill --execute` rebuilds it
  (the Sheet is the source until the switch; a rebuild-from-R2 script is a follow-up).
- *Phase 1 (dual write):* stop the inbox step on the bot side and set `X_LAKE_SOURCE=sheet`; the
  Sheet is still complete. Inbox rows can stay (they are idempotent against the Sheet rows: same
  `_row_key`) or be truncated with owner approval.
- *After the switch, back to the Sheet:* point the daily runs back at the Sheet append (keep the old
  code path behind a flag, e.g. `X_LAKE_WRITER=sheet|lake|both`, during the first month). Rows
  written only to the lake in the meantime must be copied back: export them from raw with DuckDB
  (`copy (…) to 'rows.csv'`) and append them to the tab. That needs owner approval because it
  writes to a source tab.

## 12. Risks and open decisions

1. **Timezone of `posted_at` / `ref_posted_at`.** Resolved: ICT, verified against post-id time (§0).
2. **Overlap with the intake path.** `x_feed_lake_export.py` already lands intake x-feed pulls into
   Bronze `domain=social/dataset=x_feed` (envelope format). Once Grok Bot pushes to intake-api,
   there will be two lake copies of X Feed. Recommendation: this `raw/` layer is the typed history
   of everything the Sheet era wrote. When intake pushes are live, decide on one writer for x_feed.
3. **Bots may not run Python.** The `append_rows` function is ready, but the switch may need an HTTP
   wrapper. Until then the idempotent catch-up backfill is the bridge.
4. **Single writer.** R2 has no compare-and-swap through pyarrow. Two writers on the same partition
   at the same moment could both pick the same part number; the second write is refused (local) or
   detected by `validate` (R2). Schedule backfill and append so they never overlap.
5. **Supabase 500 MB.** About 12 months without the 180-day retention job; build that job
   before month 9.
6. **Lookups grow with history.** Undated context rows scan every `x_post_context` part's
   `_row_key` column, and late signals read 7 days of `x_feed`. Both are a few MB today; revisit with
   a key index file if appends get slow.
7. **Small files.** About 25 parts/day, about 9 k/year. That is fine for DuckDB. If it grows, a
   compaction into a new `v1c/` prefix (never rewriting `raw/`) is the path.
8. **Sheet edits after the fact.** Raw is append-only by natural key. If someone edits a non-key cell
   of an old Sheet row, the raw layer keeps the first version (curated takes the edit). Claims are
   keyed on `last_checked` precisely so every new version is kept.
9. **Untrusted text.** `text`, `ref_text` and `claim` are X content. Keep them as data; never render
   them unescaped in a web view.
    Inbox: a raw key resent with different values while the first version is still in the inbox
    (3 days after its drain) is refused with "send a new version"; after that it behaves like a
    Sheet edit. A retried config file (same run, same payload) is skipped, so it cannot roll back a
    later run's query-plan cursor (`x_intel.ingest_deliveries`, kept 30 days).
10. **Sheet row numbers in config.** `x_query_plan.watchlist_rows` and
    `x_account_snapshots.watchlist_row` point at X Watchlist *Sheet row numbers*. Postgres has no
    row number, so once the watchlist is edited in Postgres (Phase 2) those references need a stable
    id (e.g. `requesting_chief|type|value`) in the bots' planner. Decide before Phase 2.
11. **Inbox row keys.** `ingest()` computes `_row_key` in SQL (`x_intel.row_key`), which must equal
    `coerce.row_key` on what the drain reads back from jsonb. Tested on 50 generated rows per
    dataset (Python whitespace set, Thai, ids ≥ 2^53, whole floats). Not supported: non-whole numbers
    ≥ 10^15 in key columns (no key column holds one).

## 13. Dry run (2026-11-05, read-only against the Sheet)

```
dataset                  sheet  raw new raw skip parts  curated coerce err dup key dup post blank key
x_feed                   27950    27950        0    11        -          0       0        0         0
x_post_context           14662    14662        0    11        -          0       0        -         0
x_trends_th                355      355        0    10        -          0       0        -         0
x_trends_intl              795      795        0    10        -          0       0        -         0
x_topic_counts             102      102        0    10        -          0       0        -         0
x_account_snapshots        953      953        0     8        -          0       0        -         0
alpha_feed                 479      479        0     5        -          0       0        -         0
x_signals                 2930     2930        0     4     2930          0       0        0         0
claims                      18       18        0     3       18          0       0        -         0
x_watchlist               1011        -        -     -     1011          0       0        -         0
x_query_plan               209        -        -     -      209          0       0        -         0
taxonomy_map                84        -        -     -       84          0       0        -         0
```

All 12 headers match the contract. The first dry run found 56 `video_views` cells holding lists
(`1743; 206`) and 12,500 media-only context rows with blank ref keys; both are fixed in the
contract above. Rehearsal against **local** targets only (Postgres 17 in Docker + a local
directory): `--execute` twice. The second run wrote 0 raw rows (48,244 skipped) and updated 0 curated
rows. `validate` printed ok for all 12 datasets, and all 2,930 x_signals rows got their X Feed author.
