# X-intelligence lakehouse: Sheet → R2 raw + Supabase curated + small Sheet view

Status: **design + dry run, awaiting owner approval.** Nothing has been written to R2, Supabase or
the Sheet. Code: `infra/scripts/x_lakehouse/`. Tests: `infra/tests/x_lakehouse/`.

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
`README`, `All Findings`, `Datasets`, `X Analyses`, `X Triage Audit`.

## 2. Layout and naming

```
r2://<bucket>/raw/<dataset>/date=YYYY-MM-DD/part-NNNN.parquet   immutable, zstd level 6, explicit Arrow schema
r2://<bucket>/_manifests/<dataset>/<batch_id>.json              what one batch wrote (keys, rows, dates)
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
column list is skipped with an error, never a header rewrite. `assert_writable()` refuses every
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
`validate --since <date>` checks only recent partitions (the daily parallel-run check); X Post
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
- Each morning: `validate --since <yesterday>` and `run_queries 04_rows_per_partition`; log the
  result (date, per-dataset sheet/raw/pg counts, ok). Any `FAIL`, duplicate or unexplained coercion
  error resets the 7-day clock.
- `serve_sheet` runs in **dry run** only during this week.

**Day 8 — switch writers (only after 7 consecutive ok days)**
1. Each daily run calls `append_rows({...})` instead of appending to the Sheet. If a bot cannot run
   Python, wrap `append_rows` in a small FastAPI endpoint (follow-up, not built).
2. `serve_sheet --execute` runs after each append.
3. The old X tabs stay in the Sheet, frozen. Nothing is deleted.
4. `validate` now checks raw vs Postgres on recent partitions (the Sheet stops growing).

**Rollback**
- *Before the switch:* nothing to undo for the Sheet, which was never changed. To reset the new
  path, drop schema `x_intel` and/or empty the bucket, then run the backfill again.
- *A bad batch in raw:* its manifest lists the exact part keys. Delete only those parts (owner
  approval), then rerun the backfill or append.
- *Curated corrupted:* `drop schema x_intel cascade`, then `backfill --execute` rebuilds it
  (the Sheet is the source until the switch; a rebuild-from-R2 script is a follow-up).
- *After the switch, back to the Sheet:* point the daily runs back at the Sheet append (keep the old
  code path behind a flag, e.g. `X_LAKE_WRITER=sheet|lake|both`, during the first month). Rows
  written only to the lake in the meantime must be copied back: export them from raw with DuckDB
  (`copy (…) to 'rows.csv'`) and append them to the tab. That needs owner approval because it
  writes to a source tab.

## 12. Risks and open decisions

1. **Timezone of `posted_at` / `ref_posted_at`.** It's assumed to be ICT, like the capture columns.
   If the bot writes X's UTC time, those two columns are 7 h off. Confirm with the writer before
   `--execute` (it is a one-line change in `schemas.SHEET_TZ` per column).
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
