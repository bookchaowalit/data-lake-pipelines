# X lakehouse: bot-only datasets (raw only), the private inbox, drain_log, digest_log

Status: code and tests in this PR; **nothing applied or deployed**. The owner applies the migration
and runs `railway up` (steps in §8). Source: Chief Data `schema_spec.md` (task m1110-src, §8 from
task lakemodefix) and the `digest_log` proposal, prepared 2026-11-10 ICT. Plan context:
`docs/plans/2026-11-05-x-lakehouse.md`.

**Owner decision 2026-11-12: raw only.** Every dataset the bots send is stored as raw data in the
lake (R2 `x-intel-lake`), one dataset each. This PR creates **no new Supabase table** for it: no
curated table for the seven bot-only datasets and none for the section-8 datasets (trends, topic
counts, account snapshots, Alpha Feed). Tables, marts or APIs over the raw data are built later by
whoever needs them (backend or AI coding), reading raw. An earlier revision of this PR had those
tables; they were removed before merge, so nothing has to be dropped anywhere.

Nothing in this PR is real owner data: every fixture is synthetic
(`infra/tests/fixtures/x_own_accounts_history_synthetic.csv`, rows built in
`infra/tests/x_lakehouse/x_lakehouse_private_test.py`).

## 1. Datasets

All of them have **no Sheet tab** and **no curated table**: bots send them through `ingest()` only, the
cron drains them into raw Parquet. The Sheet catch-up, the Sheet validation and the
`X_LAKE_SOURCE=both` inbox-vs-Sheet compare skip them (no `missing_in_sheet` failure); `validate`
checks them in raw only. `bot_sql.HEADERS` holds their column order (= the contract below).

| dataset | privacy | raw key (`_row_key`) | raw path | readers take |
|---|---|---|---|---|
| `x_own_account_daily` | private | `snapshot_date, handle` | `private/raw/` | one row per key |
| `x_own_mentions` | private | `post_id, own_handle` | `private/raw/` | one row per key |
| `x_bookmarks` | private | `post_id, folder_id, last_seen_at, removed_at` | `private/raw/` | the newest `last_seen_at` / `removed_at` per `(post_id, folder_id)` |
| `x_home_timeline` | private | `post_id, collected_at` | `private/raw/` | one row per key |
| `x_spaces` | public | `space_id, collected_at` | `raw/` | one row per key |
| `x_communities` | public | `community_id, snapshot_date` | `raw/` | one row per key |
| `x_list_sources` | public | `list_id, verified_at` | `raw/` | the newest `verified_at` per `list_id` |
| `x_post_engagers` | public | `post_id, engagement, user_id, collected_at` | `raw/` | the earliest `collected_at` per `(post_id, engagement, user_id)` |
| `x_news` | public | `story_id, collected_at` | `raw/` | the newest `collected_at` per `story_id` |
| `x_own_likes` | private | `post_id, own_handle, collected_at` | `private/raw/` | the earliest `collected_at` per `(post_id, own_handle)` (newest for the counts) |
| `x_bookmark_folders` | private | `folder_id, own_handle, collected_at` | `private/raw/` | the newest `collected_at` per `(folder_id, own_handle)` |

**Added 2026-11-14 (owner: "everything Chief Data pulls goes to the lake").** Chief Data started
pulling these on 2026-11-13/14 and kept them as files on its machine because the lake had no dataset
for them: `x_post_engagers` (who quoted, reposted or liked a top post), `x_news` (X News stories,
`search_news`), `x_own_likes` and `x_bookmark_folders` (private). There is no writer spec for them:
the columns are a **proposed v1** from the X API v2 objects, so the writer must send exactly these
column names (a different name is refused as `unknown columns`, nothing is dropped silently). List
values (`x_news.keywords`, `x_news.post_ids`) are `; `-joined text, since ingest() refuses nested
values. Raw checks keys only inside a day partition (the partition is the ingest timestamp's ICT
date), so these four carry `collected_at` in the key: the same engagement or like seen again on a
later day is a new version, never a duplicate key or a refused "changed values" row. Replies, full-archive and X Lists posts are ordinary posts and go to `x_feed` with their
source in `watch_type` / `watch_value`; they need no dataset of their own.

The version columns sit in the raw key (`x_bookmarks`, `x_list_sources`), so a re-seen or removed
bookmark and a new verification are new raw versions instead of a refused "changed values" row.

Column order and kinds: `infra/scripts/x_lakehouse/schemas.py` (`X_OWN_ACCOUNT_DAILY` …
`X_LIST_SOURCES`). Required at ingest: the key columns, plus `user_id` for `x_own_account_daily`;
for `x_bookmarks` only `post_id` and `last_seen_at` (a bookmark outside a folder has `folder_id = ''`,
stored as `''`, never NULL); for `x_list_sources` only `list_id`.

Rules shared with the other datasets: `run_id` as today; timestamps ISO-8601 with `+07:00` (or the
Sheet's ICT wall time) stored as `timestamptz`; a key sent again with the same values is skipped, with
different values it is refused ("send a new version"); unknown columns reject that row (and only that
row) and are reported by name. Phone numbers in free text become `[phone]` in `ingest()` itself and
again when the drain types the row (`coerce.PHONE_PATTERN`, one pattern used by Python and SQL):
Thai mobiles, Bangkok and provincial landlines (`053-123-456`), area codes in parentheses and
`+<country>` numbers of 8+ digits (`+1 (555) 123-4567`; `+1 2 3` is not a phone) in `text` of the three private post datasets, `x_spaces.title`, `x_communities.name` / `description`.
The row is never dropped for a phone number.

Writer responsibilities (the lake stores what it is sent, it does not compute): `followers_delta`,
`posts_24h`, `views_24h`, `member_delta`, `age_min`, `velocity`, `eng_rate`, `rank`,
`calendar_candidate`, `filter_flags`, and mapping the collection files' `themes` to `domain` /
`topic` / `tags` (`themes` itself is an unknown column). Rows with `filter_flags` (adult, spam, …)
are stored with their flags; none of these datasets is served anywhere. No reply text is stored for
`x_home_timeline` (there is no such column; `reply_text` is refused). Community posts are not a
dataset: topic posts keep going to `x_feed`. `x_list_sources` has no writer until the owner approves
X Lists; the allowlist is ready.

Kinds: "numeric" in the spec is `float` (`double precision`) and "int" is `int` (`bigint`), the
kinds this package already uses. **`x_list_sources` kinds are proposed** from the spec's one-line
column list (no closer model in the repo): `list_id/name/owner_handle/mapped_qid/members_hash/status`
text, `member_count/follower_count` bigint, `verified_at` timestamptz.

## 2. Private datasets: how they are kept apart

| layer | public datasets | private datasets |
|---|---|---|
| bot door | `x_intel.ingest(dataset, rows, run_id)` | `x_intel_private.ingest(dataset, rows, run_id)` |
| inbox | `x_intel.ingest_inbox` | `x_intel_private.ingest_inbox` (+ `ingest_seen`), RLS enabled, no policy |
| raw (R2 `x-intel-lake`) | `<prefix>/raw/<dataset>/…`, `<prefix>/_manifests/…` | `<prefix>/private/raw/<dataset>/…`, `<prefix>/private/_manifests/…` |
| Sheet | serve tabs as before | never: not in `SERVE_TABS`, `sheet_summaries` (`DATA_TABS` / `FINDING_TABS`), no view |

Bot call (the SQL file `bot_sql.py` writes picks the right door by dataset name):

    select x_intel_private.ingest('x_bookmarks', $json$[{...}]$json$::jsonb, 'run1111m');

`x_intel.ingest()` refuses the four private names (`private dataset: call x_intel_private.ingest()
instead; nothing stored`), and `x_intel_private.ingest()` refuses every other name. The cron drains
the private inbox after the public one (`scheduled.py` → `private.drain`), writes private raw only
under `private/`, and checks every drained private row reached R2 (`private.landed`). A listing of
`raw/` or `_manifests/` (validate, DuckDB queries, the BI site) never sees private files.
`serve_sheet.source_for()` refuses all seven new names before any query (`schemas.SERVE_DENY`).

Schema `x_intel_private` holds the two inbox tables and the door, nothing else. Grants
(`private._grants`, re-applied on every inbox/both run): `REVOKE ALL` on the schema, its tables,
sequences and functions from `PUBLIC`, `anon` and `authenticated`; `postgres` (the bots' connector
role) gets `USAGE` on the schema and `EXECUTE` on `x_intel_private.ingest()` only — no `SELECT` on
either inbox table. The schema is owned by the lake role `x_lake`. Drained inbox rows are deleted 3
days after the drain (their payload hash stays in `ingest_seen`), so Postgres holds private rows for
at most ~3 days; R2 holds them from then on.

Logs: ingest/drain log dataset names and counts only (`event=drain_private`, `event=prune_private`);
`drain_log` holds counts only. Coercion errors of a masked column keep the masked text. Unknown
columns of a private row are never kept in raw `_extra` (written as `[dropped]`).

### Limits (read before claiming "other bots cannot read it")

- **`postgres` usually bypasses RLS on Supabase.** The bots connect through the Supabase connector as
  `postgres`. This PR grants `postgres` no table privilege on `x_intel_private`, but if `postgres` is
  a member of `x_lake`, has `BYPASSRLS`, or is otherwise privileged in the project, schema + grants +
  RLS do **not** hide the inbox rows (≤ 3 days) from a bot that uses `postgres`.
- `serve_sheet`, the digest and the BI site connect with `X_LAKE_DB_URL` (role `x_lake`, the schema
  owner). Grants cannot keep the owner out, so for them the guard is code: deny lists and the tests
  P1–P3. `sheet_summaries` has no database access at all (it reads Sheet tabs).

Owner follow-up (no new cost; after merge; do **not** put a password in Git or a PR): create a role
for general bot SQL that has no privilege on `x_intel_private` and is not a member of `x_lake`, and
move the other bots' connector to it; keep `postgres` (or a dedicated `x_lake_private` role) only for
the job allowed to write private rows. Check with:

    select rolname, rolbypassrls, rolsuper from pg_roles where rolname in ('postgres', 'x_lake');
    select pg_has_role('postgres', 'x_lake', 'usage');
    select has_table_privilege('postgres', 'x_intel_private.ingest_inbox', 'select');

### Retention of private raw: R2 lifecycle rules (follow-up after merge, not done here)

Raw Parquet is append-only and the cron never deletes it. Retention for the private datasets is an R2
lifecycle rule on each prefix (free, set by the owner or by Claude with the owner's OK after merge):

| prefix | rule |
|---|---|
| `<prefix>/private/raw/x_home_timeline/` | delete objects after 30 days |
| `<prefix>/private/raw/x_own_mentions/` | delete objects after 365 days |
| `<prefix>/private/raw/x_bookmarks/` | keep (no rule): the intended rule is "180 days after `removed_at`", which depends on row content; an object-age rule cannot express it. A reader or a later job applies it. |
| `<prefix>/private/raw/x_own_account_daily/` | keep (never deleted) |
| `<prefix>/private/raw/x_own_likes/` | keep (no rule yet; the owner picks one) |
| `<prefix>/private/raw/x_bookmark_folders/` | keep (a few rows a day) |

The `_manifests` under `private/` are small and stay. Public `x_spaces` / `x_communities` /
`x_list_sources` / `x_post_engagers` / `x_news` raw is kept like every other public raw dataset.

## 3. Section 8: dropped (owner 2026-11-12)

The prompt's section 8 asked for small curated tables `x_trends_th`, `x_trends_intl`,
`x_topic_counts`, `x_account_snapshots` and `alpha_feed` in `x_intel` after Phase 2 (2026-11-11
07:00 ICT, when the Sheet X tabs stop growing), plus two optional serve tabs. The owner chose raw only:
those five datasets stay in raw on R2 like `x_feed` and `x_post_context` (which were never curated:
~1,000–2,000 rows/day would outgrow the Supabase free tier's 500 MB, and the post facts the bots need
are in `x_intel.x_signals`). No table, view, serve tab or switch exists for them. The existing curated
tables (`x_signals`, `claims`, `x_watchlist`, `x_query_plan`, `taxonomy_map`, `x_analyses`,
`x_triage_audit`) and the marts the digest, the BI site and the intake API read are unchanged.

**`x_intel.drain_log`** (kept): one row per inbox/both run, written at the end of the run (also when
`missing_from_raw` is not zero): `batch_id, started_at, source, rows_by_dataset, raw_written, curated,
coerce_errors, missing_from_raw, ok`. Values are dataset names and counts only; private datasets
appear by name with counts. `ok` is the run's lake result (before the optional serve refresh).
`postgres` may read it (bots check landing without the Railway log). It is not a dataset:
`ingest('drain_log', …)` is refused as unknown. It is never pruned. The read-only inbox health query
still works:

    select dataset, count(*) filter (where drained_at is null) pending, max(drained_at) from x_intel.ingest_inbox group by 1;

## 4. Section 9: `digest_log`

`x_intel.digest_log` (marts.py) gets four nullable columns (`alter table … add column if not
exists`): `ai_status text`, `fallback_reason text`, `telegram_message_ids jsonb` (a list of the
delivered Telegram `message_id`s; the existing `sent_messages` count stays), `auth_alerted_at timestamptz`.

| `ai_status` | meaning |
|---|---|
| `ok` | the AI summary was sent |
| `fallback` | the gateway was called but its answer was not used; the facts-only message was sent; `fallback_reason` says why (e.g. `gateway http 502 provider_auth`) |
| `skipped` | the gateway was not called (no signals, or a client term in the facts — then `fallback_reason` says so) |

`error` is now **delivery only**: Telegram not HTTP 200 / `ok:true` (`status` `error` or `partial`),
or a message blocked for a client term (`status = blocked`). A delivered digest has `error` null, even
when the AI summary fell back. `sent` is still set only after Telegram answers HTTP 200 with `ok:true`.

Alert: when the newest 3 digests that called the gateway all fell back on `provider_auth`, one message
goes to the same Telegram chat the digest uses (`TELEGRAM_NOTI_CHAT_ID`), once per streak: only a
digest that itself failed on `provider_auth` can send it, and the sending row is marked in a fourth
nullable column, `auth_alerted_at`. Skipped digests (gateway not called) neither count nor reset; any
gateway call that does not fail on `provider_auth` resets the count. Switch off with
`X_DIGEST_AUTH_ALERT=off` (history stays). No new service, credential or recipient; the alert text
holds no token or gateway body.

Old rows (before this change; `ai_status` is null): for `status = 'sent'`, a non-null `error` was the
AI fallback reason (delivery had succeeded); for `error` / `partial`, `error` is the delivery failure
and the AI outcome was not recorded; `blocked` is unchanged. The migration does not move old values.
To read old sent rows the new way:

    select digest_id, error as fallback_reason from x_intel.digest_log where ai_status is null and status = 'sent';

## 5. Differences from the prompt (repo facts win)

- There is no `lake_phase1.ALLOWED`: the drain/append allowlist is `schemas.DATASETS` (now the 14
  Sheet datasets + the 7 new ones), and `ingest()`'s `case` list is generated from it.
- `ingest()` rejects a row with an unknown column as a whole (reported with the column names); other
  rows of the call are stored with all their columns. That is the existing rule, kept.
- Raw keys of `x_bookmarks` (+ `last_seen_at`, `removed_at`) and `x_list_sources` (+ `verified_at`)
  carry the version column (§1).
- Curated tables, curated retention and the section-8 tables of the prompt are not built (raw only,
  §3); retention of private raw is an R2 lifecycle rule (§2).

## 6. Configuration (names only; nothing set by this PR)

Existing: `X_LAKE_DB_URL`, `X_LAKE_URI`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`,
`X_LAKE_SOURCE` (unchanged by this work), `X_LAKE_SERVE`, `X_LAKE_DIGEST`, `TELEGRAM_NOTI_BOT_TOKEN`,
`TELEGRAM_NOTI_CHAT_ID`, `AI_GATEWAY_TOKEN`.
New switches (optional; defaults shown): `X_LAKE_NEW_DATASETS=on`, `X_DIGEST_AUTH_ALERT=on`.

## 7. Tests

`infra/tests/x_lakehouse/x_lakehouse_private_test.py` (P1–P9, I1–I4, I6–I7, R5, S1–S6, S8, rollback
switch, schema-missing path; S1/S3 assert that no curated table exists for the seven datasets or the
five section-8 datasets) and new D1–D5 tests in `x_lakehouse_digest_test.py`; I5 / S7 / D5 are the
existing suites, unchanged and green. Local Postgres 17 + `file://` lake, as in CI.

## 8. Owner steps (in order; none done by this PR)

1. Create the private schema as `postgres` in the Supabase SQL editor (the lake role has no CREATE on
   the database): `create schema if not exists x_intel_private authorization x_lake;`
   Until then every private step logs `drain_private status=schema_missing` and nothing fails.
2. Review the DDL: from `infra/scripts`, `python -m x_lakehouse.schemas`,
   `python -m x_lakehouse.inbox --print-ddl`, `python -m x_lakehouse.private --print-ddl`.
3. Apply (optional before deploy; the cron also applies it on its next inbox/both run):
   `infisical run --path /x-lakehouse -- python -m x_lakehouse.inbox --apply` then
   `… -- python -m x_lakehouse.private --apply`. The `digest_log` columns are added on the next digest run
   (or `python -c "…marts.ensure_marts…"`).
4. `railway up` the `data-schedules/x-lakehouse` service as usual (push to main does not deploy).
5. Check: the role queries in §2, `select * from x_intel.drain_log order by started_at desc limit 3`,
   and `select dataset, count(*) from x_intel_private.ingest_inbox group by 1` (counts only).
6. Optional backfill of `x_own_account_daily` from the history CSV (outside the repo):
   `python -m x_lakehouse.own_account_backfill /path/x_own_accounts_history.csv` (dry run, counts only),
   then `--execute` (via `x_intel_private.ingest()`; the cron writes R2). Dates missing from the file
   stay missing; `snapshot_at`, `listed`, `followers_delta`, `posts_24h`, `reposts_24h`, `mentions_24h`
   stay NULL.
7. Follow-up: the R2 lifecycle rules in §2 and the `postgres` role split in §2.

## 9. Rollback

- Code: redeploy the previous image / commit (`railway up`); the owner does it.
- Switches, no data deleted: `X_LAKE_NEW_DATASETS=off` (the seven datasets become unknown again:
  `ingest()` refuses them, the drain leaves their inbox rows untouched, `x_intel_private.ingest()`
  refuses everything); `X_DIGEST_AUTH_ALERT=off`.
- Data: no `DROP TABLE`, `DROP SCHEMA`, `DROP COLUMN`, Parquet delete, tab delete or row delete in any
  rollback path. `drain_log` and `digest_log` rows stay. Dropping `x_intel_private` is a separate,
  owner-approved task. `X_LAKE_SOURCE` is not touched.
