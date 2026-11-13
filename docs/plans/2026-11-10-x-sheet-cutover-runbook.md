# Runbook: X Sheet cutover (plan §11 Phase 2, steps 1 and 3)

Prepared 2026-11-10 on branch `prep/sheet-cutover` (not merged: `ensure_inbox` applies its DDL on every
cron run, so merging + uploading the image *is* step 2 below). Plan: `2026-11-05-x-lakehouse.md` §11.
Every step below runs only on the owner's go; earliest day **2026-11-13** (Day 1 = 11-06, 7 days of ok runs).

## Where things stand (2026-11-10)

| Phase-2 item | State |
|---|---|
| Phase 1 dual write (`both`) | done 11-09 07:00 UTC; compare 0 mismatch |
| Step 2 `X_LAKE_SOURCE=inbox` | done 11-09 08:05 UTC; all 13 datasets now arrive through the inbox (11-10) |
| Step 1 Chief Data stops Sheet writes, reads config from views | **open**: needs the watchlist row map (this branch) + the bot change (brief below) |
| Step 3 `X_LAKE_SERVE=execute` | **open**: dry run 11-10 = Serve · X Signals 7d 5,912 × 23, Open Claims 25 × 17, X Query Plan 218 × 24 |

**Run record** (Railway `event="done"`, all deployments): every logged run `ok=true` from 11-05 12:50 to
11-10 13:27 UTC, 21 runs; the only empty slot is 11-07 01:20 UTC (deployment replaced, known). Re-read before go:

```bash
cd <scratch dir linked to data-schedules/x-lakehouse>
# scheduled._log writes JSON lines ({"event": "done", "ok": true, ...}), not key=value text
for id in $(railway deployment list --json | jq -r '.[].id'); do railway logs $id --json --lines 5000; done \
  | jq -r 'select(.event == "done") | "\(.timestamp[0:16]) ok=\(.ok)"' | sort -u
# expect one line per scheduled run (20 1,9,15 * * * UTC) and no ok=false; an empty list is a failed check
```

**Watchlist row numbers** (the blocker in plan §12): `x_query_plan.watchlist_rows` holds 710 references to X
Watchlist **Sheet row numbers** (102 of 218 query-plan rows). `v_watchlist_sheet` sorted by key, so after the
cutover those numbers would have pointed at other rows. Fix on this branch: `x_lakehouse/watchlist_rows.py` —
`x_intel.watchlist_row_map` (frozen `row_no` per key, seeded from the tab), an AFTER INSERT trigger that numbers
new entries like an append, and `v_watchlist_sheet` in that order with a new last column `sheet_row`.
Read-only dry run 2026-11-10: tab rows 2–1,044 contiguous, 1,043 keys = curated 1,043, 0 duplicates,
0 conflicts; all 710 references resolve; Sheet and Postgres query plans agree on all 218 rows.

## Cutover day (owner go)

1. **Re-check:** run record above (no `ok=false` since 11-06) and the seed dry run:
   `python -m x_lakehouse.watchlist_rows --seed` (env: Infisical `/sheet-summaries` + `/x-lakehouse`) →
   `moved 0, number_conflicts 0, duplicate_keys_in_sheet 0`.
2. **Ship the map:** merge `prep/sheet-cutover` to main locally, push, then `railway up` a `git archive` of main
   to `data-schedules/x-lakehouse` (as before). The next cron run creates the table, trigger and view; the trigger
   stays inert until step 3 (empty map).
3. **Freeze watchlist edits, then seed.** Ask Chief Data to add no X Watchlist entries from now until step 4, and
   wait for any bot run in progress to finish: two entries appended in one run reach `x_watchlist` in key order, so
   the trigger (or the seed's catch-up insert) could number them differently from their tab rows. Then seed: `python -m x_lakehouse.watchlist_rows --seed --execute` → `written 1043` (+ any entries added since).
   Check: `select count(*), min(sheet_row), max(sheet_row) from x_intel.v_watchlist_sheet` = rows, 2, max+.
   Re-run the dry run (`--seed`) right after the seed and again just before step 4: it must show `moved 0`,
   `number_conflicts 0`, `to_insert 0`, `curated_unmapped_before 0`. Any `moved` row means a number differs from the
   tab: stop and fix the map row by hand (the tab is the truth until step 4) before the bot switches.
4. **Bot change:** the owner gives Chief Data the brief below. From its next run it writes nothing to the X tabs.
5. **Verify one bot run later:** the cron `event="drain"` shows every dataset, `event="validate"` ok, no
   `config_coerce_errors`; the X tabs have no new rows after the switch time.
6. **Serving tabs:** Railway var `X_LAKE_SERVE=execute` (no redeploy needed; read each run). The next run writes
   "Serve · X Signals 7d", "Serve · Open Claims", "Serve · X Query Plan" (tabs it owns; never AI Summaries*, Chief *).
7. The old X tabs stay frozen. Nothing is deleted (Phase 3, ≥ 30 days, owner approval).

**Rollback:** before step 4 — nothing to undo (drop `watchlist_row_map` if wanted; the view still works with
`sheet_row` null). After step 4 — tell Chief Data to resume the Sheet appends and set `X_LAKE_SOURCE=both`; rows
written only to the lake meanwhile are copied back from raw (plan §11 rollback, owner approval to write a source tab).

## Brief for Chief Data (owner pastes it into the Grok bot)

> From <switch time ICT> the X pipeline no longer uses the Google Sheet X tabs.
>
> 1. **Stop** appending to X Feed, X Signals, X Post Context, Alpha Feed, X Trends TH/Intl, X Topic Counts,
>    X Account Snapshots, X Analyses, X Triage Audit and Claims, and stop writing X Query Plan columns G:P.
>    Keep sending every dataset exactly as today with `select x_intel.ingest('<dataset>', '<rows json>'::jsonb,
>    '<run_id>')` (≤ 500 rows / 900 KB per call; x_feed before x_signals).
> 2. **Read config from the database, not the tab:**
>    - query plan: `select * from x_intel.v_query_plan_sheet` (same columns and cell format as the tab);
>    - watchlist: `select * from x_intel.v_watchlist_sheet order by sheet_row` — `sheet_row` is the row number the
>      query plan's `watchlist_rows` and snapshots' `watchlist_row` refer to (unchanged from the tab);
>    - open claims: `select * from x_intel.v_claims_open`.
> 3. **Config edits** (new watchlist entry, query-plan change, run state such as `last_since_id`/`last_run_at`) go
>    through `ingest('x_watchlist' | 'x_query_plan', …)`. A new watchlist entry gets the next `sheet_row`
>    immediately; read it back from `v_watchlist_sheet` before referencing it in `watchlist_rows`.
> 4. If an `ingest` call fails, retry once, then report the run id; never fall back to writing the tab.
