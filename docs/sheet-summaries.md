# Sheet summaries (daily Thai digest of the Chief Findings Warehouse)

Spec: [`docs/specs/sheet-summaries.md`](specs/sheet-summaries.md). Code:
`infra/scripts/sync/sheet_summaries.py`. Tests: `infra/tests/data_lake/sheet_summaries_test.py`.

## What it does

Once a day it reads each Chief tab, `All Findings`, `Datasets` and `X Feed` of the Google Sheet
`1zajA2iEfa9TS5JNjV1Y7LOurrTmiqrP-8ygn9clvBdI`, sends only the rows added since its last run (first
run: last 7 days) to the AI gateway (`POST https://api.bookchaowalit.com/api/ai/generate`, route
`sheet-summaries`, profile `fast` = GPT-6 Luna) and appends one row per tab to `AI Summaries`:
a 3–6 sentence Thai summary, up to 5 top items (with `x_post` / `website` link source), up to 5
candidate tasks for the tasks-api backlog (suggestions only, nothing is written to tasks-api; each
`suggested_owner_role` is a real `Chief *` tab name or empty, never a made-up title) and any
dated deadlines. `AI Summaries Latest` holds the newest row per tab. Source tabs are never written.

Safety: rows matching the client denylist (Oak, freelance-hub clients) are dropped before the call,
and the run refuses to start without a denylist. Answers are validated against a JSON schema; an
answer with a link that is not in the rows, or with a client term, is rejected (`status=error`).
Logs carry run id, tab, status, tokens, cost and latency only.

## Where it runs and why

Repository: `bookchaowalit/data-lake-pipelines`, the owner of scheduled data jobs. Its own host is
local only, so the job runs as a Railway cron service next to `news-capture` in the existing project
`data-schedules` (owner decision D6). No new fixed cost: a cron service bills only while it runs
(seconds a day), and GPT-6 Luna is billed per token.

| Item | Value |
|---|---|
| Railway project / service | `data-schedules` / `sheet-summaries` |
| Image | `deploy/sheet-summaries/Dockerfile` (build runs a fake-gateway dry run as its smoke test) |
| Cron | `0 12 * * *` UTC = 19:00 Asia/Bangkok, after the last X pull (~17:30) |
| Restart policy | NEVER (one run per schedule, no loop) |
| Deploy | CLI upload: `railway up` of a clean `git archive <commit>` (Railway refuses `serviceConnect` to this repo for the CLI user, as for news-capture). First deploy 2026-10-26: d7ddc9d. A push to `main` does **not** redeploy; repeat the upload for a new commit |
| Heartbeat | posts to `/heartbeat/sheet-summaries` on the uptime Worker when `HEARTBEAT_TOKEN` is set; the Worker needs a `sheet-summaries` entry (e.g. alert after 26 h) before it alerts |
| Sentry | static "capture run failed" event, scrubbed (`data_lake/observability.py`) |

## Variables (owner creates; nothing is set by this PR)

| Variable | Value |
|---|---|
| `AI_GATEWAY_TOKEN` | gateway client token, scopes `ai:generate` + `api:read`, subject `sheet-summaries`: `task ai-gateway:client:provision -- --subject sheet-summaries` |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | the service-account key JSON (or a path to it) |
| `CLIENT_DENYLIST` | client terms, one per line (Oak, freelance-hub client names, domains). The entrypoint writes it to a file; or set `CLIENT_DENYLIST_PATH` to a file |
| `SENTRY_DSN`, `SENTRY_ENVIRONMENT=production` | optional |
| `HEARTBEAT_TOKEN` | optional, Infisical `/monitoring` |
| `AI_GATEWAY_URL`, `SHEET_SUMMARIES_SPREADSHEET_ID` | optional overrides; defaults are the live gateway and the sheet above |

### Google service account (one sheet only)

1. Google Cloud console → a project you own → enable **Google Sheets API**.
2. IAM → Service accounts → create `sheet-summaries` (no project roles).
3. Keys → Add key → JSON. Store it in Infisical, then set `GOOGLE_SERVICE_ACCOUNT_JSON` on Railway.
4. Open the sheet → Share → add the service account's email as **Editor** (it must create and
   append to `AI Summaries`). Share nothing else with it.

The job asks only for the `https://www.googleapis.com/auth/spreadsheets` scope; access is limited by
what is shared with the account, which is this one sheet.

## Run it by hand

```bash
# Offline: fake fixture + fake gateway, prints the would-be rows, writes nothing
CLIENT_DENYLIST_PATH=deploy/sheet-summaries/smoke-denylist.txt uv run python infra/scripts/sync/sheet_summaries.py --dry-run --fixture infra/tests/fixtures/sheet_summaries/sheet.json --fake-gateway

# Real sheet and gateway, still no writes (costs a few cents at most)
infisical run -- uv run --extra sheets python infra/scripts/sync/sheet_summaries.py --dry-run

# One tab for real
infisical run -- uv run --extra sheets python infra/scripts/sync/sheet_summaries.py --tab "Chief Property"
```

These run from the owner's machine with the same variables; the scheduled run stays on Railway.

## Window and limits

The `window` column is `after_row=<a>;to_row=<b>` (sheet row numbers); the next run starts after the
newest `to_row` with status `ok`, `budget_exceeded` or `no_change`. Error rows do not move it, so a
failed day is retried the next day. If a tab shrinks below its watermark, it starts over at the last
7 days. Each request fits the live limits from `GET /api/ai/status`: at most 200 rows, 500 characters
per cell, and input counted as UTF-8 bytes (Thai is 3 bytes per character). Rows go in priority
order: `X Feed` every `top` row first, the requesting Chiefs taking turns (best `score` first), other tabs the newest rows.

A tab too large for one request (X Feed gets ~1,400 rows a day, ~230 of them `top`) is split into up
to 8 batches, each summarized as a short partial, then merged into one answer by merge calls that see
only the validated partials. With the gateway's `max_token_budget` of 24,576 a batch holds ~25 X Feed
rows, so a day covers ~170 of the highest-priority rows (measured 2026-10-26); rows past the 8th
batch are skipped. A partial that fails validation or mentions a client term is dropped (never
written) and the rest are merged; a malformed merge is retried once. Raising the gateway's
`AI_GATEWAY_MAX_TOKEN_BUDGET` fits more rows per batch.

## Cost estimate (GPT-6 Luna: $0.10 / 1M input, $0.50 / 1M output)

| Case | Per tab | 14 tabs / day | Per month |
|---|---|---|---|
| Typical day (~3K input, ~600 output tokens) | $0.0006 | $0.008 | ~$0.25 |
| Every tab at the 32K ceiling (~28K in, 2K out) | $0.0038 | $0.053 | ~$1.60 |
| Default 4K gateway budget | ≤ $0.0006 | ≤ $0.008 | ≤ $0.25 |
| X Feed in 8 batches + merges (measured 2026-10-26: ~58K in, ~11K out) | ~$0.011 | — | ~$0.35 |

`no_change` tabs make no call. The gateway's monthly budget guard still applies: the run checks
`budget.decision == "allow"` first and stops on a budget denial.

## Status values

`ok`, `budget_exceeded` (answer written; the gateway flagged the budget), `no_change` (no call),
`error` with `error` = `invented_link`, `invalid_output`, `output_truncated`, `client_term_in_output`, `row_too_large`,
`gateway_400:<code>`, `gateway_stop:<code>` (429 / 5xx / auth / network after at most one retry) or
`budget_<decision>` (preflight).

## Rollback

Turn it off: remove the cron schedule of `sheet-summaries` in Railway (nothing else runs it), or
delete the service. Undo the data: delete the `AI Summaries` and `AI Summaries Latest` tabs; no source
tab is changed. Revoke the gateway token (`sheet-summaries` subject) and remove the service account
from the sheet's sharing list.
