# Spec: daily AI summaries for the "Chief Findings Warehouse" sheet

Status: accepted (2026-10-26); batches and merge added 2026-10-26. Standard: ADR-0004 (spec, then failing test, then code).
Code: `infra/scripts/sync/sheet_summaries.py`. Tests: `infra/tests/data_lake/sheet_summaries_test.py`.
Operator docs: [`docs/sheet-summaries.md`](../sheet-summaries.md).

## Problem

The Google Sheet "Chief Findings Warehouse" (`1zajA2iEfa9TS5JNjV1Y7LOurrTmiqrP-8ygn9clvBdI`) holds
raw rows only. The owner wants a short, current Thai digest per tab, made once a day after the last
X pull (~17:30 Asia/Bangkok), through the owner's own AI gateway and with no new fixed cost.

## Behaviour

1. **Tab discovery.** Read the tab titles at run time. Summarize the known finding tabs
   (`All Findings`, `Chief Market Strategist`, `Chief Intelligence Officer`, `Chief Oppotunities`,
   `Chief Business Builder`, `Chief Finance`, `Chief SME Operations`, `Chief Property`,
   `Chief Health & Lifestyle`, `Chief Career`, `Chief of Staff`, `Chief Social Media`), the data tabs
   (`Datasets`, `X Feed`) and any other tab whose title starts with `Chief ` (so `Chief Legal & Trust`
   is picked up when it appears). Never summarize `README`, `X Watchlist`, `X Query Plan`,
   `Retired Bots Archive`, `AI Summaries` or `AI Summaries Latest`.
2. **Headers.** Row 1 of each tab is its header; columns are found by name, never by position.
3. **Window.** Each `AI Summaries` row records the window as `after_row=<a>;to_row=<b>` (sheet row
   numbers). A tab's watermark is `to_row` of its newest row with status `ok`, `budget_exceeded` or
   `no_change`. Rows after the watermark are new. With no watermark (first run), or when the tab
   shrank below the watermark, the window is the last 7 days by the first date-like column
   (`collected_at`, `posted_at`, `date`, `found_at`, `created_at`, ...); a tab without one uses the
   newest rows.
4. **No new rows** → write a `no_change` row (same `to_row`) and make no gateway call.
5. **Client guard (hard).** Load the denylist from `CLIENT_DENYLIST_PATH` (one term per line, `#`
   comments). Missing path, missing file or no terms → the run fails before reading any tab. Terms
   and text match if either the NFKC case-folded form or the letters-and-digits skeleton (NFKD, no
   format, combining, invisible or punctuation characters) contains the term. A row whose serialized form
   (header names and cells) contains a term is dropped before it reaches the model; a tab whose name
   contains a term is skipped; the final prompt is checked again; a model answer that contains a
   term is rejected and not written. A malformed service-account value never appears in an error.
6. **Input cap.** At most 200 rows per tab and 500 characters per cell. The prompt must also fit the
   gateway's live limits from `GET /api/ai/status` (`limits.max_context_chars`,
   `limits.max_token_budget` minus the output reservation; the gateway counts input tokens as UTF-8
   bytes + 32 per message). Rows are taken in priority order: for `X Feed` every `top` row before any
   other, and within each group the requesting Chiefs (the first named in `requesting_chief`) take
   turns, highest `score` first, so one busy Chief cannot fill the request; the newest rows elsewhere.
6a. **Batches and merge.** A tab whose rows do not fit one request is split into at most 8 batches
   (batch 1 holds the highest-priority rows); rows left over once 8 batches are full are not sent and
   the window still moves past them. Each batch answer is validated on its own (item 9) and asks for
   a short partial. Merge calls (`prompt_version=sheet-summaries-merge-v1`) then combine the
   validated partials, as many per call as fit, round after round until one answer is left; the
   merge sees only partials, never raw rows, and its links must appear in the partials it merged. A
   batch whose answer is invalid, truncated, has an invented link or a client term is dropped and
   never written; a merge with a malformed answer is tried once more; a failing merge fails the tab.
   `rows_considered`, tokens and cost cover every call; `run_id` is the last call's.
7. **Gateway call** (`solo-empire.ai-gateway.v1`, unchanged): `POST /api/ai/generate` with
   `route=sheet-summaries`, `prompt_version=sheet-summaries-v1`, `profile=fast`,
   `privacy_class=internal`, `response_format=json_object`, a `token_budget` and
   `max_output_tokens`. Before any generate call the run reads `GET /api/ai/status` and stops unless
   `budget.decision == "allow"`.
8. **Error classes.** HTTP 200 with `status` `success` or `budget_exceeded` is a result. 429, 5xx,
   401/403 and network errors stop the run; 502/503/504, network errors and 429 `rate_limited` get
   one retry after a short backoff (never a loop), budget denials (`monthly_budget_exceeded`,
   `budget_unavailable`, `budget_not_configured`) get none. A stopped run writes `status=error` rows
   for the current and remaining tabs. A 400 marks only that tab as `error`.
9. **Output validation.** The answer must be JSON matching the schema: `summary_th` (Thai text),
   `top_items` ≤ 5 of `{title, why, link, link_source}`, `candidate_tasks` ≤ 5 of
   `{title, value, deadline|null, link|null, suggested_owner_role}`, `deadlines` of
   `{date, what, link|null}`. Every link must appear in the tab's input rows; an invented link
   rejects the answer. `link_source` is recomputed from the URL (`x_post` for x.com / twitter.com
   `/status/` URLs, else `website`).
9a. **Owners.** Every prompt lists the sheet's `Chief *` tab names as the only allowed
   `suggested_owner_role` values. After validation each task's owner is set to a listed Chief: the
   model's choice when it names one (the first when it names several), else the Chief that requested
   the task's linked row (`requesting_chief` / `Source Chief`), else the tab itself when it is a Chief
   tab, else `""` (unassigned). An invented job title is never written.
10. **Write-back.** Create `AI Summaries` if missing with the header
    `tab, summary_date, window, rows_considered, summary_th, top_items, candidate_tasks, deadlines,
    model, run_id, tokens_in, tokens_out, cost_usd, status, error` and append one row per tab per run.
    Rewrite `AI Summaries Latest` (same header) with the newest row per tab. Source tabs are never
    written. Nothing is written to tasks-api.
11. **Logs.** One JSON line per tab with `run_id, tab, status, tokens_in, tokens_out, cost_usd,
    latency_ms` only. No prompt, answer, token or cell text in logs or Sentry.
12. **Dry run.** `--dry-run` prints the would-be `AI Summaries` rows and writes nothing.
    `--fixture <json>` reads tabs from a file instead of Google, `--fake-gateway` answers from the
    rows without a network call.

## Acceptance tests

| # | Test |
|---|---|
| A1 | Header discovery works on tabs with different columns |
| A2 | Skip list respected; a new `Chief *` tab is picked up |
| A3 | Second run with no new rows makes zero gateway calls and writes `no_change` |
| A4 | An answer with a link not present in the rows is rejected (`status=error`) |
| A5 | Denylisted rows never reach the gateway; a missing or empty denylist fails closed |
| A6 | `budget_exceeded` is a result; 429 / 5xx / budget deny stop the run with one retry at most |
| A7 | Preflight aborts when `budget.decision != allow` with no generate call |
| A8 | Logs carry no prompt, answer, token or cell text |
| A9 | X Feed over the cap keeps the `top` rows (A13 sets their order) |
| A10 | A tab over one request is split into ≤ 8 batches and merged (in rounds when needed) into one row |
| A11 | A merge answer with a link not in its partials is rejected; a malformed one is retried once |
| A12 | A bad or client-term batch answer is dropped and never written; a client term in the merge fails the tab |
| A13 | X Feed sends every `top` row before any other and rotates the requesting Chiefs within each group |
| A14 | Prompts list the Chief tabs as owners; an invented owner title becomes a real Chief or `""` |

## Out of scope

Writing to tasks-api, changing the gateway, creating the service account or tokens, deploying.
