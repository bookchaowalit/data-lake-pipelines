# SPEC: X intelligence digest to Telegram

Status: approved by the owner 2026-11-09 ("Yes, Telegram first"). Code: `infra/scripts/x_lakehouse/marts.py`,
`infra/scripts/x_lakehouse/digest.py`. Tests: `infra/tests/x_lakehouse/x_lakehouse_digest_test.py`.

## Goal

Every cron run of `x-lakehouse` (08:20 / 16:20 / 22:20 ICT) turns the curated X data into a short Thai digest
and sends it to the owner's Telegram chat. No new service, no new fixed cost; email follows once
`platform-communication` is deployed.

## Layers

| layer | object | content |
|---|---|---|
| curated (existing) | `x_intel.x_signals`, `x_intel.claims` | one row per post / claim |
| mart | `x_intel.mart_x_domain_hourly` | per ICT hour and domain: signals, alerts, opportunities, T1/T2 sources, avg score |
| mart | `x_intel.mart_x_signal_priority` | every signal with a `priority` score (alert tier, opportunity score, source tier, engagement) and the post facts |
| mart | `x_intel.mart_claims_queue` | open claims ordered by priority and next check |
| log | `x_intel.digest_log` | one row per digest id: window, status, counts, tokens, cost; never message text |

Marts are plain views (the curated tables are small; ADR-0013: one lake, no new engine). Raw-only datasets
(trends, topic counts, snapshots) are out of scope for v1.

## Windows (ICT)

| run hour | kind | window |
|---|---|---|
| before 12:00 | `morning` | yesterday 22:00 → now |
| 12:00–18:59 | `midday` | today 08:00 → now |
| 19:00 and later | `daily` | today 00:00 → now (recap of the whole day) |
| Monday morning run | `weekly` (extra) | now − 7 days → now, aggregates only plus top 10 |

Digest id = `<kind>:<window date>`. A digest id already `sent` is never sent again (reruns, manual runs).

## Message

1. Facts (built in SQL, counts and the top rows) go to the AI gateway (`X_DIGEST_ROUTE`, default
   `sheet-summaries`, profile `fast`, JSON answer): headline, 3–5 bullet summary, alerts, opportunities, things to watch.
2. Validation: JSON shape; every link must be one of the input links; no client denylist term.
3. Telegram HTML message, every value escaped (X text is untrusted), split at 4,000 characters, link previews off.
4. If the gateway fails or its answer is invalid, a facts-only message is sent instead (counts + top links), marked as such.

## Guards

- `CLIENT_DENYLIST` (one term per line, same list as sheet-summaries) is required to send; a row whose text
  matches never leaves the host; a final message with a match is not sent (status `blocked`). Fails closed.
- `X_LAKE_DIGEST`: `off` (default) | `dry` (facts counts only, no gateway, no send) | `send`.
- Logs and `digest_log` carry counts, ids, tokens and cost only, never post text.
- A digest failure is logged (`event=digest status=error`) and does not fail the lake run.

## Acceptance

- D1 windows per run hour, weekly extra on Monday morning.
- D2 marts: priority orders urgent > digest > none, then score; hourly counts match the base table.
- D3 a sent digest id is not sent twice.
- D4 denylist: matching rows are left out of the facts; a matching answer blocks the send; no denylist = no send.
- D5 invented link in the answer → facts-only fallback; gateway error → facts-only fallback.
- D6 HTML escaping of X text; messages split under 4,096 characters.
- D7 `dry` never calls the gateway or Telegram; `off` does nothing.
