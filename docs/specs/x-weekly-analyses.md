# SPEC: weekly X analyses and triage audit, built by the lakehouse

Status: requested by the owner 2026-11-11 ("ทำเลย", move X Analyses and X Triage Audit to our side).
Code: `infra/scripts/x_lakehouse/weekly.py`. Tests: `infra/tests/x_lakehouse/x_lakehouse_weekly_test.py`.

## Why

`x_analyses` and `x_triage_audit` were written once a week by the Chief Data bot (Sheet tabs "X Analyses" and
"X Triage Audit"). Both are derived from data the lake already holds, so the Sheet cutover should not wait on
the bot. Only one week exists so far (`2026-W45`: 60 analysis rows, 1 audit row).

## Parallel run first

v1 writes to its own tables, `x_intel.lh_analyses` and `x_intel.lh_triage_audit`. They have the same columns as
the bot tables, plus `run_id` and `created_at`. The bot rows stay untouched. After two weeks the owner compares
both and decides whether to switch the readers and stop the bot job.

## When it runs

`X_LAKE_WEEKLY` = `off` (default) | `dry` | `write`, called by `scheduled.py` after the digest.
- The target is the last complete ISO week in ICT (Monday 00:00 to the next Monday 00:00).
- `write` runs only when `lh_analyses` has no row for that week yet, so the first cron run of each week builds
  it and later runs do nothing (idempotent).
- `dry` computes the analyses and logs counts only. It never calls the gateway and never writes.
- A failure is logged (`event=weekly status=error`, exception type only) and never fails the lake run.

## Analyses (no AI; SQL over the curated tables and marts, one week)

| analysis | rows | rule |
|---|---|---|
| `fad_sustained` | top 10 topics | count of signals this week; `metric` = the last 6 weeks as `W45:60 W44:..`; `note` = weeks in a row with at least 20 signals |
| `debate` | top 5 | open claims (not closed, done, resolved, archived or dismissed) first seen or checked this week, by `spread_count` desc. Writes one placeholder row when there are none, like the bot |
| `local_discovery` | top 5 | signals whose post text has Thai script and whose label is `fact_report` or `evidence`, by priority |
| `community_knowledge` | top 5 | `signal_type = 'Discussion'` with at least 20 likes, by reposts per like |
| `life_path` | top 7 | per `life_cat` value (split on `;`): signals labelled `evidence` or `fact_report` this week |
| `price_reported` | top 5 | signals with a price-like token in the text (`%`, `฿`, `$`, `บาท`, `USD`), label not `promo`; `value` = first token; `value_kind` = `reported (anecdotal, unverified)` |
| `price_verified` | top 5 | `alpha_feed` raw rows pulled this week from a price source (`PRICE_SOURCES`: `energy_food_health/fao_ffpi` for now; `who_don` and `eia_today_in_energy` carry no price) with `metric_1` set, newest first; `value_kind` = `verified (official/Alpha source)`; `source_tier` = `T1` |

`item` holds the first 200 characters of the post or claim text, `link` the post URL, and `source_tier` the
`cred_tier`. The bot's `geo` and `book_domain` columns are filled from `domain` only (no geo inference in v1).

## Triage audit (AI via the gateway, route `x-digest`)

1. Sample 30 signals of the week in a stable order (`md5(post_id || week)`). Rows matching the client denylist
   are skipped before sampling. Without `CLIENT_DENYLIST` the audit does not run (fails closed).
2. The gateway labels each post (`fact_report`, `opinion`, `claim`, `evidence`, `promo`) and picks a route
   (`alert_digest`, `store`, `verify`, `discard`). It sees only the post text, author tier and domain, with
   short definitions written here (`LABELS`, `ROUTES` in weekly.py). The answer is JSON, and any unknown value
   counts as a disagreement.
3. The row gets `n`, `label_agree` and `route_agree` (shares, 2 decimals), and `label_confusions` /
   `route_confusions` as `bot->audit xN` (the bot's existing format). `drift_vs_prev` is the change in
   `label_agree` against the previous lh week. `rule_version` is `lh-v1`.
4. Gateway failure means no audit row for that week. The next run retries it, because the week counts as done
   only once `lh_triage_audit` has its row.

The auditor is a different model and prompt from the bot, so the agreement is an independent check. Our
definitions are not the bot's tv4 rule text, which this repo does not have. Expect lower agreement until
the owner aligns them.

## Guards

- Logs carry counts, the week, and exception types only, never post text.
- The prompt is screened with the client denylist before it is sent. Rows that match are never sent.
- Writes are one transaction per table, keyed `(report_week, analysis, rank, item)` / `(audit_week)`.

## Acceptance

- W1 week bounds: the last complete ISO week in ICT; a run on Monday 08:20 targets the week that just ended.
- W2 each analysis returns the rows the table above defines; `debate` writes its placeholder when empty.
- W3 `write` is idempotent: a second run in the same week writes nothing.
- W4 `dry` writes nothing and never calls the gateway.
- W5 triage: agreement and confusions are computed from the sample; denylisted rows never reach the prompt;
  a gateway error leaves no audit row and the next run retries it.
- W6 `off` does nothing; a failure never fails the lake run.
