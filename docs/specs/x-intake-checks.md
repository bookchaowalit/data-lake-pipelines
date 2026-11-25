# SPEC: X intake sanity checks

Status: implemented 2026-11-26 (`infra/scripts/x_lakehouse/intake_checks.py`, cron hook `scheduled._intake_checks`).

## Problem

The inbox accepts any row with the right columns and a new key. Two bot-side faults passed that for days:

1. From the 2026-11-17 evening run the Chief Data bot stamped non-news `x_feed` rows with a UTC+9 clock and no
   zone, 2 h late in ICT (found 2026-11-25, nine days later, through a midday digest with 0 posts).
2. On 2026-11-24 the morning run's second send reused its batch folder and overwrote its own queue: 84 of
   464 `x_feed` rows arrived (the rest came as `run1124m_reland` on 2026-11-25).

## Behaviour

After each drain the cron reads the inbox rows received in the last day and logs one `intake_checks` event.
Its `issues` go to the existing ops alert (Telegram, `X_LAKE_OPS_ALERT`). Rows are never refused or changed,
and a check failure is one `intake_checks: error <type>` line; the run's result stands.

| Check | Rule | Alert |
|---|---|---|
| Sender clock | a row of `x_feed`, `x_news`, `x_post_engagers` or `x_spaces` received in the look-back whose `collected_at` (zone-less read as ICT) is later than its inbox `received_at` + 5 min | one line per (run_id, dataset): rows and the largest lead |
| Missing or thin run | an expected-run window (ICT inbox arrival, default `06:00-08:15` and `18:00-20:30`) that closed in the look-back (6 h) brought fewer than 100 `x_feed` rows | one line per window: `no x_feed run` or the count |
| Zone-less stamps | `collected_at` without `Z`/`±hh:mm` | none; counts per dataset in the log (`naive`) |

The look-back matches the gap between cron runs (08:2x, 16:2x, 22:2x ICT), so each window and each run is
reported once. Settings: `X_LAKE_INTAKE_CHECKS=off`, `X_LAKE_INTAKE_WINDOWS`, `X_LAKE_INTAKE_MIN_ROWS`,
`X_LAKE_INTAKE_LOOKBACK_HOURS`, `X_LAKE_INTAKE_FUTURE_MINUTES`. Change the windows when the bot schedule changes
(the midday run is paused since 2026-11-17).

## Evidence (replayed on the real inbox, 2026-11-26)

- 11-24 08:24: `intake 2026-11-24 06:00-08:15: x_feed 84 rows (< 100)`
- 11-24 22:23, 11-25 08:23, 11-25 22:22: `intake run11xx x_feed: N rows stamped up to 118 min after arrival`
- 16:2x runs: no lines (no window closed, no new rows).

## Tests

`infra/tests/x_lakehouse/x_lakehouse_intake_checks_test.py` (offline, 11 tests).
