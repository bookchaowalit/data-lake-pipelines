# Spec: X readers on raw X Feed (2026-11-14)

Status: approved by the owner 2026-11-14 ("ปรับให้ใช้ x_feed"). Code: `infra/scripts/x_lakehouse/feed.py`,
`serve_sheet.py`, `digest.py`, `bi_site.py`. Tests: `infra/tests/x_lakehouse/x_lakehouse_feed_test.py`.

## Why

On 2026-11-13 08:31 ICT the owner told Chief Data to send raw data only: no AI tagging, triage or claim
extraction (token cost). From run1113d on, the bots send no `x_signals`, `x_post_context` or `claims`.
Every reader was built on those curated tables, so the Serve tab, the Telegram digest and the x-insights site
would run dry within a week. `x_feed` still arrives on every run.

## Rules

- No AI call and no new curated table (RAW-ONLY rule 2026-11-12): the facts come from raw `x_feed` on R2.
- One entry per post: the newest observation in the window (its counts are freshest); `watches` = how many
  distinct watches (`watch_type:watch_value`) saw the post. Repeats across watches are real (d15d9be).
- Ranking: `engagement = likes + 2 × reposts + replies`, then views, then newest.
- The digest and the site screen every field against the client denylist (fail closed), as for signals.
  The Sheet tab is the owner's own and is not screened (same as the Signals tab).
- A failing lake read never costs the run: the digest logs `feed_error` and goes on without the feed; the site
  builds without the feed page's data and logs `feed_error`.

## Readers

| Reader | Change |
|---|---|
| Sheet | new owned tab `Serve · X Feed 7d`: top 2,000 posts of the last 7 days, 17 columns. `Serve · X Signals 7d` and `Serve · Open Claims` keep refreshing from Postgres (they empty out / freeze on their own). |
| Telegram digest | facts gain `feed` (totals: posts, authors, watches; top 8 watches; top 15 posts, 10 weekly). The gateway is called when there are signals **or** feed posts; feed links are allowed links. Facts-only fallback lists the top feed posts. `PROMPT_VERSION` = `x-digest-v2`. |
| x-insights site | new page `/feed/` "Top posts": top 50 posts of the last 7 days (window ends on the current hour, so a rebuild within the hour is byte-stable) and posts per watch. |

## Weekly analyses and Gold (2026-11-14, second step)

| Reader | Change |
|---|---|
| `weekly.py` | adds the raw twins `watch_sustained` (top 10 watches by distinct posts this week, the last 6 weeks, weeks in a row at 20+), `feed_local_discovery` (Thai posts), `feed_community_knowledge` (likes ≥ 20, by reposts/like) and `feed_price_reported` (price-like text), each by engagement. The signal analyses stay and run dry; `life_path`, `debate` and the triage audit need AI labels and have no raw twin (with no signals the audit finds no posts and never calls the gateway). |
| Gold | `02_top_accounts_by_signals` is replaced by `02_top_accounts_by_posts`: authors by distinct posts in 30 days (newest counts per post), likes, reposts, views, watches, main watch. |

## Not changed

Gold `01_daily_volume_per_domain` (X Signals domains) and the site's Signals / Claims pages still read
x_signals / claims and show the last data.
