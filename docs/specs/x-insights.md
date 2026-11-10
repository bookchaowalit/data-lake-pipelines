# SPEC: X insights BI site (x-insights.pages.dev)

Status: approved by the owner 2026-11-10 ("Yes, build it" — a dashboard like news-insights over the X marts).
Code: `infra/scripts/x_lakehouse/bi_site.py`, `infra/scripts/x_lakehouse/pages_deploy.py`, hook in
`scheduled.py`. Tests: `infra/tests/x_lakehouse/x_lakehouse_bi_test.py`.

## Why

The X pipeline lands signals, claims and analyses in Supabase `x_intel`, and the only reader is the
Telegram digest. The news, jobs and sales loops each end in a private static BI page; X has none.

## Where it runs, and why not GitHub Actions

`data-lake-pipelines` is a **public** repository, and the X data sits in Postgres (not in R2 Gold), so a
GitHub job would need `X_LAKE_DB_URL` (the write-capable `x_lake` role; no read-only role exists and
`x_lake` cannot create one). The site is therefore built by the Railway cron `x-lakehouse` that already
holds that secret, at the end of each run (3× a day), and uploaded to Cloudflare Pages through the Pages
Direct Upload API (Python, no Node in the image).

## Requirements

- **B1 Pages.** `/` overview: KPIs over the last 7 days (signals, alerts, urgent, opportunities, open
  claims), a per-domain table, a 14-day stacked chart of signals per day by domain (server-side SVG), and
  the newest digests from `digest_log` (id, status, signals, cost; never message text). `/signals/`: the
  top 50 signals of the last 7 days by `mart_x_signal_priority.priority`. `/claims/`: the open queue from
  `mart_claims_queue`. `404.html`.
- **B2 Denylist, fail closed.** Same `CLIENT_DENYLIST` and matcher as the digest (`digest.any_term`). A
  signal or claim with any field matching a term is left out and only counted ("N withheld"); a domain
  label that matches is shown as "(withheld)". With no denylist the build fails; nothing is deployed.
- **B3 Escaping and links.** Every value from the marts is escaped. Links are kept only for plain
  http(s) URLs; text is cut to 280 characters after screening. Thai text gets `lang="th"`.
- **B4 No script.** Same assets, tokens and CSS as the jobs site (minus `filter.js`), `_headers` with
  `script-src 'none'`, `noindex`. Private: Cloudflare Access app "X insights" with the reusable owner
  policy (as news/jobs/revenue).
- **B5 Deterministic.** Same rows + same `now` → same bytes. The deploy mark is
  `x-insights@<sha256 of the build, 16 hex>`; when the live deployment carries that mark, nothing is
  uploaded.
- **B6 Switch.** `X_LAKE_BI=off|build|deploy` (default `off`). `build` renders to a temp dir and logs
  counts; `deploy` also uploads (needs `CLOUDFLARE_ACCOUNT_ID` + `CLOUDFLARE_PAGES_TOKEN`). A failure is
  logged with its exception type and never changes the lake run's exit code or heartbeat (like the digest).
- **B7 Logs.** Counts, the deploy mark and the deployment id only, never row content or tokens.

## Operations

- Pages project `x-insights` (created 2026-11-10, Direct Upload, production branch `main`). Access app
  "X insights" covers `x-insights.pages.dev` and `*.x-insights.pages.dev` (every deployment URL), owner
  policy `29f0eea4…`, same as news/jobs/revenue. It was in place before the first upload.
- Railway `data-schedules/x-lakehouse` vars: `X_LAKE_BI=deploy`, `CLOUDFLARE_ACCOUNT_ID`,
  `CLOUDFLARE_PAGES_TOKEN` (account token with Pages Write only, Infisical `/x-lakehouse`, expires
  2027-02-28; re-mint before then). Deploy the image with `railway up` as before (push does not redeploy).
- "Is the site fresh?" → the Railway log line `event="bi"` (status `deployed`/`unchanged`, mark,
  deployment id), or the project's latest deployment commit message.

## Out of scope

Live (per-request) queries, filters that need JavaScript, the watchlist and query plan (config, not
insight), editing anything from the page.
