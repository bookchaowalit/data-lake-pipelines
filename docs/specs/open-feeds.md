---
title: "SPEC — Open feeds: every free RSS feed, keyless API and allowed page into Bronze"
status: approved
created: 2026-11-18
owner: bookchaowalit
related: [../../repository-catalog/registries/open-feeds.yaml, ../../repository-catalog/registries/source-coverage.yaml, wiki-pageviews.md, hf-trending.md]
---

# SPEC — Open feeds into Bronze

The owner asked on 2026-11-18 for one system that collects every free source that can be pulled
(RSS feeds, public APIs, scraping) so there is data to analyse at no cost. Until now each source
needed its own script, spec and workflow (wiki-pageviews, hf-trending, github-activity), so only a
handful were ingested. Open feeds is one collector driven by one registry: adding a source is adding
a row to `repository-catalog/registries/open-feeds.yaml`.

## Scope

- **Registry** `open-feeds.yaml`: 881 rows on 2026-11-18, 854 enabled, in 82 categories
  (RSS/Atom 814, keyless JSON APIs 35, HTML scrapes 5). Seed:
  - hand-picked: Thai news (Bangkok Post, Matichon, Khaosod, Thairath, Prachatai, Blognone, The
    Standard, …), Google News TH/US by topic, Google Trends daily RSS (TH, US, GB, JP, SG), world
    news (BBC, Guardian, NYT, NPR, DW, Al Jazeera, Nikkei Asia, …), tech and AI blogs, arXiv RSS
    (12 categories, metadata only), security (CISA KEV + advisories, Krebs, …), earth and space
    (USGS earthquakes, NASA EONET, NOAA SWPC, launches), weather (met.no Bangkok, Chiang Mai,
    Phuket), economy (ECB rates via Frankfurter, World Bank Thailand indicators, US Treasury, central
    bank press), government (Federal Register, GOV.UK, data.go.th), Hacker News (titles, points),
    npm / crates / MCP registry, GitHub releases of 30 key projects, GitHub trending, Mastodon
    trends, and the status pages of the platforms we run on;
  - the CC0 OPML lists of `github.com/plenaryapp/awesome-rss-feeds` (recommended + countries).
  - Every enabled row answered with items on the 2026-11-18 probe. Curated rows that failed stay in
    the registry with `enabled: false` and a `note` (for example Air4Thai: incomplete TLS chain).
- **Not added**: sources whose `source-coverage.yaml` verdict is `skip` or `blocked-legal`, or that
  need an owner decision first (Reddit, CoinGecko, Product Hunt, LinkedIn, Stack Exchange dumps,
  Open-Meteo free tier, property and booking sites), anything behind a login, key, paywall or CAPTCHA.
- **Collector** `infra/scripts/data_lake/open_feeds.py`, subcommands `run` (default), `check`,
  `probe`, `report`, `import-opml`.

## Run

- Hourly at :25 UTC from the jobs queue (`platform-jobs-worker` job `open-feeds`), workflow
  `.github/workflows/open-feeds.yml` on a GitHub-hosted runner (public repository, $0).
  `workflow_dispatch` with `force` fetches every enabled feed.
- A feed is fetched when its `every_hours` (1, 2, 3, 6, 12, 24 or 168) is due (10 min slack).
- Politeness: hosts run in parallel (24 workers), feeds of one host run one after another with a 1 s
  pause; a descriptive User-Agent; conditional GET (`If-None-Match` / `If-Modified-Since`); one retry
  after 3 s for a transient failure (network error, 429, 5xx); 25 s timeout; 8 MB body cap.
- Safety: https only, every redirect hop must stay https; TLS verification is never turned off;
  XML with an `<!ENTITY` declaration is refused (no entity bombs or external entities); `kind: html`
  checks robots.txt for our User-Agent on every run, and a host whose robots.txt cannot be read
  (403, timeout) is skipped (`robots-denied`).
- One broken feed never stops a run: its error is recorded in `feed_runs`.

## Contract

Two Bronze datasets under `domain=open-data`, `source=open-feeds`, `privacy_class=public`.

`feed_items` (one row per item not seen before):

| Field | Meaning |
|---|---|
| `id` | `<feed_id>:<sha256(guid)[:16]>` (the `source_record_id`; guid = RSS guid/Atom id, else link, else title; JSON: the `id` field(s); snapshot: content hash) |
| `feed_id`, `category`, `subcategory`, `kind`, `lang` | from the registry row |
| `title` | tags stripped, whitespace folded, ≤ 500 chars |
| `link` | absolute http(s) URL or null |
| `published_at` | UTC ISO 8601 from the feed, or null |
| `fetched_at` | UTC run time (`event_time`) |
| `data` | JSON string: RSS `tags` (+ `summary` ≤ 2000 chars only for `keep: full`, used for open licences such as arXiv metadata); JSON the `keep` fields (or the whole `payload` for snapshots); HTML the `fields` |

Personal data: author fields are never read; publisher RSS keeps headline + link only (as news-marts).
Snapshots (`json.snapshot: true`, for example exchange rates or a weather forecast) store the whole
document, and only when its content hash changes.

`feed_runs` (one row per feed fetched): `id` (`<feed_id>@<fetched_at>`), `feed_id`, `category`,
`fetched_at`, `status` (`ok` | `not-modified` | `error` | `robots-denied`), `http_status`, `error`
(short reason, no body), `items_seen`, `items_new`, `duration_ms`, `bytes`, `consecutive_errors`.

## State and idempotence

- The lake is write-once, so the dedupe state (per feed: hashes of the items it shows now, ETag,
  Last-Modified, last fetch, error streak) is one gzip JSON file (~0.5 MB) kept in the GitHub Actions
  cache (`open-feeds-state-<run_id>`, restored by prefix, saved after every run including partial
  failures). Locally it defaults to `data/runtime/open-feeds/state.json.gz` (`--state-file`,
  `OPEN_FEEDS_STATE`).
- A lost state (cache evicted after 7 days without runs) only re-sends what the feeds still show:
  Bronze gets extra rows with the same `id`, and Silver/`report` keep one row per `id`.
- The state is written after Bronze, so a failed state write can only duplicate, never lose items.

## Cost

- Per run: 2 ingests (landing JSONL + Bronze Parquet + manifests ≈ 8 R2 Class A writes) whatever the
  number of feeds; about 6 k Class A per month (free tier 1 M).
- Volume: the first run took 44,457 items (21 MB JSONL + 6.5 MB Parquet); an hourly run then adds a
  few hundred to a few thousand items (second run 15 minutes later: 571 new, 382 feeds `304`).
  Expect roughly 1–3 GB a year. The ADR-0013 trigger (storage > 10 GB) is watched by the posture
  monitor.
- Runtime ~3 minutes on the runner; Actions minutes are free for a public repository.

## Failure and heartbeat

- Exit `0`: the run completed (some feeds may have failed; see `failed_feeds` and `feed_runs`).
- Exit `3`: more than half of the due feeds failed (runner or network problem).
- Exit `2`: invalid registry, lake error, empty or local lake URI with `--require-cloud`.
- Heartbeat `open-feeds` goes to `monitor.bookchaowalit.com`, which alerts after 3 h without an ok run.

## Analysis

`report` reads Bronze with DuckDB (local path or the R2 lake with the same env as the workflow):

```bash
.venv/bin/python infra/scripts/data_lake/open_feeds.py report --days 7 --search "AI" --limit 20
```

It prints items by category and by feed, the feeds failing on their latest run, and the newest titles
matching `--search` (one row per item id). Any other question is SQL on
`bronze/domain=open-data/dataset=feed_items/**/*.parquet` (`json_extract_string(payload_json, '$.title')`).

## Adding a source

1. Add a row (see the field list at the top of `open-feeds.yaml`); for a new OPML list:
   `open_feeds.py import-opml file.opml --category <topic>` prints rows.
2. `open_feeds.py check` (offline, also a test) and `open_feeds.py probe --only <id>` (live).
3. Keep the guardrails: free, keyless, public; read the source's terms; `keep: full` only for open
   licences; a new html scrape only where robots.txt and the terms allow it.

## Tests

`infra/tests/data_lake/open_feeds_test.py` (offline): registry validation (https only, unknown
fields, required json/html specs, duplicate ids) and the shipped registry (≥ 500 enabled, no Reddit,
every disabled row has a note); RSS, Atom, JSON (dot paths, composite ids, link templates, epoch
times, snapshots) and HTML parsing; author never kept; entity bomb refused; due logic; dedupe across
runs; conditional GET and 304; one retry for transient errors only; robots allow/deny/unknown;
capture into a local lake (manifests, state, nothing due → no write, lost state → re-send); dry run
writes nothing; the DuckDB report dedupes by id; exit codes 0/2/3.
