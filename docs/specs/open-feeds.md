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

- **Registry** `open-feeds.yaml`: 881 rows on 2026-11-18, 853 enabled, in 82 categories
  (RSS/Atom 813, keyless JSON APIs 35, HTML scrapes 5). Seed:
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
  `BLOCKED_HOSTS` in the collector refuses an enabled row on those hosts, so an OPML import cannot
  slip one in (the Product Hunt feed of the OPML list is kept disabled with a note).
- **Collector** `infra/scripts/data_lake/open_feeds.py`, subcommands `run` (default), `check`,
  `probe`, `report`, `import-opml`.

## Run

- Hourly at :25 UTC from the jobs queue (`platform-jobs-worker` job `open-feeds`), workflow
  `.github/workflows/open-feeds.yml` on a GitHub-hosted runner (public repository, $0).
  `workflow_dispatch` with `force` fetches every enabled feed.
- A feed is fetched when its `every_hours` (1, 2, 3, 6, 12, 24 or 168) is due (10 min slack).
- Politeness: hosts run in parallel (12 workers), feeds of one host run one after another with a 1 s
  pause (3 s for arXiv, as its terms ask); a descriptive User-Agent; conditional GET
  (`If-None-Match` / `If-Modified-Since`, sent only to the URL that issued them); one retry after 3 s
  for a transient failure (network or DNS error, 429, 5xx); 25 s I/O timeout plus a 60 s wall-clock
  deadline per response checked per chunk (a slow-drip body cannot hold the run); 8 MB body cap.
- Run budget: no feed starts after 15 minutes and no retry is made past it; the fetch phase returns
  at 15 min + 85 s whatever is still in flight (workers are daemon threads, a stalled connection is
  abandoned), so everything already fetched is written well inside the 25 min workflow cap. Feeds not
  finished are left out (`feeds_deferred`), keep their state and stay due, and the next run starts
  with the never-fetched and longest-waiting feeds first; a feed still in flight at the hard stop is
  recorded as `abandoned at the run deadline` so it cannot jump the queue every run. The failure
  ratio counts attempted feeds, and a run where feeds were due but none finished exits 3.
- A 429/503 with a `Retry-After` (seconds or an HTTP date) longer than our 3 s pause is not retried
  in the same run.
- `per_day: true` (GitHub trending, Hacker News) stores one observation per UTC day, so a repo that
  trends again or today's points and comment count land as new rows.
- Safety: redirects are followed by the collector one hop at a time (a 3xx body is never read), and
  every hop, robots.txt included, must be https and off `BLOCKED_HOSTS` (hostnames normalised). The
  HTTP client's network backend does the only DNS lookup, refuses the connection unless every address
  is public (global), and connects to a checked address while TLS still verifies the original
  hostname (SNI), so DNS rebinding cannot slip in a loopback, private or cloud-metadata address; the
  connected peer is checked again before the body is read, and no environment proxy is used; TLS
  verification is never turned off;
  XML with an `<!ENTITY` declaration is refused (no entity bombs or external entities) and feedparser
  only ever gets a byte stream (never a string it could read as a path or URL); `kind: html`
  checks robots.txt for our User-Agent on every run and on every redirect hop (redirects are followed
  one at a time), and a host whose robots.txt cannot be read (403, timeout) is skipped (`robots-denied`).
- A 200 answer that is not RSS/Atom/RDF (an HTML maintenance page) or broken XML with no entries
  (a truncated body) is an error, so it cannot reset the feed's dedupe list; a valid feed with no
  entries is `ok`. The HTTP status of such an answer is kept in `feed_runs`.
- One broken feed never stops a run: parse errors and text that is not valid UTF-8 are recorded in
  `feed_runs` for that feed only.

## Contract

Two Bronze datasets under `domain=open-data`, `source=open-feeds`, `privacy_class=public`.

`feed_items` (one row per item not seen before):

| Field | Meaning |
|---|---|
| `id` | `<feed_id>:<sha256(guid)[:16]>` (the `source_record_id`; guid = RSS guid/Atom id, else link, else title; JSON: the `id` field(s), plus a hash of the `changes` fields when set; snapshot: content hash). A JSON row missing an id field is skipped and counted (`items_skipped`), never merged with another row |
| `feed_id`, `category`, `subcategory`, `kind`, `lang` | from the registry row |
| `title` | tags stripped, whitespace folded, ≤ 500 chars |
| `link` | absolute http(s) URL or null |
| `published_at` | UTC ISO 8601 from the feed, or null |
| `fetched_at` | UTC run time (`event_time`) |
| `data` | JSON string: RSS `tags` (+ `summary` ≤ 2000 chars only for `keep: full`, used for open licences such as arXiv metadata); JSON the `keep` fields (or the whole `payload` for snapshots); HTML the `fields` |

Personal data: author fields are never read; publisher RSS keeps headline + link only (as news-marts).
Snapshots (`json.snapshot: true`, for example exchange rates or a weather forecast) store the whole
document, and only when its content hash changes.

Revisions: a JSON row whose values can change after publication (World Bank back-fills a `null` or
revises a year; USGS revises magnitude, place or alert and bumps `properties.updated`; CISA changes
a KEV entry's due date or ransomware flag) lists those
fields under `json.changes`; the revised row then lands as a new item, and
the latest `fetched_at` per indicator and date is the current value.

`feed_runs` (one row per feed fetched): `id` (`<feed_id>@<fetched_at>`), `feed_id`, `category`,
`fetched_at`, `status` (`ok` | `not-modified` | `error` | `robots-denied`), `http_status` (also on
4xx/5xx errors; null only without an HTTP answer), `error` (short reason, no body), `items_seen`,
`items_new`, `items_skipped`, `items_truncated` (past the row's `max_items`: default 300, which keeps the newest entries and drops only the archive of podcast-style feeds; arXiv 1000, CISA KEV 5000),
`duration_ms`, `bytes`, `consecutive_errors`.

Landing (ADR-0001 exception, owner request 2026-11-18): the landing object is the normalised JSONL of
the new items, not the publisher's response. Raw RSS bodies carry article summaries and author names
that the headline-only rule must not keep (copyright and personal data), so the raw response is not
stored; a parser fix is replayed by re-fetching (feeds keep their recent items) rather than from landing.

## State and idempotence

- The lake is write-once, so the dedupe state (per feed: hashes of the items it shows now, ETag,
  Last-Modified, last fetch, error streak) is one gzip JSON file (~0.5 MB) kept in the GitHub Actions
  cache (`open-feeds-state-<run_id>-<run_attempt>`, restored by prefix, saved after every run
  including partial failures). `run --only` fetches the chosen feeds but keeps the state of the
  others; an unknown id is an error. Locally it defaults to `data/runtime/open-feeds/state.json.gz` (`--state-file`,
  `OPEN_FEEDS_STATE`).
- The state names the lake it belongs to (a hash of the lake URI, plus the S3 endpoint when one is
  set; neither is stored; states written with the earlier URI-only hash are accepted once);
  pointed at another lake it starts empty, so a test lake's dedupe never hides items from the real one.
- Validators carry the fingerprint of how the feed is read (`PARSER_VERSION`, URL, kind, parser spec,
  keep, max_items); after a registry or parser change they are not sent and the items the feed shows
  are re-sent with the new normalisation (replay); report keeps the newest row per id. Truncation at
  `max_items` keeps the newest items when every item has a publish time. An html selector that
  matches nothing is an error (the page layout changed).
- Bronze batches are split at 24 MB of JSONL (the lake refuses objects over 64 MiB), so a cache miss
  that brings ~45 k items at once still commits.
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
- Exit `3`: more than half of the due feeds failed, or more than half of the enabled feeds failed on
  their latest fetch (so a re-run with nothing due cannot report ok while the feeds are broken).
- Exit `2`: invalid registry, lake error, empty or local lake URI with `--require-cloud`.
- Heartbeat `open-feeds` goes to `monitor.bookchaowalit.com` (window 3 h; the monitor adds its usual
  1 h grace to every heartbeat, so a silent job alerts after about 4 h, as job-radar does). `ok:false`
  alerts at once.

## Analysis

`report` reads Bronze with DuckDB (local path or the R2 lake with the same env as the workflow):

```bash
.venv/bin/python infra/scripts/data_lake/open_feeds.py report --days 7 --search "AI" --limit 20
```

It prints items by category and by feed, the feeds failing on their latest run (with HTTP status), and
the newest titles matching `--search` (one row per item id). It reads only the Parquet parts that a
commit manifest names (ADR-0001), so a part left by an interrupted run is never counted. The manifest
prefix is listed once per report (one recursive copy); about 50 manifests a day accumulate, so after a
year or more a per-day manifest index is the next step if `report` gets slow. Any other question is SQL on
`bronze/domain=open-data/dataset=feed_items/**/*.parquet` (`json_extract_string(payload_json, '$.title')`).

## Adding a source

1. Add a row (see the field list at the top of `open-feeds.yaml`); for a new OPML list:
   `open_feeds.py import-opml file.opml --category <topic>` prints valid rows (ids from the URL when a
   title has no Latin letters, blocked hosts dropped).
2. `open_feeds.py check` (offline, also a test) and `open_feeds.py probe --only <id>` (live).
3. Keep the guardrails: free, keyless, public; read the source's terms; `keep: full` only for open
   licences; a new html scrape only where robots.txt and the terms allow it.

## Tests

`infra/tests/data_lake/open_feeds_test.py` (offline, 58 tests): registry validation (https only,
unknown fields, required json/html specs, duplicate ids, blocked hosts, max_items) and the shipped
registry (≥ 500 enabled, no blocked host enabled, every disabled row has a note); HTML page is not a
feed, empty feed is ok, links resolve against the final URL; JSON rows without id skipped, `changes`
versions, bracket paths, truncation counted, lone surrogates isolated; robots on every redirect hop;
validators tied to the final URL after a redirect; redirect to a blocked host never requested; HTTP
status on errors and parse failures; truncated XML is an error; run budget defers unstarted feeds;
batch splitting; state bound to one lake; `--only` keeps other state; report with no items and
report ignoring parts without a manifest; USGS revisions; RSS, Atom, JSON (dot paths, composite ids, link templates, epoch
times, snapshots) and HTML parsing; author never kept; entity bomb refused; due logic; dedupe across
runs; conditional GET and 304; one retry for transient errors only; robots allow/deny/unknown;
capture into a local lake (manifests, state, nothing due → no write, lost state → re-send); dry run
writes nothing; the DuckDB report dedupes by id; exit codes 0/2/3.
