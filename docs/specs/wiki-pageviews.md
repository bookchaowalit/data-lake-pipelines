---
title: "SPEC — Wikipedia top articles into Bronze"
status: approved
created: 2026-11-12
owner: bookchaowalit
related: [../../repository-catalog/registries/source-coverage.yaml, hf-trending.md]
---

# SPEC — Wikipedia top articles into Bronze

This is the `ingest-now` source of source coverage batch 4 (`wikimedia-pageviews` row). It is a daily
signal of what Thai and English readers look up.

## Scope

- Wikimedia Analytics API `pageviews/top/{project}/all-access/{yyyy}/{mm}/{dd}` for `th.wikipedia`
  and `en.wikipedia`.
- The data is CC0 1.0 and needs no credential. The access policy asks for a descriptive User-Agent
  and one request at a time; the job sends 2 requests, 1 s apart.
- Each run keeps the top 200 articles per project in one Bronze dataset, `wiki_top_articles`,
  under `domain=market-intelligence`, `source=wikimedia`.
- A day's ranking is published the next day, so the default date is two days back (UTC).
- The run is daily at 03:50 UTC from the jobs queue (`platform-jobs-worker` job `wiki-pageviews`).
  `workflow_dispatch` also works.

## Contract

| Field | Meaning |
|---|---|
| `id` | `<project>:<article>@<view_date>` (the `source_record_id`) |
| `project`, `article` | the wiki and the page title (underscores, as Wikimedia returns them) |
| `namespace_page` | true for titles with a colon (`Special:Search` and similar); they are kept but flagged so Silver can drop them |
| `rank`, `views` | the position and the all-access views that day |
| `view_date` | the day being ranked (`event_time`) |

- Counts are aggregates, so there is no personal data. Titles of articles about living people are
  public encyclopaedia titles. `privacy_class=public`, and the licence `CC0-1.0` is stored in the
  metadata.
- **Idempotence**: there is one ranking per `view_date`. A re-run for the same day writes nothing.
- **Cost**: 1 object and 3 R2 Class A writes per run, under 100 KB.

## Failure and heartbeat

- Exit `0` means ok. Exit `2` covers an API error, an empty ranking or an unexpected response shape,
  an empty or local lake URI with `--require-cloud`, and a bad `--date`. The public log shows only
  the exception type.
- Heartbeat `wiki-pageviews` goes to `monitor.bookchaowalit.com`, which alerts after 26 h without a run.

## Tests

`infra/tests/data_lake/wiki_pageviews_test.py` checks these offline:
- the top-N per project and the URL shape
- the namespace flag
- the error on an empty ranking or a bad response shape
- the host pin
- that a same-day re-run writes nothing
- the exit codes
