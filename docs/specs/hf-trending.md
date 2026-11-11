---
title: "SPEC — Hugging Face trending into Bronze"
status: approved
created: 2026-11-12
owner: bookchaowalit
related: [../../repository-catalog/registries/source-coverage.yaml, github-activity.md]
---

# SPEC — Hugging Face trending into Bronze

This is the `ingest-now` source of source coverage batch 3 (`huggingface` row). It is an AI-market
signal: what the open model ecosystem is building and using each day. No credential is needed.

## Scope

- Public Hub API (`https://huggingface.co/api/{models,datasets,spaces}?sort=trendingScore&limit=100`).
  It runs without a token. The anonymous tier allows 500 calls per 5 minutes per IP, and a run makes 3.
  robots.txt is `Allow: /`.
- Three Bronze datasets under `domain=market-intelligence`, `source=huggingface`:
  `hf_trending_models`, `hf_trending_datasets` and `hf_trending_spaces`.
- The run is daily at 03:40 UTC from the jobs queue (`platform-jobs-worker` job `hf-trending`).
  `workflow_dispatch` also works.

## Contract

| Field | Meaning |
|---|---|
| `id` | `<repo_id>@<snapshot_date>` (the `source_record_id`) |
| `repo_id`, `kind`, `author` | repository identity (`kind` is `model`, `dataset` or `space`) |
| `rank` | position in that day's trending list, starting at 1 |
| `snapshot_date` | the UTC date of the run (`event_time`) |
| `trending_score`, `likes`, `downloads` | numbers at snapshot time (`downloads` is empty for spaces) |
| `pipeline_tag`, `library_name`, `sdk`, `gated` | descriptive metadata when the Hub returns it |
| `tags` | the first 20 tags |
| `created_at`, `last_modified` | Hub timestamps |

- Free text such as dataset descriptions and card data is dropped. Private items are dropped.
  Usernames and organisation names are public. `privacy_class=public`.
- **Idempotence**: every day is its own snapshot. A same-day re-run with the same numbers writes
  nothing, because the batch is content-addressed.
- **Cost**: about 3 objects per run (6 R2 Class A writes) and well under 1 MB/day. Both are far
  below the ADR-0013 §7 triggers.

## Failure and heartbeat

- Exit `0` means ok. Exit `2` covers an API or storage error, an empty or local lake URI with
  `--require-cloud`, and a bad `--snapshot-date`. The public log shows only the exception type.
- Heartbeat `hf-trending` goes to `monitor.bookchaowalit.com`, which alerts after 26 h without a run.

## Tests

`infra/tests/data_lake/hf_trending_test.py` checks four things offline:
- trimming and ranking
- the host pin
- that a same-day re-run writes nothing while a new day writes
- the exit codes
