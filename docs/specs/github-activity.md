---
title: "SPEC — GitHub activity into Bronze"
status: approved
created: 2026-11-12
owner: bookchaowalit
related: [../../repository-catalog/registries/source-coverage.yaml, sales-marts.md, news-marts.md]
---

# SPEC — GitHub activity into Bronze

First `ingest-now` source from the source coverage registry
(`repository-catalog/registries/source-coverage.yaml`, batch 1). It covers the `github` flow in
solo-empire `data-sources.yaml`. Until now that flow was `not-captured` and only read on demand
with `gh`.

## Scope

- Public data only, read with the workflow's own `GITHUB_TOKEN` (1,000 REST requests/hour per
  repository). No new secret. Private repositories need a fine-grained PAT later; that is an
  owner action in the registry row, not part of this spec.
- Two Bronze datasets under `domain=engineering`, `source=github`:
  - `github_repos`: one row per public repository the owner owns, a daily snapshot.
  - `github_events`: the owner's public events (GitHub keeps the last 90 days, at most 300).
- Daily at 03:10 UTC from the jobs queue (`platform-jobs-worker` job `github-activity`), plus manual
  `workflow_dispatch`.

## Contract

| Dataset | `source_record_id` | `event_time` | Fields |
|---|---|---|---|
| `github_repos` | repository id | `pushed_at` | `id, full_name, name, fork, archived, language, topics, stargazers_count, forks_count, open_issues_count, size_kb, default_branch, created_at, updated_at, pushed_at, html_url` |
| `github_events` | event id | `created_at` | `id, type, created_at, repo, actor, action, ref, ref_type, push_size, number` |

- **PII minimisation**: commit messages, author names and e-mails in push payloads, and issue or
  comment bodies are dropped before landing. Landing holds the same trimmed JSONL as Bronze, not the
  raw API body. `privacy_class=public`, `retention_class=operational`.
- **Idempotence**: batches are content-addressed, so an unchanged snapshot writes nothing. Events
  overlap from one day to the next. Bronze keeps each batch as it came, and Silver removes the
  duplicates by `source_record_id`.
- **Bounds**: at most 10 pages of 100 repositories and 3 pages of 100 events. A run makes about
  7 requests and about 6 R2 Class A writes, far below the ADR-0013 §7 trigger (300 k/month).

## Failure and heartbeat

- Exit `0` means ok. Exit `2` means an API, storage or contract error; only the exception type goes
  to the public log.
- The workflow posts `monitor.bookchaowalit.com/heartbeat/github-activity` with `ok` and `failed`.
  The uptime Worker alerts when a run reports a failure or when no run arrives for 26 h.

## Tests

`infra/tests/data_lake/github_activity_test.py` runs offline against a local lake and a fake HTTP
client. It covers trimming (no e-mails or messages reach Bronze), pagination bounds, a re-run
writing nothing, and an API error giving exit 2.
