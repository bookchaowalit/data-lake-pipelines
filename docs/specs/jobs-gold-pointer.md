# Spec: a "published" pointer for each jobs Gold run

Status: accepted (2026-11-05). Standard: ADR-0004 (spec, then failing test, then code).
Code: `infra/scripts/data_lake/jobs_marts.py`. Tests: `infra/tests/data_lake/jobs_marts_test.py`.
Consumer: `book-job-data-api` (career-job-market-data `src/book_job_data/store.py`).

## Problem

`book-job-data-api` builds its `job_postings` latest view by reading every Bronze part ever
written (one Parquet file per capture batch), parsing `payload_json` in Python and keeping the
newest row per record. One load takes ~140 s on R2 and grows with every batch. The Silver part
that `jobs_marts.py` publishes every 2 h already is that view (newest row per record, same
`payload_json`, quarantined and excluded rows removed), in one file.

Readers cannot find the newest Silver part: Gold manifests are keyed by `gold_run_id`, a content
hash, and the object store is write-once, so a mutable `latest.json` is not allowed.

## Behaviour

1. After the Gold manifest is written, `run_jobs_marts` writes one pointer object:
   `control/pointers/gold/source=book-job-data/domain=jobs/published_at=<YYYYMMDDTHHMMSSZ>-<gold_run_id>.json`
   where the timestamp is the run's `now` in UTC. Keys sort lexically in publish order, so the
   newest pointer is the greatest key under the prefix.
2. The pointer is canonical JSON with `pointer_version` (`"1"`), `layer` (`"gold"`),
   `source`, `domain`, `published_at` (ISO 8601 UTC, `Z`), `gold_run_id`, `manifest_key`,
   `silver_run_id`, `silver_key` and `silver_record_count`.
3. A pointer is written on every successful run, including a stale run and a rerun whose Gold
   already existed (so readers see that the snapshot was re-confirmed). A rerun with the same
   `now` writes the identical object, which the store treats as a no-op.
4. A run that fails before Gold (quality gate, dbt failure) writes no pointer.
5. The result dict gains `pointer_key` and `pointer_existed`.
6. (2026-11-05) After Gold, the run also publishes Silver for `job_matches` and `job_leads` with
   the `jobs_envelope` normalizer: newest row per `source_record_id`, the common Silver columns
   (`payload_json` kept verbatim), only `source_record_id` required, `offline_fixture` excluded,
   same `max_invalid_ratio`. These are best-effort: a failure (empty Bronze, quality gate) is
   reported in the result under `envelope_silver.<dataset>.error` and never blocks Gold or the
   pointer.
7. The pointer gains `datasets`: `{<dataset>: {"silver_key", "silver_run_id", "record_count"}}`
   for `job_postings` and every envelope dataset that succeeded; a failed dataset is absent.
   The top-level `silver_*` fields stay (they describe `job_postings`), so it is still
   `pointer_version` `"1"`.

## Reader contract

A reader lists `control/pointers/gold/source=book-job-data/domain=jobs/`, takes the greatest key
ending in `.json`, checks `pointer_version == "1"` and `layer == "gold"`, then reads
`silver_key` (or `datasets.<dataset>.silver_key`). If any step fails, or the dataset is absent, the reader must fall back to its own Bronze read, not serve
empty data.

## Out of scope

Silver for the `_history` datasets; pointers for other domains; pruning old pointers (each is a few hundred bytes, ~12 a day).
