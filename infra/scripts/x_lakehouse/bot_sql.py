#!/usr/bin/env python3
"""Sheet-shaped rows -> SQL files that call ``x_intel.ingest()``. Standard library only, no network:
copy this one file to the bot box (e.g. /workspace/dq/scripts/bot_sql.py).

Each file holds ONE statement, at most 500 rows and 900 KB, for one Supabase connector
``execute_sql`` call (pass the file with ``"$file:/abs/path.sql"``):

    select x_intel.ingest('x_feed', $json$[{...}, ...]$json$::jsonb, 'run1113m');

The four private datasets (the owner's own account data) go to their own door instead, which
x_intel.ingest() refuses to replace:

    select x_intel_private.ingest('x_bookmarks', $json$[...]$json$::jsonb, 'run1113m');

Rows are dicts keyed by the Sheet header names, or lists in the Sheet's column order (the header is
built in below and must match the live Sheet; ``python -m x_lakehouse.bot_sql --check`` compares it
with the package contract). Values are sent as they are (text, numbers, booleans); the lake types
them exactly like the Sheet path does.

    python3 bot_sql.py x_feed run1113m rows.json out_dir/     # rows.json: a JSON list of rows
    -> out_dir/x_feed-run1113m-000.sql, -001.sql, ...       (prints the file paths)

A dollar-quote tag is picked that does not occur in the payload, so any ``$`` in X text is safe.
NUL characters (Postgres cannot store them in jsonb) are removed. Retrying a file is safe: ingest()
ignores rows it already holds.
"""

from __future__ import annotations

import json
import re
import secrets
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

MAX_ROWS = 500
MAX_BYTES = 900_000
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")

# Sheet column order per dataset (dataset name = snake_case of the tab). Kept in sync with
# schemas.py by the test suite.
HEADERS: dict[str, Sequence[str]] = {
    "x_feed": "collected_at requesting_chief watch_type watch_value post_id post_url author_handle posted_at "
              "text likes reposts replies views tag query_id score top".split(),
    "x_post_context": "post_id ref_type ref_post_id ref_post_url ref_author_handle ref_author_type ref_text "
                      "ref_likes ref_posted_at media_count media_types media_urls media_alt_text "
                      "video_duration_s video_views".split(),
    "x_trends_th": "snapshot_date snapshot_at rank trend_name trend_type post_count search_url "
                   "picked_for_posts note".split(),
    "x_trends_intl": "snapshot_date snapshot_at woeid country rank trend_name trend_type post_count "
                     "search_url note".split(),
    "x_topic_counts": "snapshot_date snapshot_at query_id topic buckets total_24h max_hour_24h "
                      "max_hour_start_ict median_hour_prior6d ratio spike drilldown_query_id note".split(),
    "x_account_snapshots": "snapshot_date snapshot_at list watchlist_row handle user_id followers_count "
                           "following_count tweet_count listed_count like_count media_count verified "
                           "protected account_status followers_delta_vs_prev note".split(),
    "alpha_feed": "pulled_at_ict source sub_source date title description metric_1 metric_2 extra url "
                  "time_bound_date tags requesting_chief notes".split(),
    "x_signals": "post_id collected_at query_id category subcategory signal_type author_class expanded_link "
                 "novelty opportunity_score verification_status suggested_action life_cat domain topic tags "
                 "tax_version cred_tier manip_flags content_label route alert_tier triage_version".split(),
    "claims": "claim_id claim first_seen_link spread_count verification_status verification_source "
              "first_post_id first_seen topic tags supporting_links contradicting_links last_checked "
              "priority queue_state checked_by next_check".split(),
    "x_watchlist": "requesting_chief type value purpose added_date last_used tag".split(),
    "x_query_plan": "query_id query requesting_chiefs tags watchlist_rows priority last_since_id last_run_at "
                    "runs calls_total posts_total new_posts_last avg_yield status calls_last_run yield_last6 "
                    "query_type lang_filter result_cap author_caps run_slot endpoint plan_ref max_pages".split(),
    "taxonomy_map": "section v3_topic_or_type book_domain_or_type note".split(),
    "x_analyses": "report_week analysis rank item post_id link topic book_domain geo value value_kind "
                  "source_tier metric note".split(),
    "x_triage_audit": "audit_week logged_at n label_agree route_agree label_confusions route_confusions "
                      "drift_vs_prev rule_version note".split(),
    # Bot-only datasets (no Sheet tab; contract docs/specs/x-lake-private-datasets.md).
    "x_own_account_daily": "snapshot_date snapshot_at handle user_id followers following posts listed "
                           "followers_delta posts_24h views_24h likes_24h replies_24h reposts_24h mentions_24h "
                           "source".split(),
    "x_own_mentions": "post_id own_handle kind collected_at posted_at author_handle author_id text "
                      "in_reply_to_post_id conversation_id likes reposts replies quotes views url filter_flags".split(),
    "x_bookmarks": "post_id folder_id folder_name first_seen_at last_seen_at removed_at author_handle author_id "
                   "posted_at text lang domain topic tags time_bound_date calendar_candidate likes reposts replies "
                   "views url filter_flags".split(),
    "x_home_timeline": "post_id collected_at posted_at author_handle author_id author_followers text lang likes "
                       "reposts replies quotes views age_min velocity eng_rate rank domain topic filter_flags "
                       "url".split(),
    "x_spaces": "space_id collected_at state title scheduled_start started_at lang creator_handle creator_id "
                "participant_count query domain topic calendar_candidate filter_flags url".split(),
    "x_communities": "community_id snapshot_date name description member_count created_at access query domain "
                     "topic member_delta filter_flags url".split(),
    "x_list_sources": "list_id name owner_handle member_count follower_count mapped_qid members_hash verified_at "
                      "status".split(),
}
# Datasets whose door is x_intel_private.ingest() (kept equal to schemas.PRIVATE_NAMES by the tests).
PRIVATE = frozenset({"x_own_account_daily", "x_own_mentions", "x_bookmarks", "x_home_timeline"})


def ingest_function(dataset: str) -> str:
    return "x_intel_private.ingest" if dataset in PRIVATE else "x_intel.ingest"


class BotSqlError(ValueError):
    pass


def _clean(v: Any) -> Any:
    if isinstance(v, str):
        return v.replace("\x00", "")
    if v is None or isinstance(v, (bool, int, float)):
        return v
    raise BotSqlError(f"cell value of type {type(v).__name__}: send text, numbers or booleans")


def as_dicts(dataset: str, rows: Iterable[Sequence[Any] | dict[str, Any]]) -> list[dict[str, Any]]:
    if dataset not in HEADERS:
        raise BotSqlError(f"unknown dataset {dataset!r}; known: {sorted(HEADERS)}")
    header = HEADERS[dataset]
    out = []
    for i, row in enumerate(rows, 1):
        if isinstance(row, dict):
            rec = {str(k): _clean(v) for k, v in row.items() if k}
        else:
            vals = list(row)
            if len(vals) > len(header):
                raise BotSqlError(f"row {i}: {len(vals)} cells, the {dataset} header has {len(header)}")
            rec = {h: _clean(v) for h, v in zip(header, vals)}
        out.append(rec)
    return out


def _tag(payload: str) -> str:
    tag = "$json$"
    while tag in payload:
        tag = f"$json_{secrets.token_hex(4)}$"
    return tag


def statement(dataset: str, rows: list[dict[str, Any]], run_id: str) -> str:
    if not _NAME.match(run_id):
        raise BotSqlError("run_id must match [A-Za-z0-9_.:-], 1-64 chars")
    if dataset not in HEADERS:
        raise BotSqlError(f"unknown dataset {dataset!r}")
    payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    tag = _tag(payload)
    return f"select {ingest_function(dataset)}('{dataset}', {tag}{payload}{tag}::jsonb, '{run_id}');\n"


def chunks(dataset: str, rows: list[dict[str, Any]], run_id: str, *, max_rows: int = MAX_ROWS,
           max_bytes: int = MAX_BYTES) -> list[str]:
    """Statements of at most ``max_rows`` rows and ``max_bytes`` UTF-8 bytes each."""
    out: list[str] = []
    cur: list[dict[str, Any]] = []
    size = len(statement(dataset, [], run_id).encode())
    for rec in rows:
        rec_size = len(json.dumps(rec, ensure_ascii=False, separators=(",", ":")).encode()) + 1
        if size + rec_size + 16 > max_bytes and not cur:
            raise BotSqlError(f"one {dataset} row is {rec_size} bytes, over the {max_bytes} limit")
        if cur and (len(cur) >= max_rows or size + rec_size + 16 > max_bytes):
            out.append(statement(dataset, cur, run_id))
            cur, size = [], len(statement(dataset, [], run_id).encode())
        cur.append(rec)
        size += rec_size
    if cur:
        out.append(statement(dataset, cur, run_id))
    for s in out:  # the size estimate is conservative; never hand the connector an oversize file
        if len(s.encode()) > max_bytes:
            raise BotSqlError("statement over the size limit; lower max_rows")
    return out


def write_files(dataset: str, rows: Iterable[Sequence[Any] | dict[str, Any]], run_id: str,
                out_dir: str | Path) -> list[Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for i, sql in enumerate(chunks(dataset, as_dicts(dataset, rows), run_id)):
        p = out / f"{dataset}-{run_id}-{i:03d}.sql"
        p.write_text(sql, encoding="utf-8")
        paths.append(p)
    return paths


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--check"]:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from x_lakehouse.schemas import PRIVATE_NAMES, SHEET_DATASETS, NEW_DATASETS

        known = {**SHEET_DATASETS, **NEW_DATASETS}
        bad = [n for n, d in known.items() if list(HEADERS.get(n, [])) != d.names] + [n for n in HEADERS if n not in known]
        bad += ["PRIVATE"] if PRIVATE != set(PRIVATE_NAMES) else []
        print("ok" if not bad else f"header drift: {bad}")
        return 1 if bad else 0
    if len(args) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    dataset, run_id, rows_file, out_dir = args
    rows = json.loads(Path(rows_file).read_text(encoding="utf-8"))
    for p in write_files(dataset, rows, run_id, out_dir):
        print(p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
