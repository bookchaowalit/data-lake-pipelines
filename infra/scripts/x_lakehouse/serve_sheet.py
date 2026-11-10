#!/usr/bin/env python3
"""Rebuild the small serving view in the Sheet from the curated Postgres tables.

Writes ONLY three tabs it owns: "Serve · X Signals 7d" (X Signals collected in the last 7 days),
"Serve · Open Claims" (claims whose queue_state is not closed) and "Serve · X Query Plan" (a
read-only mirror of x_intel.x_query_plan once the plan is edited in Postgres). It creates a tab with its header
once, then each refresh clears and rewrites rows 2+ only. It never touches an existing data tab,
"AI Summaries", "AI Summaries Latest", any "Chief *" tab, or any header row after creation.

Two more NEW tabs exist but are OFF by default (``X_LAKE_SERVE_SMALL_TABS=on`` turns them on; the owner
decides): "Serve · Alpha Feed 7d" from x_intel.alpha_feed and "Serve · X Trends TH 3d" from
x_intel.x_trends_th. Turning the switch off again only stops writing them: tabs and rows stay.

Never served, whatever the configuration: the four private datasets (x_own_account_daily,
x_own_mentions, x_bookmarks, x_home_timeline) and x_spaces / x_communities / x_list_sources
(``schemas.SERVE_DENY``); :func:`source_for` refuses them before any query.

DRY RUN BY DEFAULT: prints what it would write. ``--execute`` writes.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

import httpx  # noqa: E402

from . import curated as cur  # noqa: E402
from .coerce import TZ  # noqa: E402
from .config import Config  # noqa: E402
from .schemas import CURATED, DATASETS, OPTIONAL_SERVE_TABS, PG_SCHEMA, SERVE_DENY, SERVE_TABS  # noqa: E402
from .sheet import SheetWriter, assert_writable  # noqa: E402

DAYS = 7
CLOSED_STATES = ("closed", "done", "resolved", "archived", "dismissed")
SIGNAL_COLUMNS = ["post_id", "collected_at", "author_handle", "post_url", "posted_at", "domain", "topic",
                  "category", "subcategory", "signal_type", "cred_tier", "alert_tier", "route",
                  "content_label", "novelty", "opportunity_score", "verification_status",
                  "suggested_action", "tags", "manip_flags", "likes", "views", "post_text"]
CLAIM_COLUMNS = [c for c, _ in CURATED["claims"].data_columns]
QUERY_PLAN_COLUMNS = [c for c, _ in CURATED["x_query_plan"].data_columns]
SMALL_TAB_DAYS = {"alpha_feed": (7, "pulled_at_ict", "pulled_at_ict desc, source, sub_source, url, title"),
                  "x_trends_th": (3, "snapshot_at", "snapshot_at desc, rank, trend_name")}


class ServeError(RuntimeError):
    pass


def source_for(dataset: str) -> tuple[str, str]:
    """(tab, table) a dataset may be served from. Refuses private and denied datasets before any read."""
    ds = DATASETS.get(dataset)
    if dataset in SERVE_DENY or (ds is not None and ds.private):
        raise ServeError(f"refusing to serve {dataset!r}: never mirrored to the Sheet")
    if dataset in SERVE_TABS:
        return SERVE_TABS[dataset], f"{PG_SCHEMA}.{dataset}"
    if dataset in OPTIONAL_SERVE_TABS and dataset in CURATED:
        return OPTIONAL_SERVE_TABS[dataset], CURATED[dataset].qualified
    raise ServeError(f"refusing to serve {dataset!r}: no serving tab")


def small_tabs_on(cfg: Config) -> bool:
    return cfg.get("X_LAKE_SERVE_SMALL_TABS", "off").lower() == "on"


def _cell(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.astimezone(TZ).strftime("%Y-%m-%d %H:%M")  # same ICT wall time the bots write
    if isinstance(v, date):
        return v.isoformat()
    if v is None:
        return ""
    if isinstance(v, str) and len(v) > 2000:
        return v[:2000]
    return v


def fetch(conn: Any, *, small_tabs: bool = False) -> dict[str, tuple[list[str], list[list[Any]]]]:
    for name in ("x_signals", "claims", "x_query_plan"):
        source_for(name)
    out: dict[str, tuple[list[str], list[list[Any]]]] = {}
    with conn.cursor() as c:
        c.execute(f"select {', '.join(SIGNAL_COLUMNS)} from {PG_SCHEMA}.x_signals "
                  f"where collected_at >= now() - interval '{DAYS} days' order by collected_at desc, post_id")
        signals = [[_cell(v) for v in row] for row in c.fetchall()]
        c.execute(f"select {', '.join(CLAIM_COLUMNS)} from {PG_SCHEMA}.claims "
                  "where coalesce(lower(queue_state), '') <> all(%s) "
                  "order by priority nulls last, next_check nulls last, claim_id", (list(CLOSED_STATES),))
        claims = [[_cell(v) for v in row] for row in c.fetchall()]
        c.execute(f"select {', '.join(QUERY_PLAN_COLUMNS)} from {PG_SCHEMA}.x_query_plan order by query_id")
        plan = [[_cell(v) for v in row] for row in c.fetchall()]
        out = {SERVE_TABS["x_signals"]: (SIGNAL_COLUMNS, signals), SERVE_TABS["claims"]: (CLAIM_COLUMNS, claims),
               SERVE_TABS["x_query_plan"]: (QUERY_PLAN_COLUMNS, plan)}
        if small_tabs:
            for name, (days, col, order) in SMALL_TAB_DAYS.items():
                tab, table = source_for(name)
                cols = DATASETS[name].names
                c.execute(f"select {', '.join(cols)} from {table} where {col} >= now() - interval '{days} days' "
                          f"order by {order}")
                out[tab] = (cols, [[_cell(v) for v in row] for row in c.fetchall()])
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true")
    args = p.parse_args(argv)
    cfg = Config()
    conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
    try:
        views = fetch(conn, small_tabs=small_tabs_on(cfg))
    finally:
        conn.close()
    for tab, (header, rows) in views.items():
        assert_writable(tab)
        print(f"{tab}: {len(rows)} rows x {len(header)} columns")
    if not args.execute:
        print("dry run: nothing written to the Sheet")
        return 0
    with httpx.Client(timeout=120) as http:
        w = SheetWriter(cfg, http)
        titles = w.titles()
        skipped = []
        for tab, (header, rows) in views.items():
            if tab not in titles:
                w.create_tab(tab, header)
            elif w.header(tab) != header:
                # Header is never rewritten; a changed column list needs a human decision.
                print(f"!! {tab}: header differs from the code's column list; skipped", file=sys.stderr)
                skipped.append(tab)
                continue
            w.replace_body(tab, rows)
            print(f"wrote {tab}: {len(rows)} rows")
    return 1 if skipped else 0  # a stale view must not look like a successful refresh


if __name__ == "__main__":
    sys.exit(main())
