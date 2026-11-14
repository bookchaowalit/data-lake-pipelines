"""X Feed facts from the raw lake, for the serving tab, the digest and the site (spec:
docs/specs/x-feed-serving.md).

Since 2026-11-13 the bots send raw data only (no AI tagging, triage or claim extraction), so x_signals and
claims stop growing. Everything a reader needs from the feed is computed here from raw ``x_feed`` without
an AI call and without a new curated table (RAW-ONLY rule, 2026-11-12):

* one row per post: the newest observation in the window (fresher counts), with ``watches`` = how many
  distinct watches saw it;
* ``engagement`` = likes + 2 x reposts + replies (views break ties), the ranking every reader uses;
* per watch (``watch_type:watch_value``), the distinct posts it saw.

Callers screen the rows against the client denylist themselves (digest / site), on the whole post dict:
``_screen`` carries every raw field of every observation of the post (_extra, _coerce_errors, the other
watches' values), so a client term outside COLUMNS still withholds the post. The Sheet tab is the
owner's own and is not screened, like the Signals tab before it.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .coerce import TZ
from .lake import read_dataset
from .schemas import X_FEED

COLUMNS = (
    "post_id",
    "collected_at",
    "requesting_chief",
    "watch_type",
    "watch_value",
    "author_handle",
    "posted_at",
    "post_url",
    "likes",
    "reposts",
    "replies",
    "views",
    "tag",
    "query_id",
    "text",
)


def engagement(row: dict[str, Any]) -> int:
    return (
        int(row.get("likes") or 0)
        + 2 * int(row.get("reposts") or 0)
        + int(row.get("replies") or 0)
    )


def watch_label(row: dict[str, Any]) -> str:
    kind, value = (
        (row.get("watch_type") or "").strip(),
        (row.get("watch_value") or "").strip(),
    )
    return f"{kind}:{value}" if kind or value else "(none)"


@dataclass
class FeedWindow:
    start: datetime
    end: datetime
    posts: list[dict[str, Any]] = field(
        default_factory=list
    )  # ranked by engagement, one per post
    watches: list[tuple[str, int]] = field(
        default_factory=list
    )  # (watch, distinct posts), most first
    observations: int = (
        0  # raw rows in the window (a post seen by two watches counts twice)
    )

    @property
    def authors(self) -> int:
        return len(
            {
                (p.get("author_handle") or "").lower()
                for p in self.posts
                if p.get("author_handle")
            }
        )


def summarise(rows: list[dict[str, Any]], start: datetime, end: datetime) -> FeedWindow:
    """``rows``: raw x_feed rows (dicts). Keeps rows collected in [start, end) with a post_id."""
    newest: dict[str, dict[str, Any]] = {}
    seen_by: dict[str, set[str]] = defaultdict(set)
    raw_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    n = 0
    for r in rows:
        pid, at = r.get("post_id"), r.get("collected_at")
        if not pid or not isinstance(at, datetime) or not (start <= at < end):
            continue
        n += 1
        seen_by[pid].add(watch_label(r))
        raw_rows[pid].append(r)
        cur = newest.get(pid)
        if cur is None or at > cur["collected_at"]:
            newest[pid] = r
    posts = []
    for pid, r in newest.items():
        row = {c: r.get(c) for c in COLUMNS}
        row["watches"] = len(seen_by[pid])
        row["engagement"] = engagement(r)
        # Every field of every observation (_extra, _coerce_errors, other watches' values, ...), for the
        # denylist screen in the digest and the site: a client term outside COLUMNS still withholds the post.
        row["_screen"] = raw_rows[pid]
        posts.append(row)
    posts.sort(
        key=lambda p: (
            -p["engagement"],
            -int(p.get("views") or 0),
            -p["collected_at"].timestamp(),
            str(p["post_id"]),
        )
    )
    per_watch = Counter(w for ws in seen_by.values() for w in ws)
    watches = sorted(per_watch.items(), key=lambda kv: (-kv[1], kv[0]))
    return FeedWindow(start, end, posts, watches, n)


FEED_SOURCE_NOTE = "raw X Feed by engagement (no AI labels)"


def daily(rows: list[dict[str, Any]], start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Distinct posts per (ICT day, watch) collected in [start, end): the site's chart (Gold 01's twin)."""
    seen: dict[tuple[Any, str], set[str]] = defaultdict(set)
    for r in rows:
        pid, at = r.get("post_id"), r.get("collected_at")
        if pid and isinstance(at, datetime) and start <= at < end:
            seen[(at.astimezone(TZ).date(), watch_label(r))].add(pid)
    return [{"day": d, "watch": w, "posts": len(p)} for (d, w), p in sorted(seen.items())]


def load_rows(store: Any, prefix: str, start: datetime) -> list[dict[str, Any]]:
    """Every raw x_feed row stored from the day before ``start`` (ICT) on, all columns."""
    since = (start.astimezone(TZ) - timedelta(days=1)).date()
    tbl = read_dataset(store, prefix, X_FEED, since)
    return [] if tbl is None else tbl.to_pylist()


def load(store: Any, prefix: str, start: datetime, end: datetime) -> FeedWindow:
    """Raw x_feed collected in [start, end). Partitions are dated by collected_at in ICT, so reading from
    the start's ICT date covers the window (one extra day for rows stored a day late)."""
    return summarise(load_rows(store, prefix, start), start, end)  # all columns: kept in _screen


def loader(cfg: Any) -> Any:
    """``loader(start, end)`` over the configured lake (X_LAKE_URI), for digest.run(feed_loader=...)."""
    from .lake import open_store

    opened: list[tuple[Any, str]] = []

    def _load(start: datetime, end: datetime) -> FeedWindow:
        if not opened:  # opened on first use: a missing X_LAKE_URI fails the call, not the caller's setup
            opened.append(open_store(cfg))
        store, prefix = opened[0]
        return load(store, prefix, start, end)

    return _load
