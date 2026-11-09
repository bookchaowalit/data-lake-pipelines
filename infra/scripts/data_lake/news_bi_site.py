#!/usr/bin/env python3
"""News BI static site: one Gold snapshot (DuckDB) → plain HTML pages.

Reads the file news_bi_snapshot.py builds (docs/specs/news-marts.md item 7).
No script at all: HTML rendered here, the chart as server-side SVG, and the
jobs site's CSS, design-floor tokens and strict CSP. Every value from the
marts is scraped text, so all of it goes through ``html.escape``; links out
are kept only when they are plain http(s) URLs. Same snapshot, same bytes.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.jobs_bi_site import (  # noqa: E402
    ASSETS as JOBS_ASSETS,
    HEADERS,
    STYLESHEETS,
    _fmt_date,
    _fmt_int,
    _fmt_time,
    _nice_step,
    _rows,
    _table,
)

SITE_DIR = ROOT / "infra" / "bi" / "news"
DEFAULT_DATABASE = SITE_DIR / "news.duckdb"
DEFAULT_OUT = SITE_DIR / "build"
SITE_NAME = "News insights"
NAV = [("/", "Overview"), ("/latest/", "Latest headlines")]
WINDOW_DAYS = 14
STALE_HOURS = 6
THAI = re.compile("[฀-๿]")
# The jobs site's only script (its companies filter) is not shipped here.
SKIP_ASSETS = shutil.ignore_patterns("filter.js")


@dataclass(frozen=True)
class Snapshot:
    meta: dict[str, Any]
    daily: list[dict[str, Any]]
    feeds: list[dict[str, Any]]
    latest: list[dict[str, Any]]


def load_snapshot(database: Path) -> Snapshot:
    import duckdb  # type: ignore

    con = duckdb.connect(str(database), read_only=True)
    try:
        meta = _rows(con, "select gold_run_id, silver_run_id, as_of_date, max_seen_at from gold_meta")[0]
        as_of = meta["as_of_date"]
        daily = (
            _rows(
                con,
                "select published_date, publisher, sum(articles) as articles from news_articles_daily "
                "where published_date > ? and published_date <= ? group by 1, 2 order by 1, 2",
                [as_of - timedelta(days=WINDOW_DAYS), as_of],
            )
            if as_of
            else []
        )
        feeds = _rows(
            con,
            "select publisher, articles_total, articles_7d, last_published_at, last_seen_at "
            "from news_feed_health order by articles_7d desc, publisher",
        )
        latest = _rows(
            con,
            "select publisher, section, published_at, headline, canonical_url, rank "
            "from news_latest_articles order by publisher, rank",
        )
    finally:
        con.close()
    return Snapshot(meta, daily, feeds, latest)


def _safe_href(url: Any) -> str | None:
    parts = urlsplit(str(url or ""))
    return str(url) if parts.scheme in ("http", "https") and parts.netloc else None


def _lang(text: str) -> str:
    return ' lang="th"' if THAI.search(text) else ""


def _page(title: str, path: str, body: str, meta: dict[str, Any]) -> str:
    links = "".join(
        f'<li><a href="{href}"{" aria-current=\"page\"" if href == path else ""}>{escape(name)}</a></li>'
        for href, name in NAV
    )
    styles = "".join(f'<link rel="stylesheet" href="{href}">' for href in STYLESHEETS)
    return (
        '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        '<meta name="robots" content="noindex, nofollow">\n'
        f"<title>{escape(title)} · {SITE_NAME}</title>\n{styles}\n</head>\n<body>\n"
        '<a class="skip-link" href="#main">Skip to content</a>\n'
        f'<header><div class="bar"><a class="brand" href="/">{SITE_NAME}</a>'
        f'<nav aria-label="Main"><ul>{links}</ul></nav></div></header>\n'
        f'<main id="main" tabindex="-1">\n{body}\n</main>\n'
        f"<footer>Gold run {escape(str(meta['gold_run_id']))} (Silver {escape(str(meta['silver_run_id']))}). "
        "Built only from the news Gold marts on the R2 lake. Headlines belong to their publishers.</footer>\n"
        "</body>\n</html>\n"
    )


def _daily_chart(snapshot: Snapshot) -> str:
    as_of = snapshot.meta["as_of_date"]
    if not snapshot.daily or as_of is None:
        return f"<p>No articles in the last {WINDOW_DAYS} days.</p>"
    publishers = sorted({row["publisher"] for row in snapshot.daily})
    series = {name: f"s{i if i < 6 else 6}" for i, name in enumerate(publishers)}
    days = [as_of - timedelta(days=offset) for offset in range(WINDOW_DAYS - 1, -1, -1)]
    stacks: dict[date, list[tuple[str, int]]] = {day: [] for day in days}
    for row in snapshot.daily:
        stacks[row["published_date"]].append((series[row["publisher"]], int(row["articles"])))
    highest = max(sum(value for _, value in stack) for stack in stacks.values())
    tick_step = _nice_step(highest / 5)
    ticks = max(1, -(-highest // tick_step))
    peak = tick_step * ticks

    width, height, left, bottom, top = 900, 260, 40, 24, 8
    plot_h = height - bottom - top
    step = (width - left) / len(days)
    parts = []
    for tick in range(ticks + 1):
        value = tick_step * tick
        y = top + plot_h - plot_h * value / peak
        parts.append(f'<line class="grid" x1="{left}" x2="{width}" y1="{y:.1f}" y2="{y:.1f}"/>')
        parts.append(f'<text x="{left - 6}" y="{y + 4:.1f}" text-anchor="end">{value}</text>')
    for i, day in enumerate(days):
        x = left + i * step
        y = top + plot_h
        for css, value in sorted(stacks[day]):
            h = plot_h * value / peak
            y -= h
            parts.append(
                f'<rect class="{css}" x="{x + 1:.1f}" y="{y:.1f}" width="{max(step - 2, 0.5):.1f}" height="{h:.1f}"/>'
            )
        if i % 2 == 0:
            parts.append(f'<text x="{x + step / 2:.1f}" y="{height - 6}" text-anchor="middle">{day.strftime("%d %b")}</text>')
    legend = "".join(
        f'<li><svg viewBox="0 0 10 10" aria-hidden="true" focusable="false"><rect class="{css}" width="10" height="10"/></svg>'
        f"{escape(str(name))}</li>"
        for name, css in series.items()
    )
    totals = {name: 0 for name in publishers}
    for row in snapshot.daily:
        totals[row["publisher"]] += int(row["articles"])
    svg = (
        '<div class="chart-wrap" tabindex="0" role="region" aria-label="Articles per day chart, scrolls sideways">'
        f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-labelledby="daily-title daily-desc">'
        f'<title id="daily-title">Articles per day by publisher, {_fmt_date(days[0])} to {_fmt_date(as_of)}</title>'
        f'<desc id="daily-desc">{sum(totals.values())} articles over {WINDOW_DAYS} days; '
        "the table below lists each publisher.</desc>"
        f"{''.join(parts)}</svg></div>"
    )
    rows = [
        f'<tr><th scope="row">{escape(str(name))}</th><td class="num">{_fmt_int(totals[name])}</td></tr>'
        for name in sorted(publishers, key=lambda name: (-totals[name], name))
    ]
    table = _table(
        "daily-publishers",
        f"Articles per publisher, last {WINDOW_DAYS} days",
        [("Publisher", False), ("Articles", True)],
        rows,
        f"Articles per publisher, last {WINDOW_DAYS} days",
    )
    return f'<ul class="legend">{legend}</ul>{svg}{table}'


def _feed_status(row: dict[str, Any], max_seen: Any) -> tuple[str, str]:
    last = row["last_seen_at"]
    if last is None:
        return "Never captured", "status-none"
    if max_seen is not None and (max_seen - last).total_seconds() > STALE_HOURS * 3600:
        return f"No capture for {STALE_HOURS}+ h", "status-quiet"
    return "Capturing", ""


def render_overview(snapshot: Snapshot) -> str:
    meta = snapshot.meta
    articles = sum(int(row["articles_total"]) for row in snapshot.feeds)
    articles_7d = sum(int(row["articles_7d"]) for row in snapshot.feeds)
    kpis = [
        ("articles", "Articles", articles),
        ("articles_7d", "Published in the last 7 days", articles_7d),
        ("publishers", "Publishers", len(snapshot.feeds)),
    ]
    tiles = "".join(
        f'<div class="kpi"><dt>{escape(label)}</dt><dd data-kpi="{key}">{_fmt_int(value)}</dd></div>'
        for key, label, value in kpis
    )
    rows = []
    for row in snapshot.feeds:
        status, css = _feed_status(row, meta["max_seen_at"])
        rows.append(
            f"<tr><th scope=\"row\">{escape(str(row['publisher']))}</th>"
            f'<td{f" class={chr(34)}{css}{chr(34)}" if css else ""}>{escape(status)}</td>'
            f"<td class=\"num\">{_fmt_int(row['articles_7d'])}</td>"
            f"<td class=\"num\">{_fmt_int(row['articles_total'])}</td>"
            f"<td>{_fmt_time(row['last_published_at'])}</td><td>{_fmt_time(row['last_seen_at'])}</td></tr>"
        )
    feeds = _table(
        "feeds",
        "Feeds",
        [("Publisher", False), ("Status", False), ("Last 7 days", True), ("All articles", True),
         ("Newest article (UTC)", False), ("Last capture (UTC)", False)],
        rows,
        "Feeds table",
    )
    body = (
        "<h1>News overview</h1>"
        f'<p class="lede">Data as of {_fmt_date(meta["as_of_date"])} in Bangkok (newest capture '
        f"{_fmt_time(meta['max_seen_at'])} UTC). Dates are publish dates in Bangkok time; windows end on the "
        "as-of date, not today.</p>"
        f'<dl class="kpis">{tiles}</dl>'
        f"<h2>Feeds</h2>{feeds if rows else '<p>No feeds in this snapshot.</p>'}"
        f"<h2>Articles per day, last {WINDOW_DAYS} days</h2>{_daily_chart(snapshot)}"
    )
    return _page("Overview", "/", body, meta)


def render_latest(snapshot: Snapshot) -> str:
    body = [
        "<h1>Latest headlines</h1>",
        '<p class="lede">The newest 30 articles from each feed. Links open the publisher’s page.</p>',
    ]
    by_publisher: dict[str, list[dict[str, Any]]] = {}
    for row in snapshot.latest:
        by_publisher.setdefault(str(row["publisher"]), []).append(row)
    if not by_publisher:
        body.append("<p>No articles in this snapshot.</p>")
    for publisher in sorted(by_publisher):
        items = []
        for row in by_publisher[publisher]:
            headline = str(row["headline"])
            href = _safe_href(row["canonical_url"])
            text = (
                f'<a href="{escape(href)}" rel="noopener noreferrer"{_lang(headline)}>{escape(headline)}</a>'
                if href
                else f"<span{_lang(headline)}>{escape(headline)}</span>"
            )
            items.append(f'<li>{text} <span class="muted">{_fmt_time(row["published_at"])} UTC</span></li>')
        body.append(f"<h2>{escape(publisher)}</h2><ol>{''.join(items)}</ol>")
    return _page("Latest headlines", "/latest/", "".join(body), snapshot.meta)


def render_not_found(snapshot: Snapshot) -> str:
    body = '<h1>Page not found</h1><p>That page does not exist. <a href="/">Back to the overview</a>.</p>'
    return _page("Page not found", "", body, snapshot.meta)


def build_site(database: Path, out_dir: Path) -> dict[str, Any]:
    snapshot = load_snapshot(database)
    pages = {
        "index.html": render_overview(snapshot),
        "latest/index.html": render_latest(snapshot),
        "404.html": render_not_found(snapshot),
    }
    if out_dir.exists():
        shutil.rmtree(out_dir)
    for relative, html in pages.items():
        target = out_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(html, encoding="utf-8")
    shutil.copytree(JOBS_ASSETS, out_dir / "assets", ignore=SKIP_ASSETS)
    (out_dir / "_headers").write_text(HEADERS.replace("script-src 'self'", "script-src 'none'"), encoding="utf-8")
    return {"gold_run_id": snapshot.meta["gold_run_id"], "pages": len(pages), "out_dir": str(out_dir)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    try:
        result = build_site(args.database, args.out)
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        print(json.dumps({"status": "failed", "error": type(exc).__name__}))
        return 2
    print(json.dumps({"status": "success", **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
