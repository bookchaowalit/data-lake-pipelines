#!/usr/bin/env python3
"""Jobs BI static site: one Gold snapshot (DuckDB) → plain HTML pages.

Reads the file jobs_bi_snapshot.py builds and writes a static site with no
third-party code: HTML rendered here, charts as server-side SVG, one small
first-party script (the companies filter), and a strict CSP in ``_headers``
(Cloudflare Pages). Every value from the marts is scraped text, so all of it
goes through ``html.escape``. The same snapshot always gives the same bytes.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from html import escape
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SITE_DIR = ROOT / "infra" / "bi" / "jobs"
ASSETS = SITE_DIR / "assets"
DEFAULT_DATABASE = SITE_DIR / "jobs.duckdb"
DEFAULT_OUT = SITE_DIR / "build"

SITE_NAME = "Jobs insights"
NAV = [("/", "Overview"), ("/boards/", "Boards"), ("/skills/", "Skills"), ("/companies/", "Companies")]
STYLESHEETS = ["/assets/design-floor/tokens.css", "/assets/app-theme.css", "/assets/site.css"]
DAILY_WINDOW_DAYS = 90
QUIET_DAYS = 14
SERIES_COLORS = 6  # boards beyond the top 6 in the window share "Other" (.s6)
HEADERS = """/*
  Content-Security-Policy: default-src 'none'; style-src 'self'; script-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'
  X-Content-Type-Options: nosniff
  Referrer-Policy: no-referrer
  X-Robots-Tag: noindex, nofollow
"""


@dataclass(frozen=True)
class Snapshot:
    meta: dict[str, Any]
    daily: list[dict[str, Any]]
    boards: list[dict[str, Any]]
    skills: list[dict[str, Any]]
    companies: list[dict[str, Any]]


def _rows(con: Any, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    cursor = con.execute(sql, params or [])
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def load_snapshot(database: Path) -> Snapshot:
    import duckdb  # type: ignore

    con = duckdb.connect(str(database), read_only=True)
    try:
        meta = _rows(con, "select gold_run_id, silver_run_id, as_of_date, max_seen_at from gold_meta")[0]
        as_of = meta["as_of_date"]
        daily = (
            _rows(
                con,
                "select posted_date, board, postings from jobs_postings_daily "
                "where posted_date > ? and posted_date <= ? order by posted_date, board",
                [as_of - timedelta(days=DAILY_WINDOW_DAYS), as_of],
            )
            if as_of
            else []
        )
        boards = _rows(
            con,
            "select board, postings_total, postings_7d, companies, last_posted_date, last_seen_at "
            "from jobs_board_health order by postings_total desc, board",
        )
        skills = _rows(
            con, "select posted_week, tag, postings, demand_rank from jobs_skills_demand order by posted_week, demand_rank, tag"
        )
        companies = _rows(
            con,
            "select company, postings_30d, postings_total, boards, last_posted_date "
            "from jobs_company_hiring order by postings_30d desc, postings_total desc, company_key",
        )
    finally:
        con.close()
    return Snapshot(meta, daily, boards, skills, companies)


# ---------------------------------------------------------------- rendering


def _fmt_int(value: Any) -> str:
    return f"{int(value or 0):,}"


def _fmt_date(value: Any) -> str:
    return value.isoformat() if isinstance(value, date) else "—"


def _fmt_time(value: Any) -> str:
    return value.strftime("%Y-%m-%d %H:%M") if value is not None else "—"


def _nice_step(raw: float) -> int:
    """Smallest whole 1, 2, 2.5 or 5 × 10^n step that is at least ``raw`` (minimum 1)."""
    magnitude = 1
    while True:
        for factor in (1, 2, 2.5, 5):
            step = factor * magnitude
            if step == int(step) and step >= raw:
                return int(step)
        magnitude *= 10


def _bar_cell(value: float, maximum: float) -> str:
    width = 0.0 if maximum <= 0 else round(100.0 * value / maximum, 2)
    return (
        '<td class="barcell" aria-hidden="true"><svg viewBox="0 0 100 10" preserveAspectRatio="none" '
        f'focusable="false"><rect x="0" y="0" width="{width}" height="10"/></svg></td>'
    )


def _table(
    table_id: str, caption: str, head: list[tuple[str, bool]], rows: list[str], label: str, *, tall: bool = False
) -> str:
    cols = "".join(
        f'<th scope="col"{" class=\"num\"" if numeric else ""}>{escape(name)}</th>' for name, numeric in head
    )
    return (
        f'<div class="table-wrap{" tall" if tall else ""}" tabindex="0" role="region" aria-label="{escape(label)}">'
        f'<table id="{table_id}"><caption>{escape(caption)}</caption>'
        f"<thead><tr>{cols}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


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
        "Built only from the jobs Gold marts on the R2 lake.</footer>\n"
        "</body>\n</html>\n"
    )


def _daily_chart(snapshot: Snapshot) -> tuple[str, int]:
    as_of = snapshot.meta["as_of_date"]
    if not snapshot.daily or as_of is None:
        return "<p>No postings in the last 90 days.</p>", 0
    totals: dict[str, int] = {}
    for row in snapshot.daily:
        totals[row["board"]] = totals.get(row["board"], 0) + int(row["postings"])
    ranked = sorted(totals, key=lambda board: (-totals[board], board))
    series = {board: f"s{i}" for i, board in enumerate(ranked[:SERIES_COLORS])}
    days = [as_of - timedelta(days=offset) for offset in range(DAILY_WINDOW_DAYS - 1, -1, -1)]
    stacks: dict[date, list[tuple[str, int]]] = {day: [] for day in days}
    for row in snapshot.daily:
        stacks[row["posted_date"]].append((series.get(row["board"], "s6"), int(row["postings"])))
    highest = max(sum(value for _, value in stack) for stack in stacks.values())
    tick_step = _nice_step(highest / 5)
    ticks = max(1, -(-highest // tick_step))  # the axis ends on the first tick at or above the data
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
        order = {f"s{k}": k for k in range(SERIES_COLORS + 1)}
        for css, value in sorted(stacks[day], key=lambda item: order[item[0]]):
            h = plot_h * value / peak
            y -= h
            parts.append(
                f'<rect class="{css}" x="{x + 0.5:.1f}" y="{y:.1f}" width="{max(step - 1, 0.5):.1f}" height="{h:.1f}"/>'
            )
        # Month starts, plus the first day; none so close to the right edge it would clip.
        if (day.day == 1 or i == 0) and x < width - 60:
            parts.append(f'<text x="{x:.1f}" y="{height - 6}">{day.strftime("%b %d")}</text>')
    legend = "".join(
        f'<li><svg viewBox="0 0 10 10" aria-hidden="true" focusable="false"><rect class="{css}" width="10" height="10"/></svg>'
        f"{escape(str(board))}</li>"
        for board, css in series.items()
    )
    if len(ranked) > SERIES_COLORS:
        legend += (
            '<li><svg viewBox="0 0 10 10" aria-hidden="true" focusable="false"><rect class="s6" width="10" height="10"/></svg>'
            "Other boards</li>"
        )
    total = sum(totals.values())
    svg = (
        '<div class="chart-wrap" tabindex="0" role="region" aria-label="Postings per day chart, scrolls sideways">'
        f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-labelledby="daily-title daily-desc">'
        f'<title id="daily-title">Postings per day by board, {_fmt_date(days[0])} to {_fmt_date(as_of)}</title>'
        f'<desc id="daily-desc">{total} postings over {DAILY_WINDOW_DAYS} days; the table below lists each board.</desc>'
        f"{''.join(parts)}</svg></div>"
    )
    rows = [
        f"<tr><th scope=\"row\">{escape(str(board))}</th><td class=\"num\">{_fmt_int(totals[board])}</td></tr>"
        for board in ranked
    ]
    table = _table(
        "daily-boards",
        f"Postings per board, last {DAILY_WINDOW_DAYS} days",
        [("Board", False), ("Postings", True)],
        rows,
        "Postings per board, last 90 days",
    )
    return f'<ul class="legend">{legend}</ul>{svg}{table}', total


def render_overview(snapshot: Snapshot) -> str:
    meta = snapshot.meta
    chart, total_90d = _daily_chart(snapshot)
    postings = sum(int(row["postings_total"]) for row in snapshot.boards)
    postings_7d = sum(int(row["postings_7d"]) for row in snapshot.boards)
    kpis = [
        ("postings", "Postings", postings),
        ("postings_90d", f"Posted in the last {DAILY_WINDOW_DAYS} days", total_90d),
        ("postings_7d", "Posted in the last 7 days", postings_7d),
        ("boards", "Boards", len(snapshot.boards)),
        ("companies", "Companies", len(snapshot.companies)),
    ]
    tiles = "".join(
        f'<div class="kpi"><dt>{escape(label)}</dt><dd data-kpi="{key}">{_fmt_int(value)}</dd></div>'
        for key, label, value in kpis
    )
    body = (
        "<h1>Jobs overview</h1>"
        f'<p class="lede">Data as of {_fmt_date(meta["as_of_date"])} (newest capture '
        f"{_fmt_time(meta['max_seen_at'])} UTC). Windows end on that date, not today.</p>"
        f'<dl class="kpis">{tiles}</dl>'
        f"<h2>Postings per day, last {DAILY_WINDOW_DAYS} days</h2>{chart}"
    )
    return _page("Overview", "/", body, meta)


def _board_status(row: dict[str, Any], as_of: date | None) -> tuple[str, str]:
    last = row["last_posted_date"]
    if last is None:
        return "No posted date", "status-none"
    if as_of is not None and last < as_of - timedelta(days=QUIET_DAYS):
        return f"Quiet {QUIET_DAYS}+ days", "status-quiet"
    return "Active", ""


def render_boards(snapshot: Snapshot) -> str:
    as_of = snapshot.meta["as_of_date"]
    peak = max((int(row["postings_total"]) for row in snapshot.boards), default=0)
    rows = []
    for row in snapshot.boards:
        status, css = _board_status(row, as_of)
        rows.append(
            f"<tr><th scope=\"row\">{escape(str(row['board']))}</th>"
            f'<td{f" class={chr(34)}{css}{chr(34)}" if css else ""}>{escape(status)}</td>'
            f"<td class=\"num\">{_fmt_int(row['postings_total'])}</td>{_bar_cell(int(row['postings_total']), peak)}"
            f"<td class=\"num\">{_fmt_int(row['postings_7d'])}</td>"
            f"<td class=\"num\">{_fmt_int(row['companies'])}</td>"
            f"<td>{_fmt_date(row['last_posted_date'])}</td><td>{_fmt_time(row['last_seen_at'])}</td></tr>"
        )
    table = _table(
        "boards",
        "Job boards by postings",
        [("Board", False), ("Status", False), ("Postings", True), ("Share", False), ("Last 7 days", True),
         ("Companies", True), ("Last posted", False), ("Last seen (UTC)", False)],
        rows,
        "Job boards table",
    )
    body = (
        "<h1>Boards</h1>"
        f'<p class="lede">A board is <strong>quiet</strong> when its newest posting is more than {QUIET_DAYS} days '
        "older than the newest capture. “Last seen” is when the scraper last captured a posting from it.</p>"
        f"{table if rows else '<p>No boards in this snapshot.</p>'}"
    )
    return _page("Boards", "/boards/", body, snapshot.meta)


def render_skills(snapshot: Snapshot) -> str:
    as_of = snapshot.meta["as_of_date"]
    body = ["<h1>Skills demand</h1>",
            '<p class="lede">Tags as the job boards list them, each counted once per posting. '
            "Weeks start on Monday; the week in progress is left out.</p>"]
    if as_of is None or not snapshot.skills:
        body.append("<p>No tags in this snapshot.</p>")
        return _page("Skills", "/skills/", "".join(body), snapshot.meta)
    last_week = as_of - timedelta(days=as_of.weekday() + 7)
    first_of_4 = last_week - timedelta(days=21)

    top = [row for row in snapshot.skills if row["posted_week"] == last_week][:20]
    peak = max((int(row["postings"]) for row in top), default=0)
    rows = [
        f"<tr><td class=\"num\">{int(row['demand_rank'])}</td><th scope=\"row\">{escape(str(row['tag']))}</th>"
        f"<td class=\"num\">{_fmt_int(row['postings'])}</td>{_bar_cell(int(row['postings']), peak)}</tr>"
        for row in top
    ]
    body.append(f"<h2>Week of {last_week.isoformat()}: top 20 tags</h2>")
    body.append(
        _table("skills-week", f"Top tags, week of {last_week.isoformat()}",
               [("Rank", True), ("Tag", False), ("Postings", True), ("Share", False)], rows, "Top tags this week")
        if rows else "<p>No tags in that week.</p>"
    )

    summed: dict[str, list[int]] = {}
    for row in snapshot.skills:
        if first_of_4 <= row["posted_week"] <= last_week:
            entry = summed.setdefault(row["tag"], [0, 0])
            entry[0] += int(row["postings"])
            entry[1] += 1
    ranked = sorted(summed.items(), key=lambda item: (-item[1][0], item[0]))[:30]
    peak4 = ranked[0][1][0] if ranked else 0
    rows4 = [
        f"<tr><th scope=\"row\">{escape(str(tag))}</th><td class=\"num\">{_fmt_int(total)}</td>"
        f"{_bar_cell(total, peak4)}<td class=\"num\">{weeks}</td></tr>"
        for tag, (total, weeks) in ranked
    ]
    body.append(f"<h2>Last 4 complete weeks ({first_of_4.isoformat()} to {(last_week + timedelta(days=6)).isoformat()})</h2>")
    body.append(
        _table("skills-4w", "Top 30 tags over the last 4 complete weeks",
               [("Tag", False), ("Postings", True), ("Share", False), ("Weeks in the top 50", True)],
               rows4, "Top tags over four weeks")
        if rows4 else "<p>No tags in those weeks.</p>"
    )
    return _page("Skills", "/skills/", "".join(body), snapshot.meta)


def render_companies(snapshot: Snapshot) -> str:
    rows = [
        f"<tr><th scope=\"row\">{escape(str(row['company']))}</th>"
        f"<td class=\"num\">{_fmt_int(row['postings_30d'])}</td><td class=\"num\">{_fmt_int(row['postings_total'])}</td>"
        f"<td>{escape(', '.join(str(board) for board in (row['boards'] or [])))}</td>"
        f"<td>{_fmt_date(row['last_posted_date'])}</td></tr>"
        for row in snapshot.companies
    ]
    count = len(rows)
    body = (
        "<h1>Companies hiring</h1>"
        '<p class="lede">Companies by postings in the 30 days ending on the newest capture date.</p>'
        '<form class="filter" data-filter hidden role="search">'
        '<label for="company-filter">Filter companies</label>'
        '<input id="company-filter" type="search" autocomplete="off" aria-describedby="filter-status">'
        f'<p id="filter-status" class="muted" aria-live="polite">{count} of {count} companies</p></form>'
        + (
            _table("companies", f"{count} companies",
                   [("Company", False), ("Last 30 days", True), ("All postings", True), ("Boards", False),
                    ("Last posted", False)], rows, "Companies table", tall=True)
            if rows else "<p>No companies in this snapshot.</p>"
        )
        + '<script src="/assets/filter.js" defer></script>'
    )
    return _page("Companies", "/companies/", body, snapshot.meta)


def render_not_found(snapshot: Snapshot) -> str:
    body = '<h1>Page not found</h1><p>That page does not exist. <a href="/">Back to the overview</a>.</p>'
    return _page("Page not found", "", body, snapshot.meta)


def build_site(database: Path, out_dir: Path) -> dict[str, Any]:
    snapshot = load_snapshot(database)
    pages = {
        "index.html": render_overview(snapshot),
        "boards/index.html": render_boards(snapshot),
        "skills/index.html": render_skills(snapshot),
        "companies/index.html": render_companies(snapshot),
        "404.html": render_not_found(snapshot),
    }
    if out_dir.exists():
        shutil.rmtree(out_dir)
    for relative, html in pages.items():
        target = out_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(html, encoding="utf-8")
    shutil.copytree(ASSETS, out_dir / "assets")
    (out_dir / "_headers").write_text(HEADERS, encoding="utf-8")
    return {
        "gold_run_id": snapshot.meta["gold_run_id"],
        "pages": len(pages),
        "out_dir": str(out_dir),
    }


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
