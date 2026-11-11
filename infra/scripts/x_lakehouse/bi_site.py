"""X insights static site: the Postgres marts → plain HTML pages (spec: docs/specs/x-insights.md).

Built by the Railway cron at the end of a run (``X_LAKE_BI``), never in the public repository's CI:
the data is in Supabase and only that job holds the URL. Same look as the jobs/news sites (their CSS,
design-floor tokens and strict CSP, no script at all). Every row is screened with the digest's client
denylist first (fail closed); every value is escaped; links are kept only for plain http(s) URLs.
Windows end on the newest signal's Bangkok date, not on today, so unchanged data renders the same bytes.
"""

from __future__ import annotations

import hashlib
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from html import escape
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import urlsplit

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.jobs_bi_site import (  # noqa: E402
    ASSETS as JOBS_ASSETS,
    HEADERS,
    STYLESHEETS,
    _fmt_date,
    _fmt_int,
    _nice_step,
    _table,
)

from .digest import any_term, contains_term  # noqa: E402
from .marts import CLAIM_FULL, SIGNAL_FULL  # noqa: E402
from .schemas import PG_SCHEMA  # noqa: E402

S = PG_SCHEMA
SITE_NAME = "X insights"
NAV = [("/", "Overview"), ("/signals/", "Top signals"), ("/claims/", "Claims")]
KPI_DAYS = 7
CHART_DAYS = 14
TOP_SIGNALS = 50
TEXT_CAP = 280
SERIES = 6  # domains beyond the top 6 in the chart window share "Other" (.s6)
WITHHELD = "(withheld)"
THAI = re.compile("[฀-๿]")
SKIP_ASSETS = shutil.ignore_patterns("filter.js")

SIGNAL_COLS = ("post_id", "collected_ict", "domain", "signal_type", "alert_tier", "cred_tier",
               "verification_status", "suggested_action", "opportunity_score", "author_handle", "post_url",
               "post_text", "likes", "reposts", "views", "priority")
CLAIM_COLS = ("claim_id", "claim", "verification_status", "queue_state", "priority", "next_check",
              "last_checked", "first_seen_link", "checked_by")
DIGEST_COLS = ("digest_id", "kind", "status", "signals", "cost_usd", "ai", "updated_at")


@dataclass
class Snapshot:
    as_of: date | None
    newest: datetime | None  # newest collected_ict
    daily: list[dict[str, Any]]  # day, domain, signals, alerts, urgent, opportunities
    signals: list[dict[str, Any]]  # top of the KPI window by priority, already screened
    claims: list[dict[str, Any]]  # open queue, already screened
    digests: list[dict[str, Any]]
    withheld: dict[str, int] = field(default_factory=dict)


def _rows(cur: Any, cols: Sequence[str]) -> list[dict[str, Any]]:
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def load_snapshot(conn: Any, denylist: Sequence[str]) -> Snapshot:
    """Read the marts and screen them. ``denylist`` must be non-empty (digest.load_denylist fails closed)."""
    if not denylist:
        raise ValueError("client denylist is empty; the site is not built (fails closed)")
    with conn.cursor() as c:
        c.execute(f"select max(collected_ict) from {S}.mart_x_signal_priority")
        newest = c.fetchone()[0]
        as_of = newest.date() if newest else None
        daily: list[dict[str, Any]] = []
        ranked: list[dict[str, Any]] = []
        if as_of:
            c.execute(f"select hour_ict::date, domain, sum(signals), sum(alerts), sum(urgent), sum(opportunities) "
                      f"from {S}.mart_x_domain_hourly where hour_ict >= %s and hour_ict < %s "
                      f"group by 1, 2 order by 1, 2",
                      (as_of - timedelta(days=CHART_DAYS - 1), as_of + timedelta(days=1)))
            daily = _rows(c, ("day", "domain", "signals", "alerts", "urgent", "opportunities"))
            # The whole window, every mart column (to_jsonb) for screen(): a fixed overfetch could be all
            # withheld rows, and a client term can sit in a column the page does not show.
            c.execute(f"select {', '.join(SIGNAL_COLS)}, {SIGNAL_FULL} from {S}.mart_x_signal_priority m "
                      f"where collected_ict >= %s and collected_ict < %s "
                      f"order by priority desc, collected_at desc, post_id",
                      (as_of - timedelta(days=KPI_DAYS - 1), as_of + timedelta(days=1)))
            ranked = _rows(c, (*SIGNAL_COLS, "_row"))
        c.execute(f"select {', '.join(CLAIM_COLS)}, {CLAIM_FULL} from {S}.mart_claims_queue m")
        claims = _rows(c, (*CLAIM_COLS, "_row"))
        c.execute(f"select digest_id, kind, status, signals, cost_usd, coalesce(tokens_out, 0) > 0, updated_at "
                  f"from {S}.digest_log order by window_end desc, digest_id limit 10")
        digests = _rows(c, DIGEST_COLS)
    conn.commit()
    return screen(Snapshot(as_of, newest, daily, ranked, claims, digests), denylist)


def screen(snap: Snapshot, denylist: Sequence[str]) -> Snapshot:
    """Drop every signal/claim with any field naming a denylisted term; mask matching domain labels."""
    withheld = {"signals": 0, "claims": 0, "domains": 0}
    clean = []
    for r in snap.signals:
        if any_term(r, denylist):
            withheld["signals"] += 1
        else:
            clean.append({k: v for k, v in r.items() if k != "_row"})
    claims = []
    for r in snap.claims:
        if any_term(r, denylist):
            withheld["claims"] += 1
        else:
            claims.append({k: v for k, v in r.items() if k != "_row"})
    masked = set()
    daily = []
    for r in snap.daily:
        if contains_term(str(r["domain"]), denylist):
            masked.add(r["domain"])
            r = {**r, "domain": WITHHELD}
        daily.append(r)
    withheld["domains"] = len(masked)
    return Snapshot(snap.as_of, snap.newest, daily, clean[:TOP_SIGNALS], claims, snap.digests, withheld)


# ------------------------------------------------------------------------------------------ render

def _safe_href(url: Any) -> str | None:
    parts = urlsplit(str(url or ""))
    return str(url) if parts.scheme in ("http", "https") and parts.netloc else None


def _lang(text: str) -> str:
    return ' lang="th"' if THAI.search(text) else ""


def _cut(text: Any) -> str:
    s = " ".join(str(text or "").split())
    return s if len(s) <= TEXT_CAP else s[: TEXT_CAP - 1].rstrip() + "…"


def _time(value: Any) -> str:
    return value.strftime("%Y-%m-%d %H:%M") if value is not None else "—"


def _e(value: Any) -> str:
    return "—" if value is None or value == "" else escape(str(value))


def _page(title: str, path: str, body: str, snap: Snapshot) -> str:
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
        f"<footer>Newest signal {_time(snap.newest)} Bangkok time. Built from the x_intel marts "
        "(Supabase) by the x-lakehouse job; rows naming a client are left out. Posts belong to their authors."
        "</footer>\n</body>\n</html>\n"
    )


def _window(snap: Snapshot, days: int) -> list[dict[str, Any]]:
    assert snap.as_of is not None
    start = snap.as_of - timedelta(days=days - 1)
    return [r for r in snap.daily if r["day"] >= start]


def _domain_totals(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for r in rows:
        t = out.setdefault(str(r["domain"]), {"signals": 0, "alerts": 0, "urgent": 0, "opportunities": 0})
        for k in t:
            t[k] += int(r[k] or 0)
    return out


def _chart(snap: Snapshot) -> str:
    rows = _window(snap, CHART_DAYS)
    if not rows:
        return f"<p>No signals in the last {CHART_DAYS} days.</p>"
    totals = _domain_totals(rows)
    order = sorted(totals, key=lambda d: (-totals[d]["signals"], d))
    named = order[:SERIES]
    series = {d: f"s{i}" for i, d in enumerate(named)}
    has_other = len(order) > SERIES
    days = [snap.as_of - timedelta(days=o) for o in range(CHART_DAYS - 1, -1, -1)]  # type: ignore[operator]
    stacks: dict[date, dict[str, int]] = {d: {} for d in days}
    for r in rows:
        css = series.get(str(r["domain"]), "s6")
        stacks[r["day"]][css] = stacks[r["day"]].get(css, 0) + int(r["signals"] or 0)
    highest = max(sum(s.values()) for s in stacks.values()) or 1
    step_v = _nice_step(highest / 5)
    ticks = max(1, -(-highest // step_v))
    peak = step_v * ticks
    width, height, left, bottom, top = 900, 260, 48, 24, 8
    plot_h = height - bottom - top
    step = (width - left) / len(days)
    parts = []
    for t in range(ticks + 1):
        v = step_v * t
        y = top + plot_h - plot_h * v / peak
        parts.append(f'<line class="grid" x1="{left}" x2="{width}" y1="{y:.1f}" y2="{y:.1f}"/>')
        parts.append(f'<text x="{left - 6}" y="{y + 4:.1f}" text-anchor="end">{v}</text>')
    for i, d in enumerate(days):
        x = left + i * step
        y = top + plot_h
        for css, v in sorted(stacks[d].items()):
            h = plot_h * v / peak
            y -= h
            parts.append(f'<rect class="{css}" x="{x + 1:.1f}" y="{y:.1f}" width="{max(step - 2, 0.5):.1f}" '
                         f'height="{h:.1f}"/>')
        if i % 2 == 0:
            parts.append(f'<text x="{x + step / 2:.1f}" y="{height - 6}" text-anchor="middle">'
                         f'{d.strftime("%d %b")}</text>')
    legend_items = [(d, series[d]) for d in named] + ([("Other", "s6")] if has_other else [])
    legend = "".join(
        f'<li><svg viewBox="0 0 10 10" aria-hidden="true" focusable="false"><rect class="{css}" width="10" '
        f'height="10"/></svg><span{_lang(name)}>{escape(name)}</span></li>' for name, css in legend_items
    )
    total = sum(t["signals"] for t in totals.values())
    return (
        f'<ul class="legend">{legend}</ul>'
        '<div class="chart-wrap" tabindex="0" role="region" aria-label="Signals per day chart, scrolls sideways">'
        f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-labelledby="daily-title daily-desc">'
        f'<title id="daily-title">Signals per day by domain, {_fmt_date(days[0])} to {_fmt_date(snap.as_of)}</title>'
        f'<desc id="daily-desc">{total} signals over {CHART_DAYS} days; the domain table lists each domain.</desc>'
        f"{''.join(parts)}</svg></div>"
    )


def render_overview(snap: Snapshot) -> str:
    if snap.as_of is None:
        body = "<h1>X overview</h1><p>No signals yet.</p>"
        return _page("Overview", "/", body, snap)
    week = _domain_totals(_window(snap, KPI_DAYS))
    sums = {k: sum(t[k] for t in week.values()) for k in ("signals", "alerts", "urgent", "opportunities")}
    kpis = [("signals", "Signals", sums["signals"]), ("alerts", "Alerts", sums["alerts"]),
            ("urgent", "Urgent candidates", sums["urgent"]), ("opportunities", "Opportunities", sums["opportunities"]),
            ("claims", "Open claims", len(snap.claims))]
    tiles = "".join(f'<div class="kpi"><dt>{escape(label)}</dt><dd data-kpi="{key}">{_fmt_int(v)}</dd></div>'
                    for key, label, v in kpis)
    order = sorted(week, key=lambda d: (-week[d]["signals"], d))
    rows = [f'<tr><th scope="row"{_lang(d)}>{escape(d)}</th>'
            + "".join(f'<td class="num">{_fmt_int(week[d][k])}</td>'
                      for k in ("signals", "alerts", "urgent", "opportunities")) + "</tr>" for d in order]
    domains = _table("domains", f"Signals per domain, last {KPI_DAYS} days",
                     [("Domain", False), ("Signals", True), ("Alerts", True), ("Urgent", True),
                      ("Opportunities", True)], rows, "Domains table")
    drows = [f"<tr><th scope=\"row\">{_e(r['digest_id'])}</th><td>{_e(r['status'])}</td>"
             f"<td>{'AI summary' if r['ai'] else 'Facts only'}</td><td class=\"num\">{_fmt_int(r['signals'] or 0)}</td>"
             f"<td class=\"num\">{float(r['cost_usd'] or 0):.4f}</td><td>{_time(r['updated_at'])}</td></tr>"
             for r in snap.digests]
    digests = _table("digests", "Telegram digests, newest first",
                     [("Digest", False), ("Status", False), ("Kind", False), ("Signals", True), ("Cost (USD)", True),
                      ("Sent (UTC)", False)], drows, "Digests table")
    held = sum(snap.withheld.values())
    note = (f'<p class="muted">{held} row(s) or domain label(s) naming a client are left out of this site.</p>'
            if held else "")
    body = (
        "<h1>X overview</h1>"
        f'<p class="lede">Data as of {_fmt_date(snap.as_of)} in Bangkok (newest signal {_time(snap.newest)}). '
        f"Windows end on that date, not today.</p>"
        f'<dl class="kpis">{tiles}</dl>{note}'
        f"<h2>Domains, last {KPI_DAYS} days</h2>{domains if rows else '<p>No signals in this window.</p>'}"
        f"<h2>Signals per day, last {CHART_DAYS} days</h2>{_chart(snap)}"
        f"<h2>Digests</h2>{digests if drows else '<p>No digests sent yet.</p>'}"
    )
    return _page("Overview", "/", body, snap)


def render_signals(snap: Snapshot) -> str:
    items = []
    for r in snap.signals:
        text = _cut(r["post_text"])
        href = _safe_href(r["post_url"])
        label = escape(text) if text else "(no text)"
        link = (f'<a href="{escape(href)}" rel="noopener noreferrer"{_lang(text)}>{label}</a>' if href
                else f"<span{_lang(text)}>{label}</span>")
        tags = " · ".join(escape(str(v)) for v in (r["domain"], r["signal_type"], r["alert_tier"], r["cred_tier"])
                          if v not in (None, "", "none"))
        items.append(
            f"<li><p>{link}</p><p class=\"muted\">@{_e(r['author_handle'])} · {_time(r['collected_ict'])} · "
            f"priority {_e(r['priority'])} · score {_e(r['opportunity_score'])}"
            f"{' · ' + tags if tags else ''}</p>"
            + (f"<p>Suggested: <span{_lang(str(r['suggested_action']))}>{escape(_cut(r['suggested_action']))}</span></p>"
               if r["suggested_action"] else "")
            + "</li>")
    held = snap.withheld.get("signals", 0)
    body = (
        "<h1>Top signals</h1>"
        f'<p class="lede">The {TOP_SIGNALS} highest-priority signals of the last {KPI_DAYS} days '
        "(urgent alerts first, then digest alerts, opportunity score, source tier and engagement). "
        "Links open the post on X.</p>"
        + (f'<p class="muted">{held} signal(s) naming a client are left out.</p>' if held else "")
        + (f"<ol>{''.join(items)}</ol>" if items else "<p>No signals in this window.</p>")
    )
    return _page("Top signals", "/signals/", body, snap)


def render_claims(snap: Snapshot) -> str:
    rows = []
    for r in snap.claims:
        text = _cut(r["claim"])
        href = _safe_href(r["first_seen_link"])
        cell = (f'<a href="{escape(href)}" rel="noopener noreferrer"{_lang(text)}>{escape(text)}</a>' if href
                else f"<span{_lang(text)}>{_e(text)}</span>")
        nxt = r["next_check"]
        rows.append(f"<tr><td>{cell}</td><td>{_e(r['verification_status'])}</td><td>{_e(r['queue_state'])}</td>"
                    f"<td class=\"num\">{_e(r['priority'])}</td><td>{nxt.strftime('%Y-%m-%d') if nxt else '—'}</td>"
                    f"<td>{_e(r['checked_by'])}</td></tr>")
    table = _table("claims", "Open claims, by priority",
                   [("Claim", False), ("Verification", False), ("Queue", False), ("Priority", True),
                    ("Next check", False), ("Checked by", False)], rows, "Claims table")
    held = snap.withheld.get("claims", 0)
    body = ("<h1>Claims queue</h1>"
            '<p class="lede">Claims still open for verification. Links open where the claim was first seen.</p>'
            + (f'<p class="muted">{held} claim(s) naming a client are left out.</p>' if held else "")
            + (table if rows else "<p>No open claims.</p>"))
    return _page("Claims", "/claims/", body, snap)


def render_not_found(snap: Snapshot) -> str:
    body = '<h1>Page not found</h1><p>That page does not exist. <a href="/">Back to the overview</a>.</p>'
    return _page("Page not found", "", body, snap)


def build_site(snap: Snapshot, out_dir: Path) -> dict[str, Any]:
    pages = {
        "index.html": render_overview(snap),
        "signals/index.html": render_signals(snap),
        "claims/index.html": render_claims(snap),
        "404.html": render_not_found(snap),
    }
    if out_dir.exists():
        shutil.rmtree(out_dir)
    for rel, html in pages.items():
        target = out_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(html, encoding="utf-8")
    shutil.copytree(JOBS_ASSETS, out_dir / "assets", ignore=SKIP_ASSETS)
    (out_dir / "_headers").write_text(HEADERS.replace("script-src 'self'", "script-src 'none'"), encoding="utf-8")
    return {"pages": len(pages), "signals": len(snap.signals), "claims": len(snap.claims),
            "withheld": snap.withheld, "mark": deploy_mark(out_dir)}


def deploy_mark(out_dir: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(x for x in out_dir.rglob("*") if x.is_file()):
        h.update(p.relative_to(out_dir).as_posix().encode() + b"\0" + p.read_bytes() + b"\0")
    return f"x-insights@{h.hexdigest()[:16]}"
