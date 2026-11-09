#!/usr/bin/env python3
"""Revenue page: one sales Gold snapshot (DuckDB) → plain HTML (docs/specs/sales-marts.md item 11).

Script-free like the news site, with the jobs site's CSS, design-floor tokens and
strict CSP. Money stays per currency (never added across currencies) and is
shown in major units; ids are shortened to 8 characters. Same snapshot, same bytes.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import timedelta
from html import escape
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.jobs_bi_site import ASSETS as JOBS_ASSETS  # noqa: E402
from data_lake.jobs_bi_site import HEADERS, STYLESHEETS, _fmt_date, _fmt_int, _fmt_time, _rows, _table  # noqa: E402

SITE_DIR = ROOT / "infra" / "bi" / "sales"
DEFAULT_DATABASE = SITE_DIR / "sales.duckdb"
DEFAULT_OUT = SITE_DIR / "build"
SITE_NAME = "Revenue"
NAV = [("/", "Overview"), ("/sales/", "Recent sales")]
WINDOW_DAYS = 30
RECENT = 20
# ISO 4217 minor-unit exponents that are not 2; any other code uses the ISO default of 2.
CURRENCY_EXPONENT = {
    **dict.fromkeys(("BIF", "CLP", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW", "PYG", "RWF", "UGX", "UYI",
                     "VND", "VUV", "XAF", "XOF", "XPF"), 0),
    **dict.fromkeys(("BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND"), 3),
    "CLF": 4,
}
SKIP_ASSETS = shutil.ignore_patterns("filter.js")


@dataclass(frozen=True)
class Snapshot:
    meta: dict[str, Any]
    daily: list[dict[str, Any]]
    totals: list[dict[str, Any]]
    recent: list[dict[str, Any]]
    vip: int
    rejected: int


def load_snapshot(database: Path) -> Snapshot:
    import duckdb  # type: ignore

    con = duckdb.connect(str(database), read_only=True)
    try:
        meta = _rows(con, "select gold_run_id, as_of_date, max_received_at from gold_meta")[0]
        as_of = meta["as_of_date"]
        daily = (
            _rows(
                con,
                "select business_date, currency, orders, gross_minor, refunds_booked_minor, net_minor "
                "from sales_daily_revenue where business_date > ? order by business_date desc, currency",
                [as_of - timedelta(days=WINDOW_DAYS)],
            )
            if as_of
            else []
        )
        totals = _rows(
            con,
            "select currency, sum(orders) as orders, sum(gross_minor) as gross_minor, "
            "sum(refunds_booked_minor) as refunds_minor, sum(net_minor) as net_minor "
            "from sales_daily_revenue group by currency order by currency",
        )
        recent = _rows(
            con,
            f"select sale_date, occurred_at, currency, gross_minor, net_minor, customer_tier_at_sale, provider, "
            f"customer_id, intent_id from fact_sales order by occurred_at desc, intent_id limit {RECENT}",
        )
        vip = _rows(con, "select count(*) as n from dim_customer where is_current and tier = 'vip'")[0]["n"]
        rejected = _rows(con, "select count(*) as n from sales_rejected_events")[0]["n"]
    finally:
        con.close()
    return Snapshot(meta, daily, totals, recent, int(vip), int(rejected))


def _money(minor: Any, currency: str) -> str:
    value = int(minor or 0)
    code = currency.upper()
    exponent = CURRENCY_EXPONENT.get(code, 2)
    sign = "-" if value < 0 else ""
    major, fraction = divmod(abs(value), 10**exponent)
    tail = f".{fraction:0{exponent}d}" if exponent else ""
    return f"{sign}{major:,}{tail} {escape(code)}"


def _short(value: Any) -> str:
    return escape(str(value or "")[:8])


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
        f"<footer>Gold run {escape(str(meta['gold_run_id']))}. Built only from the sales Gold marts "
        "(platform-payment events via Kafka and the R2 lake). Stripe test mode.</footer>\n"
        "</body>\n</html>\n"
    )


def render_overview(snapshot: Snapshot) -> str:
    meta = snapshot.meta
    kpis = "".join(
        f'<div class="kpi"><dt>Net revenue, {escape(str(row["currency"]))}</dt>'
        f'<dd data-kpi="net-{escape(str(row["currency"]).lower())}">{_money(row["net_minor"], str(row["currency"]))}</dd></div>'
        for row in snapshot.totals
    )
    kpis += (
        f'<div class="kpi"><dt>Orders</dt><dd data-kpi="orders">{_fmt_int(sum(int(r["orders"]) for r in snapshot.totals))}</dd></div>'
        f'<div class="kpi"><dt>VIP customers</dt><dd data-kpi="vip">{_fmt_int(snapshot.vip)}</dd></div>'
    )
    totals = _table(
        "totals",
        "All time, per currency",
        [("Currency", False), ("Orders", True), ("Gross", True), ("Refunds booked", True), ("Net", True)],
        [
            f"<tr><th scope=\"row\">{escape(str(r['currency']))}</th><td class=\"num\">{_fmt_int(r['orders'])}</td>"
            f"<td class=\"num\">{_money(r['gross_minor'], str(r['currency']))}</td>"
            f"<td class=\"num\">{_money(r['refunds_minor'], str(r['currency']))}</td>"
            f"<td class=\"num\">{_money(r['net_minor'], str(r['currency']))}</td></tr>"
            for r in snapshot.totals
        ],
        "Revenue per currency",
    )
    daily = _table(
        "daily",
        f"Business days (Bangkok), last {WINDOW_DAYS} days",
        [("Date", False), ("Currency", False), ("Orders", True), ("Gross", True), ("Refunds booked", True), ("Net", True)],
        [
            f"<tr><th scope=\"row\">{_fmt_date(r['business_date'])}</th><td>{escape(str(r['currency']))}</td>"
            f"<td class=\"num\">{_fmt_int(r['orders'])}</td><td class=\"num\">{_money(r['gross_minor'], str(r['currency']))}</td>"
            f"<td class=\"num\">{_money(r['refunds_booked_minor'], str(r['currency']))}</td>"
            f"<td class=\"num\">{_money(r['net_minor'], str(r['currency']))}</td></tr>"
            for r in snapshot.daily
        ],
        "Daily revenue table",
    )
    warning = (
        f'<p class="status-quiet">{_fmt_int(snapshot.rejected)} event(s) were rejected by the sales contract '
        "and are not counted; see sales_rejected_events.</p>"
        if snapshot.rejected
        else ""
    )
    body = (
        "<h1>Revenue</h1>"
        f'<p class="lede">Data as of {_fmt_date(meta["as_of_date"])} (Bangkok business date; newest event received '
        f"{_fmt_time(meta['max_received_at'])} UTC). Amounts are per currency and never added together. "
        "Net = gross minus refunds of those sales.</p>"
        f"{warning}"
        + (f'<dl class="kpis">{kpis}</dl><h2>Totals</h2>{totals}<h2>Daily revenue</h2>{daily if snapshot.daily else "<p>No sales in this window.</p>"}'
           if snapshot.totals else "<p>No sales yet.</p>")
    )
    return _page("Overview", "/", body, meta)


def render_recent(snapshot: Snapshot) -> str:
    rows = [
        f"<tr><th scope=\"row\">{_fmt_date(r['sale_date'])}</th><td>{_fmt_time(r['occurred_at'])}</td>"
        f"<td class=\"num\">{_money(r['gross_minor'], str(r['currency']))}</td>"
        f"<td class=\"num\">{_money(r['net_minor'], str(r['currency']))}</td>"
        f"<td>{escape(str(r['customer_tier_at_sale'] or '—'))}</td><td>{escape(str(r['provider'] or '—'))}</td>"
        f"<td><code>{_short(r['customer_id'])}</code></td><td><code>{_short(r['intent_id'])}</code></td></tr>"
        for r in snapshot.recent
    ]
    body = (
        "<h1>Recent sales</h1>"
        f'<p class="lede">The newest {RECENT} sales. Customer (workspace) and intent ids are shortened.</p>'
        + (
            _table(
                "recent",
                f"Newest {RECENT} sales",
                [("Date", False), ("At (UTC)", False), ("Gross", True), ("Net", True), ("Tier", False),
                 ("Provider", False), ("Customer", False), ("Intent", False)],
                rows,
                "Recent sales table",
            )
            if rows
            else "<p>No sales yet.</p>"
        )
    )
    return _page("Recent sales", "/sales/", body, snapshot.meta)


def render_not_found(snapshot: Snapshot) -> str:
    body = '<h1>Page not found</h1><p>That page does not exist. <a href="/">Back to the overview</a>.</p>'
    return _page("Page not found", "", body, snapshot.meta)


def build_site(database: Path, out_dir: Path) -> dict[str, Any]:
    snapshot = load_snapshot(database)
    pages = {
        "index.html": render_overview(snapshot),
        "sales/index.html": render_recent(snapshot),
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
