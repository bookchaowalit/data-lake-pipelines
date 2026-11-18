#!/usr/bin/env python3
"""Offline tests for the open-feeds collector (spec: docs/specs/open-feeds.md)."""

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import open_feeds as of  # noqa: E402
from data_lake.open_feeds import (  # noqa: E402
    REGISTRY,
    Feed,
    FeedError,
    RegistryError,
    Response,
    build_rows,
    capture,
    due,
    fetch_feed,
    load_registry,
    main,
    opml_rows,
    parse_html,
    parse_json,
    parse_registry,
    parse_rss,
)

NOW = datetime(2026, 11, 18, 9, 0, tzinfo=timezone.utc)

RSS = b"""<?xml version="1.0"?>
<rss version="2.0"><channel><title>T</title>
<item><title>First &amp; best</title><link>https://ex.test/a</link><guid>a-1</guid>
<pubDate>Tue, 18 Nov 2026 08:00:00 GMT</pubDate><author>someone@example.com (Some One)</author>
<category>Tech</category><description>&lt;p&gt;Summary &lt;b&gt;one&lt;/b&gt;&lt;/p&gt;</description></item>
<item><title>Second</title><link>/b</link></item>
</channel></rss>"""

ATOM = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"><title>A</title>
<entry><title>Atom one</title><id>urn:1</id><link href="https://ex.test/1"/><updated>2026-11-18T07:00:00Z</updated></entry>
</feed>"""

BOMB = b"""<?xml version="1.0"?><!DOCTYPE r [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;">]><rss><channel><item><title>&b;</title></item></channel></rss>"""


def feed(**kw) -> Feed:
    base = dict(
        id="f1", name="F1", kind="rss", url="https://ex.test/feed", category="tech"
    )
    base.update(kw)
    return Feed(**base)


class FakeFetch:
    """Serves canned bodies by URL; records the headers of each request."""

    def __init__(self, bodies: dict[str, object]):
        self.bodies = bodies
        self.calls: list[tuple[str, dict]] = []

    def __call__(
        self, url: str, headers: dict[str, str], follow_redirects: bool = True
    ) -> Response:
        self.calls.append((url, dict(headers)))
        value = self.bodies.get(url)
        if isinstance(value, list):
            value = value.pop(0) if len(value) > 1 else value[0]
        if value is None:
            raise FeedError("HTTP 404", status=404)
        if isinstance(value, Exception):
            raise value
        if isinstance(value, Response):
            return value
        return Response(200, value, etag='"v1"', final_url=url)


class RegistryTest(unittest.TestCase):
    def ok_row(self, **kw):
        row = {
            "id": "ok-feed",
            "kind": "rss",
            "url": "https://ex.test/rss",
            "category": "tech",
        }
        row.update(kw)
        return row

    def test_valid_row(self):
        (f,) = parse_registry(
            {"feeds": [self.ok_row(every_hours=6, keep="full", subcategory="x")]}
        )
        self.assertEqual(
            (f.every_hours, f.keep, f.subcategory, f.enabled), (6, "full", "x", True)
        )

    def test_rejects(self):
        bad = [
            self.ok_row(url="http://ex.test/rss"),
            self.ok_row(url="file:///etc/passwd"),
            self.ok_row(id="Bad Id"),
            self.ok_row(kind="ftp"),
            self.ok_row(every_hours=5),
            self.ok_row(keep="everything"),
            self.ok_row(category=""),
            self.ok_row(typo_field=1),
            self.ok_row(kind="json"),  # needs items or snapshot
            self.ok_row(kind="html"),  # needs an item selector
        ]
        for row in bad:
            with self.subTest(row=row), self.assertRaises(RegistryError):
                parse_registry({"feeds": [row]})
        with self.assertRaises(RegistryError):
            parse_registry({"feeds": [self.ok_row(), self.ok_row()]})
        with self.assertRaises(RegistryError):
            parse_registry({"rows": []})
        for row in [
            self.ok_row(url="https://www.producthunt.com/feed"),  # blocked-legal
            self.ok_row(
                url="https://old.reddit.com/r/x/.rss"
            ),  # later (Data API terms)
            self.ok_row(max_items=0),
            self.ok_row(max_items=99999),
        ]:
            with self.subTest(row=row), self.assertRaises(RegistryError):
                parse_registry({"feeds": [row]})
        # A blocked host may stay documented when disabled.
        (f,) = parse_registry(
            {
                "feeds": [
                    self.ok_row(
                        url="https://www.producthunt.com/feed", enabled=False, note="x"
                    )
                ]
            }
        )
        self.assertFalse(f.enabled)

    def test_shipped_registry(self):
        feeds = load_registry(REGISTRY)
        enabled = [f for f in feeds if f.enabled]
        self.assertGreater(len(enabled), 500)
        self.assertTrue(all(f.url.startswith("https://") for f in feeds))
        # Reddit is `later` in source-coverage.yaml (Data API terms): never fetched from here.
        self.assertFalse([f.id for f in feeds if "reddit.com" in f.host])
        self.assertFalse([f.id for f in enabled if of.blocked_host(f.host)])
        usgs = next(f for f in feeds if f.id == "usgs-quakes")
        self.assertEqual(usgs.spec.get("changes"), ["properties.updated"])
        import yaml

        rows = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))["feeds"]
        self.assertFalse(
            [r["id"] for r in rows if r.get("enabled") is False and not r.get("note")]
        )


class ParseTest(unittest.TestCase):
    def test_rss_headline_only(self):
        items = parse_rss(RSS, feed())
        self.assertEqual(len(items), 2)
        first = items[0]
        self.assertEqual(first["title"], "First & best")
        self.assertEqual(first["link"], "https://ex.test/a")
        self.assertEqual(first["guid"], "a-1")
        self.assertEqual(first["published_at"], "2026-11-18T08:00:00+00:00")
        self.assertEqual(first["extra"], {"tags": ["Tech"]})  # no summary, no author
        self.assertNotIn("someone", json.dumps(items))
        self.assertEqual(
            items[1]["link"], "https://ex.test/b"
        )  # relative link resolved

    def test_rss_full_keeps_clean_summary(self):
        first = parse_rss(RSS, feed(keep="full"))[0]
        self.assertEqual(first["extra"]["summary"], "Summary one")

    def test_atom(self):
        (item,) = parse_rss(ATOM, feed())
        self.assertEqual(
            (item["guid"], item["link"], item["published_at"]),
            ("urn:1", "https://ex.test/1", "2026-11-18T07:00:00+00:00"),
        )

    def test_html_page_is_not_a_feed(self):
        page = b"<html><head><title>Down for maintenance</title></head><body>soon</body></html>"
        with self.assertRaises(FeedError):
            parse_rss(page, feed())

    def test_valid_empty_feed_is_ok(self):
        self.assertEqual(
            parse_rss(
                b'<?xml version="1.0"?><rss version="2.0"><channel><title>T</title></channel></rss>',
                feed(),
            ),
            [],
        )

    def test_relative_links_use_the_final_url(self):
        items = parse_rss(RSS, feed(), "https://moved.test/news/feed")
        self.assertEqual(items[1]["link"], "https://moved.test/b")

    def test_entity_bomb_refused(self):
        with self.assertRaises(FeedError):
            parse_rss(BOMB, feed())

    def test_json_items(self):
        body = json.dumps(
            {
                "data": {
                    "rows": [
                        {
                            "code": "X1",
                            "name": "<b>Alpha</b>",
                            "t": 1763452800,
                            "score": 7,
                            "secret": "no",
                        },
                        {"code": "X2", "name": "Beta", "t": "2026-11-18", "score": 3},
                        "not-a-row",
                    ]
                }
            }
        ).encode()
        f = feed(
            kind="json",
            spec={
                "items": "data.rows",
                "id": "code",
                "title": "name",
                "link_template": "https://ex.test/p/{code}",
                "time": "t",
                "keep": ["score"],
            },
        )
        items = parse_json(body, f)
        self.assertEqual(
            [i["guid"] for i in items], ["X1", "X2", None]
        )  # "not-a-row" = skipped
        self.assertEqual(items[0]["title"], "Alpha")
        self.assertEqual(items[0]["link"], "https://ex.test/p/X1")
        self.assertEqual(items[0]["published_at"], "2025-11-18T08:00:00+00:00")
        self.assertEqual(items[1]["published_at"], "2026-11-18T00:00:00+00:00")
        self.assertEqual(items[0]["extra"], {"score": 7})

    def test_json_missing_id_is_skipped_not_merged(self):
        body = json.dumps([{"a": 1}, {"b": 2}, {"b": 3}, "x"]).encode()
        items = parse_json(body, feed(kind="json", spec={"items": "", "id": "a"}))
        self.assertEqual([i["guid"] for i in items], ["1", None, None, None])
        res = fetch_feed(
            feed(kind="json", spec={"items": "", "id": "a"}),
            FakeFetch({"https://ex.test/feed": body}),
            {},
            None,
            retry_pause=0,
        )
        self.assertEqual((res.status, len(res.items), res.skipped), ("ok", 1, 3))

    def test_json_changes_make_a_new_version(self):
        f = feed(
            kind="json",
            spec={"items": "1", "id": ["indicator.id", "date"], "changes": ["value"]},
        )
        before = parse_json(
            json.dumps(
                [{}, [{"indicator": {"id": "GDP"}, "date": "2025", "value": None}]]
            ).encode(),
            f,
        )
        after = parse_json(
            json.dumps(
                [{}, [{"indicator": {"id": "GDP"}, "date": "2025", "value": 5.1}]]
            ).encode(),
            f,
        )
        self.assertTrue(before[0]["guid"].startswith("GDP|2025#"))
        self.assertNotEqual(before[0]["guid"], after[0]["guid"])

    def test_bracket_path_for_dotted_keys(self):
        row = {
            "_meta": {
                "io.example.registry/official": {"publishedAt": "2026-11-18T01:00:00Z"}
            },
            "n": "x",
        }
        f = feed(
            kind="json",
            spec={
                "items": "",
                "id": "n",
                "time": '_meta["io.example.registry/official"].publishedAt',
            },
        )
        (item,) = parse_json(json.dumps([row]).encode(), f)
        self.assertEqual(item["published_at"], "2026-11-18T01:00:00+00:00")

    def test_max_items_truncation_is_reported(self):
        body = json.dumps([{"a": i} for i in range(10)]).encode()
        res = fetch_feed(
            feed(kind="json", max_items=4, spec={"items": "", "id": "a"}),
            FakeFetch({"https://ex.test/feed": body}),
            {},
            None,
            retry_pause=0,
        )
        self.assertEqual((len(res.items), res.truncated), (4, 6))
        _, runs, _ = build_rows([res], {}, "t")
        self.assertEqual((runs[0]["items_truncated"], runs[0]["items_skipped"]), (6, 0))

    def test_lone_surrogate_is_one_feed_error(self):
        body = b'[{"a": "x\\ud800y"}]'
        res = fetch_feed(
            feed(kind="json", spec={"items": "", "id": "a"}),
            FakeFetch({"https://ex.test/feed": body}),
            {},
            None,
            retry_pause=0,
        )
        self.assertEqual((res.status, res.error), ("error", "text is not valid UTF-8"))

    def test_json_root_list_and_composite_id(self):
        body = json.dumps([{"a": 1, "b": {"c": "x"}}]).encode()
        (item,) = parse_json(
            body, feed(kind="json", spec={"items": "", "id": ["a", "b.c"]})
        )
        self.assertEqual(item["guid"], "1|x")

    def test_json_snapshot_changes_only(self):
        f = feed(kind="json", spec={"snapshot": True, "path": "rates", "time": "date"})
        one = parse_json(b'{"date":"2026-11-18","rates":{"THB":35.1}}', f)
        same = parse_json(b'{"rates":{"THB":35.1},"date":"2026-11-18"}', f)
        other = parse_json(b'{"date":"2026-11-18","rates":{"THB":35.2}}', f)
        self.assertEqual(one[0]["guid"], same[0]["guid"])
        self.assertNotEqual(one[0]["guid"], other[0]["guid"])
        self.assertEqual(one[0]["extra"], {"payload": {"THB": 35.1}})

    def test_json_errors(self):
        with self.assertRaises(FeedError):
            parse_json(b"<html>", feed(kind="json", spec={"items": ""}))
        with self.assertRaises(FeedError):
            parse_json(b'{"a": 1}', feed(kind="json", spec={"items": "a"}))

    def test_html(self):
        body = b"""<html><article class="row"><h2><a href="/o/r">o / r</a></h2><p>desc</p></article>
        <article class="row"><h2><a href="javascript:alert(1)">bad</a></h2></article></html>"""
        f = feed(
            kind="html",
            url="https://ex.test/trending",
            spec={
                "item": "article.row",
                "title": "h2 a",
                "link": "h2 a",
                "fields": {"description": "p"},
            },
        )
        items = parse_html(body, f)
        self.assertEqual(items[0]["link"], "https://ex.test/o/r")
        self.assertEqual(items[0]["title"], "o / r")
        self.assertEqual(items[0]["extra"], {"description": "desc"})
        self.assertIsNone(items[1]["link"])  # only http(s) links are kept

    def test_iso(self):
        self.assertEqual(
            of._iso("2026-11-18T10:00:00+07:00"), "2026-11-18T03:00:00+00:00"
        )
        self.assertEqual(of._iso(1763452800000), "2025-11-18T08:00:00+00:00")
        self.assertIsNone(of._iso("not a date"))
        self.assertIsNone(of._iso(True))

    def test_opml(self):
        body = b'<opml><body><outline text="g"><outline title="A" xmlUrl="http://a.test/rss"/><outline title="B" xmlUrl="ftp://b"/></outline></body></opml>'
        rows = opml_rows(body, "news")
        parse_registry({"feeds": rows})  # importer output is a valid registry
        self.assertEqual(
            rows[0]["id"], "a-test-rss"
        )  # a one-letter title falls back to the URL
        thai = "\u0e02\u0e48\u0e32\u0e27"
        two = f'<opml><body><outline title="{thai}" xmlUrl="https://th.test/rss"/><outline title="{thai}" xmlUrl="https://th.test/rss2"/><outline title="P" xmlUrl="https://www.producthunt.com/feed"/></body></opml>'.encode()
        self.assertEqual(
            [r["id"] for r in opml_rows(two, "news")], ["th-test-rss", "th-test-rss2"]
        )
        self.assertEqual(
            [{**r, "id": "a"} for r in rows],
            [
                {
                    "id": "a",
                    "name": "A",
                    "kind": "rss",
                    "url": "https://a.test/rss",
                    "category": "news",
                }
            ],
        )


class RunTest(unittest.TestCase):
    def test_dedupe_across_runs(self):
        res = fetch_feed(
            feed(), FakeFetch({"https://ex.test/feed": RSS}), {}, None, retry_pause=0
        )
        items, runs, state = build_rows([res], {}, "t1")
        self.assertEqual(len(items), 2)
        self.assertEqual(runs[0]["items_new"], 2)
        items2, runs2, _ = build_rows([res], state, "t2")
        self.assertEqual(items2, [])
        self.assertEqual((runs2[0]["items_seen"], runs2[0]["items_new"]), (2, 0))
        self.assertTrue(items[0]["id"].startswith("f1:"))

    def test_due(self):
        f = feed(every_hours=6)
        self.assertTrue(due(f, {}, NOW))
        self.assertFalse(
            due(f, {"f1": {"last_fetch": (NOW - timedelta(hours=2)).isoformat()}}, NOW)
        )
        self.assertTrue(
            due(
                f,
                {
                    "f1": {
                        "last_fetch": (NOW - timedelta(hours=5, minutes=55)).isoformat()
                    }
                },
                NOW,
            )
        )

    def test_conditional_get_and_304(self):
        fetch = FakeFetch({"https://ex.test/feed": Response(304, b"")})
        res = fetch_feed(
            feed(), fetch, {"etag": '"v1"', "last_modified": "Tue"}, None, retry_pause=0
        )
        self.assertEqual(res.status, "not-modified")
        self.assertEqual(
            fetch.calls[0][1], {"If-None-Match": '"v1"', "If-Modified-Since": "Tue"}
        )
        self.assertEqual(res.etag, '"v1"')

    def test_retry_transient_once(self):
        fetch = FakeFetch({"https://ex.test/feed": [FeedError("HTTP 503"), RSS]})
        self.assertEqual(
            fetch_feed(feed(), fetch, {}, None, retry_pause=0).status, "ok"
        )
        fetch = FakeFetch({"https://ex.test/feed": [FeedError("HTTP 404"), RSS]})
        res = fetch_feed(feed(), fetch, {}, None, retry_pause=0)
        self.assertEqual(
            (res.status, res.error, len(fetch.calls)), ("error", "HTTP 404", 1)
        )

    def test_broken_parser_is_one_error_not_a_crash(self):
        res = fetch_feed(
            feed(kind="json", spec={"items": "x"}),
            FakeFetch({"https://ex.test/feed": b"[]"}),
            {},
            None,
            retry_pause=0,
        )
        self.assertEqual(res.status, "error")

    def test_robots(self):
        page = b'<article class="row"><a href="/x">x</a></article>'
        f = feed(
            kind="html", url="https://ex.test/trending", spec={"item": "article.row"}
        )
        deny = FakeFetch(
            {
                "https://ex.test/robots.txt": b"User-agent: *\nDisallow: /trending",
                "https://ex.test/trending": page,
            }
        )
        self.assertEqual(
            of.fetch_all([f], deny, {}, pause=0)[0].status, "robots-denied"
        )
        missing = FakeFetch(
            {"https://ex.test/trending": page}
        )  # robots.txt 404 = no rules
        self.assertEqual(of.fetch_all([f], missing, {}, pause=0)[0].status, "ok")
        unknown = FakeFetch(
            {
                "https://ex.test/robots.txt": FeedError("HTTP 403"),
                "https://ex.test/trending": page,
            }
        )
        self.assertEqual(
            of.fetch_all([f], unknown, {}, pause=0)[0].status, "robots-denied"
        )

    def test_robots_checked_on_every_redirect_hop(self):
        page = b'<article class="row"><a href="/x">x</a></article>'
        f = feed(
            kind="html", url="https://ex.test/trending", spec={"item": "article.row"}
        )
        hop = Response(301, b"", final_url="https://ex.test/private/trending")
        fetch = FakeFetch(
            {
                "https://ex.test/robots.txt": b"User-agent: *\nDisallow: /private",
                "https://ex.test/trending": hop,
                "https://ex.test/private/trending": page,
            }
        )
        res = of.fetch_all([f], fetch, {}, pause=0)[0]
        self.assertEqual(res.status, "error")
        self.assertIn("robots", res.error)
        self.assertNotIn(
            "https://ex.test/private/trending", [c[0] for c in fetch.calls]
        )
        ok = FakeFetch(
            {
                "https://ex.test/robots.txt": b"",
                "https://ex.test/trending": Response(
                    302, b"", final_url="https://ex.test/new"
                ),
                "https://ex.test/new": page,
            }
        )
        res = of.fetch_all([f], ok, {}, pause=0)[0]
        self.assertEqual(
            (res.status, res.items[0]["link"]), ("ok", "https://ex.test/x")
        )

    def test_validators_only_for_the_url_that_issued_them(self):
        fetch = FakeFetch({"https://ex.test/feed": RSS})
        fetch_feed(
            feed(),
            fetch,
            {"etag": '"old"', "url": "https://elsewhere.test/rss"},
            None,
            retry_pause=0,
        )
        self.assertEqual(fetch.calls[0][1], {})
        res = fetch_feed(feed(), fetch, {}, None, retry_pause=0)
        _, _, state = build_rows([res], {}, "t")
        self.assertEqual(state["f1"]["validator_url"], "https://ex.test/feed")
        # After a redirect the validators belong to the final URL and are sent there only.
        moved = FakeFetch(
            {
                "https://ex.test/feed": Response(
                    301, b"", final_url="https://cdn.test/feed"
                ),
                "https://cdn.test/feed": RSS,
            }
        )
        res = fetch_feed(feed(), moved, {}, None, retry_pause=0)
        _, _, state = build_rows([res], {}, "t")
        self.assertEqual(state["f1"]["validator_url"], "https://cdn.test/feed")
        fetch_feed(feed(), moved, state["f1"], None, retry_pause=0)
        self.assertEqual(moved.calls[-2], ("https://ex.test/feed", {}))
        self.assertEqual(moved.calls[-1][1], {"If-None-Match": '"v1"'})

    def test_redirect_to_blocked_host_refused(self):
        fetch = FakeFetch(
            {
                "https://ex.test/feed": Response(
                    302, b"", final_url="https://www.reddit.com/r/x/.rss"
                )
            }
        )
        res = fetch_feed(feed(), fetch, {}, None, retry_pause=0)
        self.assertEqual(res.status, "error")
        self.assertEqual(len(fetch.calls), 1)  # the blocked host is never requested

    def test_http_status_kept_when_parse_fails(self):
        res = fetch_feed(
            feed(),
            FakeFetch({"https://ex.test/feed": b"<html><title>x</title></html>"}),
            {},
            None,
            retry_pause=0,
        )
        self.assertEqual(
            (res.status, res.http_status, res.error), ("error", 200, "not a feed")
        )

    def test_truncated_rss_is_an_error(self):
        res = fetch_feed(
            feed(),
            FakeFetch(
                {
                    "https://ex.test/feed": b'<?xml version="1.0"?><rss version="2.0"><channel><title>T'
                }
            ),
            {},
            None,
            retry_pause=0,
        )
        self.assertEqual((res.status, res.error), ("error", "malformed feed"))

    def test_budget_defers_unstarted_feeds(self):
        feeds = [feed(id=f"s{i}", url=f"https://slow.test/{i}") for i in range(3)]
        results = of.fetch_all(
            feeds,
            FakeFetch({f"https://slow.test/{i}": RSS for i in range(3)}),
            {},
            pause=0,
            budget=0,
        )
        self.assertEqual(results, [])  # nothing started: all stay due, state untouched

    def test_batches_split_by_size(self):
        rows = [{"id": str(i), "t": "x" * 100} for i in range(50)]
        parts = of.batches(rows, limit=1000)
        self.assertGreater(len(parts), 1)
        self.assertEqual(sum(len(p) for p in parts), 50)
        self.assertEqual(of.batches([]), [])

    def test_state_belongs_to_one_lake(self):
        with tempfile.TemporaryDirectory() as tmp:
            sp = Path(tmp) / "s.gz"
            of.write_state(
                sp, {"f1": {"seen": ["x"]}}, "t", of.lake_fingerprint("s3://a/b")
            )
            self.assertEqual(
                of.read_state(sp, of.lake_fingerprint("s3://a/b")),
                {"f1": {"seen": ["x"]}},
            )
            self.assertEqual(
                of.read_state(sp, of.lake_fingerprint("s3://other/lake")), {}
            )

    def test_http_status_kept_on_error(self):
        res = fetch_feed(
            feed(),
            FakeFetch({"https://ex.test/feed": FeedError("HTTP 410", status=410)}),
            {},
            None,
            retry_pause=0,
        )
        self.assertEqual((res.status, res.http_status), ("error", 410))

    def test_only_keeps_the_state_of_other_feeds(self):
        feeds = [feed(), feed(id="f2", url="https://other.test/atom")]
        bodies = {"https://ex.test/feed": RSS, "https://other.test/atom": ATOM}
        with tempfile.TemporaryDirectory() as lake:
            sp = Path(lake) / "s.gz"
            capture(
                feeds, FakeFetch(dict(bodies)), lake, state_path=sp, now=NOW, pause=0
            )
            r = capture(
                feeds,
                FakeFetch(dict(bodies)),
                lake,
                only={"f1"},
                force=True,
                state_path=sp,
                now=NOW + timedelta(hours=1),
                pause=0,
            )
            self.assertEqual(r["feeds_due"], 1)
            self.assertEqual(set(of.read_state(sp)), {"f1", "f2"})

    def test_report_without_items(self):
        with tempfile.TemporaryDirectory() as lake:
            capture(
                [feed()],
                FakeFetch({}),
                lake,
                state_path=Path(lake) / "s.gz",
                now=NOW,
                pause=0,
            )
            rep = of.report(lake, days=100000, search="x")
            self.assertEqual((rep["totals"]["items"], rep["search"]), (0, []))
            self.assertEqual(rep["failing_feeds"][0]["http_status"], 404)

    def test_report_ignores_parts_without_manifest(self):
        with tempfile.TemporaryDirectory() as lake:
            capture(
                [feed()],
                FakeFetch({"https://ex.test/feed": RSS}),
                lake,
                state_path=Path(lake) / "s.gz",
                now=NOW,
                pause=0,
            )
            # A part left by a run that stopped before its commit manifest.
            part = next(Path(lake).rglob("dataset=feed_items/**/*.parquet"))
            (part.parent / "part-orphan.parquet").write_bytes(part.read_bytes())
            self.assertEqual(of.report(lake, days=100000)["totals"]["items"], 2)
            self.assertEqual(len(of.committed_parts(lake, "feed_items")), 1)

    def test_http_fetch_refuses_non_https(self):
        with self.assertRaises(FeedError):
            of.http_fetch()("http://ex.test/feed", {})

    def test_capture_into_local_lake(self):
        feeds = [
            feed(),
            feed(id="f2", url="https://other.test/atom"),
            feed(id="f3", url="https://gone.test/x"),
        ]
        bodies = {"https://ex.test/feed": RSS, "https://other.test/atom": ATOM}
        with tempfile.TemporaryDirectory() as lake:
            sp = Path(lake) / "state" / "s.json.gz"
            r1 = capture(
                feeds, FakeFetch(dict(bodies)), lake, state_path=sp, now=NOW, pause=0
            )
            self.assertEqual(
                (r1["feeds_due"], r1["ok"], r1["errors"], r1["items_new"]), (3, 2, 1, 3)
            )
            self.assertEqual(r1["failed_feeds"], ["f3"])
            self.assertEqual(set(r1["manifests"]), {"feed_items", "feed_runs"})
            self.assertFalse(r1["state_loaded"])
            state = of.read_state(sp)
            self.assertEqual(state["f1"]["etag"], '"v1"')
            self.assertEqual(state["f3"]["errors"], 1)
            # One hour later every feed is due again; nothing new arrives.
            r2 = capture(
                feeds,
                FakeFetch(dict(bodies)),
                lake,
                state_path=sp,
                now=NOW + timedelta(hours=1),
                pause=0,
            )
            self.assertTrue(r2["state_loaded"])
            self.assertEqual(r2["items_new"], 0)
            self.assertEqual(set(r2["manifests"]), {"feed_runs"})
            # Ten minutes later nothing is due: no Bronze write at all.
            r3 = capture(
                feeds,
                FakeFetch(dict(bodies)),
                lake,
                state_path=sp,
                now=NOW + timedelta(hours=1, minutes=10),
                pause=0,
            )
            self.assertEqual((r3["feeds_due"], r3["manifests"]), (0, {}))
            # A lost state file re-sends what the feeds still show (Silver dedupes on id).
            r4 = capture(
                feeds,
                FakeFetch(dict(bodies)),
                lake,
                state_path=Path(lake) / "gone.gz",
                now=NOW + timedelta(hours=2),
                pause=0,
            )
            self.assertEqual(r4["items_new"], 3)
            sp.write_bytes(b"not gzip")
            self.assertEqual(of.read_state(sp), {})
            # DuckDB report on Bronze: one row per item id even though r4 re-sent all three.
            rep = of.report(lake, days=100000, search="atom")
            self.assertEqual((rep["totals"]["items"], rep["totals"]["feeds"]), (3, 2))
            self.assertEqual([r["title"] for r in rep["search"]], ["Atom one"])
            self.assertEqual(rep["failing_feeds"][0]["feed_id"], "f3")

    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as lake:
            report = capture(
                [feed()],
                FakeFetch({"https://ex.test/feed": RSS}),
                lake,
                state_path=Path(lake) / "s.gz",
                now=NOW,
                dry_run=True,
                pause=0,
            )
            self.assertEqual((report["status"], report["items_new"]), ("dry-run", 2))
            self.assertEqual(list(Path(lake).rglob("*")), [])


class MainTest(unittest.TestCase):
    def run_main(self, argv, fetch):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = main(argv, fetch=fetch, pause=0)
        return code, json.loads(out.getvalue())

    def registry(self, folder: str, rows) -> str:
        path = Path(folder) / "reg.yaml"
        path.write_text(json.dumps({"feeds": rows}))  # JSON is valid YAML
        return str(path)

    def test_exit_codes(self):
        with tempfile.TemporaryDirectory() as tmp:
            reg = self.registry(
                tmp,
                [
                    {
                        "id": "a1",
                        "kind": "rss",
                        "url": "https://a.test/rss",
                        "category": "x",
                    },
                    {
                        "id": "b1",
                        "kind": "rss",
                        "url": "https://b.test/rss",
                        "category": "x",
                    },
                ],
            )
            lake = str(Path(tmp) / "lake")
            st = ["--state-file", str(Path(tmp) / "s.gz")]
            code, report = self.run_main(
                ["--registry", reg, "run", "--data-lake-uri", lake, *st],
                FakeFetch({"https://a.test/rss": RSS}),
            )
            self.assertEqual(
                (code, report["errors"]), (0, 1)
            )  # half failing is still a run
            code, report = self.run_main(
                ["--registry", reg, "--data-lake-uri", lake, "--force", *st],
                FakeFetch({}),
            )
            self.assertEqual(code, 3)  # every feed failing = broken runner/network
            # Re-run inside the hour: nothing is due, but the latest fetch of every feed failed.
            code, report = self.run_main(
                ["--registry", reg, "--data-lake-uri", lake, *st], FakeFetch({})
            )
            self.assertEqual((code, report["feeds_due"]), (3, 0))
            code, report = self.run_main(
                ["--registry", reg, "--data-lake-uri", lake, "--only", "a1,typo", *st],
                FakeFetch({}),
            )
            self.assertEqual((code, report["error"]), (2, "RegistryError"))
            code, report = self.run_main(
                ["--registry", reg, "run", "--require-cloud", "--data-lake-uri", lake],
                FakeFetch({}),
            )
            self.assertEqual((code, report["error"]), (2, "LakeUriNotConfigured"))
            code, report = self.run_main(["--registry", reg, "check"], FakeFetch({}))
            self.assertEqual((code, report["feeds"]), (0, 2))
            bad = self.registry(
                tmp,
                [{"id": "a1", "kind": "rss", "url": "http://a.test", "category": "x"}],
            )
            code, report = self.run_main(["--registry", bad, "check"], FakeFetch({}))
            self.assertEqual((code, report["error"]), (2, "RegistryError"))


if __name__ == "__main__":
    unittest.main()
