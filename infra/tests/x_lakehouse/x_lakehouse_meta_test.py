#!/usr/bin/env python3
"""Tests for the Meta intake cron step (spec docs/specs/meta-intake.md, M1 and M3..M11).

Postgres tests run only with X_LAKE_TEST_DB_URL (throwaway database; they drop schema x_intel).
OCR and the gateway are fakes: nothing leaves the machine.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import curated as cur  # noqa: E402
from x_lakehouse import meta  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402
from x_lakehouse.lake import LocalStore  # noqa: E402

DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
ICT = meta.TZ
NOW = datetime(2026, 11, 12, 9, 0, tzinfo=ICT)
SECRET_TEXT = "ราคาทุเรียนหมอนทองหน้าสวนวันนี้ 120 บาท"


# ------------------------------------------------------------------------------------------ parsing

class UrlTests(unittest.TestCase):
    def test_m3_tracking_params_are_stripped_and_hosts_folded(self):
        n = meta.normalize_url
        self.assertEqual(n("https://m.facebook.com/groups/123/posts/456/?mibextid=abc&rdid=x&__cft__[0]=y#z"),
                         "https://facebook.com/groups/123/posts/456")
        self.assertEqual(n("https://web.facebook.com/permalink.php?story_fbid=9&id=7&fbclid=q"),
                         "https://facebook.com/permalink.php?id=7&story_fbid=9")
        self.assertEqual(n("https://www.instagram.com/p/AbC/?igsh=1&utm_source=ig"), "https://instagram.com/p/AbC")
        self.assertEqual(n("https://instagr.am/p/AbC/"), "https://instagram.com/p/AbC")
        self.assertEqual(n("https://fb.com/story.php?story_fbid=1&id=2"), "https://facebook.com/story.php?id=2&story_fbid=1")

    def test_identifying_params_are_sorted(self):
        self.assertEqual(meta.normalize_url("https://facebook.com/permalink.php?story_fbid=9&id=7"),
                         meta.normalize_url("https://facebook.com/permalink.php?id=7&story_fbid=9"))

    def test_m3_threads_net_and_threads_com_are_one_post(self):
        a = meta.normalize_url("https://www.threads.net/@someone/post/XyZ?xmt=AQ&igshid=1")
        b = meta.normalize_url("https://threads.com/@someone/post/XyZ/")
        self.assertEqual(a, "https://threads.com/@someone/post/XyZ")
        self.assertEqual(a, b)

    def test_l_php_redirects_are_unwrapped(self):
        u = "https://l.facebook.com/l.php?u=https%3A%2F%2Fwww.facebook.com%2Fgroups%2F1%2Fposts%2F2%2F%3Fmibextid%3Dq&h=AT0"
        self.assertEqual(meta.normalize_url(u), "https://facebook.com/groups/1/posts/2")

    def test_platform_of(self):
        self.assertEqual(meta.platform_of("https://facebook.com/x"), "facebook")
        self.assertEqual(meta.platform_of("https://fb.watch/abc"), "facebook")
        self.assertEqual(meta.platform_of("https://instagram.com/p/a"), "instagram")
        self.assertEqual(meta.platform_of("https://threads.com/@a/post/b"), "threads")
        self.assertEqual(meta.platform_of("https://example.com/a"), "unknown")
        self.assertEqual(meta.platform_of(None), "unknown")


class ParseTests(unittest.TestCase):
    def test_m1_group_tags_and_author_dropped_for_groups(self):
        p = meta.parse_message("ดูอันนี้ https://m.facebook.com/groups/9/posts/8/?mibextid=1\n#g ตลาดผลไม้ภาคตะวันออก\n"
                               "#author สมชาย ใจดี\n#note ราคาขึ้น #durian #price", [])
        self.assertEqual(p.surface, "group")
        self.assertEqual(p.platform, "facebook")
        self.assertEqual(p.group_label, "ตลาดผลไม้ภาคตะวันออก")
        self.assertIsNone(p.author_name)
        self.assertTrue(p.author_ignored)
        self.assertEqual(p.owner_note, "ราคาขึ้น")
        self.assertEqual(p.tags, ["durian", "price"])
        self.assertEqual(p.post_url, "https://facebook.com/groups/9/posts/8")
        self.assertEqual(p.text, "ดูอันนี้")

    def test_author_kept_on_pages(self):
        p = meta.parse_message("#page #author ร้านลุงดำ\nโปรวันนี้", ["https://facebook.com/lungdam/posts/1"])
        self.assertEqual((p.surface, p.author_name, p.author_ignored), ("page", "ร้านลุงดำ", False))
        self.assertEqual(p.text, "โปรวันนี้")

    def test_group_from_the_url_path_and_platform_tags(self):
        self.assertEqual(meta.parse_message("", ["https://facebook.com/groups/1/permalink/2/"]).surface, "group")
        p = meta.parse_message("#threads ข่าวนี้", [])
        self.assertEqual((p.platform, p.surface, p.post_url), ("threads", "unknown", None))
        self.assertEqual(meta.parse_message("#ig #feed", []).platform, "instagram")
        self.assertEqual(meta.parse_message("#ig #feed", []).surface, "feed")

    def test_text_is_capped(self):
        self.assertEqual(len(meta.parse_message("ก" * 9000, []).text), meta.MAX_TEXT)


# ------------------------------------------------------------------------------------------ fakes

class FakeOcr:
    def __init__(self, text: str = "ข้อความจากภาพ", fail: bool = False):
        self.text, self.fail, self.calls = text, fail, 0

    def __call__(self, images: list[bytes]) -> str:
        self.calls += 1
        if self.fail:
            raise meta.OcrError("tesseract exit 1")
        return self.text


class FakeGateway:
    def __init__(self, label="fact_report", route="alert_digest", status=200):
        self.label, self.route, self.status, self.bodies = label, route, status, []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.bodies.append(body)
        if self.status != 200:
            return httpx.Response(self.status, json={"code": "x"})
        posts = json.loads(body["messages"][1]["content"])
        items = [{"i": p["i"], "label": self.label, "route": self.route, "domain": "agri", "topic": "ราคาผลไม้"}
                 for p in posts]
        return httpx.Response(200, json={"content": json.dumps({"items": items})})

    def client(self):
        return httpx.Client(transport=httpx.MockTransport(self))


def landing(store: LocalStore, prefix: str, *, msg_id: int, text: str = "", urls=(), images=(), when=NOW,
            group: str | None = None, kind: str = "capture", target: str | None = None) -> str:
    cid = f"tg-abcd1234-g{group}" if group else f"tg-abcd1234-{msg_id}"
    day = when.astimezone(ICT).date().isoformat()
    image_keys = []
    for n, data in enumerate(images):
        k = meta.key(prefix, f"landing/meta_capture_media/date={day}/{cid}/{msg_id}-{n}.jpg")
        store.put_new(k, data)
        image_keys.append(k)
    rec = {"v": 1, "kind": kind, "capture_id": target if kind == "delete" else cid, "message_id": msg_id,
           "chat_hash": "abcd1234", "date": int(when.timestamp()), "media_group_id": group, "text": text,
           "urls": list(urls), "image_keys": image_keys}
    sub = "meta_capture_del" if kind == "delete" else "meta_capture"
    store.put_new(meta.key(prefix, f"landing/{sub}/date={day}/{rec['capture_id']}/{msg_id}.json"),
                  json.dumps(rec, ensure_ascii=False).encode())
    return rec["capture_id"]


def cfg(**extra) -> Config:
    return Config({"CLIENT_DENYLIST": "บริษัทลูกค้าลับ\nSecretClientCo", "AI_GATEWAY_TOKEN": "t", **extra})


# ------------------------------------------------------------------------------------------ store only

class LandingTests(unittest.TestCase):
    def test_landing_to_raw_is_idempotent_without_a_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = LocalStore(Path(tmp))
            landing(store, "", msg_id=1, text="a https://facebook.com/x/posts/1")
            first = meta.land(store, "", deleted=set(), curated=set())
            raw = [k for k in store.list("meta/raw/meta_capture")]
            self.assertEqual(len(raw), 1)
            self.assertIn("date=2026-11-12", raw[0])
            # A raw file alone is not done: until curated records it, the part comes back (raw not rewritten).
            again = meta.land(store, "", deleted=set(), curated=set())
            self.assertEqual([p["raw_key"] for p in again], raw)
            self.assertEqual(meta.land(store, "", deleted=set(), curated={first[0]["raw_key"]}), [])  # M6

    def test_batches_fit_the_gateway_budget(self):
        posts = [{"platform": "facebook", "surface": "group", "text": "ก" * 5000} for _ in range(30)]
        out = meta.batches(posts, ["agri", "energy"])
        self.assertEqual(sum(len(b) for b in out), 30)
        room = meta.MAX_TOKEN_BUDGET - meta.MAX_OUTPUT_TOKENS - 2048 - len(meta._system(["agri", "energy"]).encode())
        for b in out:
            size = len(json.dumps([meta._post_json(i, p) for i, p in enumerate(b)], ensure_ascii=False).encode())
            self.assertLessEqual(size, room)
            self.assertLessEqual(len(b), meta.BATCH)


# ------------------------------------------------------------------------------------------ Postgres

@unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class PgTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(Path(self.tmp.name))
        self.prefix = ""
        self.conn = cur.connect(DB)
        with self.conn.cursor() as c:
            c.execute("drop schema if exists x_intel cascade")
        self.conn.commit()
        cur.ensure_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def run_meta(self, *, now=NOW, ocr=None, gw=None, c=None, mode="write"):
        gw = gw or FakeGateway()
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rep = meta.run(self.conn, c or cfg(), mode=mode, store=self.store, prefix=self.prefix, now=now,
                           ocr=ocr or FakeOcr(), http=gw.client())
        return rep, gw, out.getvalue()

    def rows(self):
        with self.conn.cursor() as c:
            c.execute("select post_key, capture_ids, platform, surface, group_label, author_name, post_url, text, "
                      "text_source, tags, owner_note, content_label, route, withheld, image_keys "
                      "from x_intel.meta_posts order by first_captured_at")
            cols = [d.name for d in c.description]
            out = [dict(zip(cols, r)) for r in c.fetchall()]
        self.conn.commit()
        return out

    def test_m1_group_capture_with_screenshot(self):
        landing(self.store, "", msg_id=10, group="77", text="#g ตลาดผลไม้\n#author นาย ก",
                urls=["https://facebook.com/groups/5/posts/6/?mibextid=z"], images=[b"jpg1"])
        landing(self.store, "", msg_id=11, group="77", images=[b"jpg2"])
        rep, gw, _ = self.run_meta(ocr=FakeOcr(SECRET_TEXT))
        [r] = self.rows()
        self.assertEqual(r["capture_ids"], ["tg-abcd1234-g77"])
        self.assertEqual((r["surface"], r["group_label"], r["author_name"]), ("group", "ตลาดผลไม้", None))
        self.assertEqual(len(r["image_keys"]), 2)
        self.assertEqual((r["text"], r["text_source"]), (SECRET_TEXT, "ocr"))
        self.assertEqual((r["content_label"], r["route"]), ("fact_report", "alert_digest"))
        self.assertEqual(rep["landed"], 2)

    def test_m3_same_post_twice_merges(self):
        landing(self.store, "", msg_id=1, text="first #a", urls=["https://www.threads.net/@x/post/P?xmt=1"])
        landing(self.store, "", msg_id=2, text="second #b #note ดูต่อ", urls=["https://threads.com/@x/post/P/"],
                when=NOW + timedelta(minutes=5))
        self.run_meta()
        [r] = self.rows()
        self.assertEqual(r["capture_ids"], ["tg-abcd1234-1", "tg-abcd1234-2"])
        self.assertEqual(r["text"], "second")  # newest text wins
        self.assertEqual(r["tags"], ["a", "b"])
        self.assertEqual(r["owner_note"], "ดูต่อ")
        self.assertEqual(r["post_url"], "https://threads.com/@x/post/P")

    def test_m4_ocr_is_local_and_a_failure_retries(self):
        landing(self.store, "", msg_id=3, images=[b"img"])
        ocr = FakeOcr(fail=True)
        self.run_meta(ocr=ocr)
        [r] = self.rows()
        self.assertEqual((r["text_source"], r["content_label"]), ("none", None))
        ok = FakeOcr("ok text")
        _, gw, _ = self.run_meta(ocr=ok)
        [r] = self.rows()
        self.assertEqual((r["text"], r["text_source"]), ("ok text", "ocr"))
        self.assertEqual(ok.calls, 1)

    def test_m5_denylisted_capture_never_reaches_the_gateway_and_no_image_is_sent(self):
        landing(self.store, "", msg_id=4, images=[b"secret-image-bytes"])
        landing(self.store, "", msg_id=5, text="ข่าวทั่วไป", images=[b"another-image"])
        _, gw, _ = self.run_meta(ocr=FakeOcr("ใบเสนอราคา บริษัทลูกค้าลับ"))
        by = {r["capture_ids"][0]: r for r in self.rows()}
        self.assertTrue(by["tg-abcd1234-4"]["withheld"])
        self.assertEqual(by["tg-abcd1234-4"]["route"], "store")
        sent = json.dumps(gw.bodies, ensure_ascii=False)
        self.assertNotIn("บริษัทลูกค้าลับ", sent)
        self.assertIn("ข่าวทั่วไป", sent)
        for body in gw.bodies:  # text only: no image part, no base64, no key
            for m in body["messages"]:
                self.assertIsInstance(m["content"], str)
            self.assertNotIn("image", json.dumps(body))
            self.assertNotIn("meta_capture_media", json.dumps(body))

    def test_no_denylist_means_no_labelling_but_the_capture_is_kept(self):
        landing(self.store, "", msg_id=6, text="hello")
        _, gw, _ = self.run_meta(c=Config({"AI_GATEWAY_TOKEN": "t"}))
        [r] = self.rows()
        self.assertIsNone(r["content_label"])
        self.assertEqual(gw.bodies, [])

    def test_gateway_error_leaves_the_label_empty_for_the_next_run(self):
        landing(self.store, "", msg_id=7, text="hello")
        self.run_meta(gw=FakeGateway(status=502))
        self.assertIsNone(self.rows()[0]["content_label"])
        self.run_meta()
        self.assertEqual(self.rows()[0]["content_label"], "fact_report")

    def test_a_crash_after_the_raw_write_is_recovered(self):
        landing(self.store, "", msg_id=21, text="after crash")
        meta.ensure_tables(self.conn)
        meta.land(self.store, "", deleted=set(), curated=set())  # raw written, then the process died
        self.assertEqual(self.rows(), [])
        rep, _, _ = self.run_meta()
        self.assertEqual(rep["landed"], 1)
        self.assertEqual([r["text"] for r in self.rows()], ["after crash"])
        self.assertEqual(len(self.store.list("meta/raw")), 1)

    def test_a_new_author_on_a_labelled_post_is_screened_again(self):
        url = "https://facebook.com/somepage/posts/5"
        landing(self.store, "", msg_id=22, text="ข่าว #page", urls=[url])
        self.run_meta()
        self.assertEqual(self.rows()[0]["route"], "alert_digest")
        landing(self.store, "", msg_id=23, text="ข่าว #page #author SecretClientCo", urls=[url],
                when=NOW + timedelta(minutes=1))
        _, gw, _ = self.run_meta()
        [r] = self.rows()
        self.assertTrue(r["withheld"])
        self.assertEqual(gw.bodies, [])
        self.assertEqual(meta.digest_items(self.conn, NOW - timedelta(hours=1), NOW + timedelta(hours=1),
                                           ("secretclientco",)), [])

    def test_digest_screens_the_full_row(self):
        landing(self.store, "", msg_id=24, text="ข่าว #page #author SecretClientCo",
                urls=["https://facebook.com/p/posts/1"])
        self.run_meta(c=cfg(CLIENT_DENYLIST="อื่น"))  # labelled under an older list
        self.assertEqual(self.rows()[0]["route"], "alert_digest")
        self.assertEqual(meta.digest_items(self.conn, NOW - timedelta(hours=1), NOW + timedelta(hours=1),
                                           ("secretclientco",)), [])

    def test_an_album_photo_arriving_later_is_read_again(self):
        landing(self.store, "", msg_id=25, group="88", images=[b"one"])
        self.run_meta(ocr=FakeOcr("ภาพแรก"))
        self.assertEqual(self.rows()[0]["text"], "ภาพแรก")
        landing(self.store, "", msg_id=26, group="88", images=[b"two"], when=NOW + timedelta(minutes=1))
        seen = []

        def ocr(images):
            seen.append(len(images))
            return "ภาพแรก ภาพสอง"

        _, gw, _ = self.run_meta(ocr=ocr)
        [r] = self.rows()
        self.assertEqual(seen, [2])
        self.assertEqual((r["text"], r["text_source"], r["content_label"]), ("ภาพแรก ภาพสอง", "ocr", "fact_report"))

    def test_pasted_text_survives_a_later_album_photo(self):
        landing(self.store, "", msg_id=27, group="89", text="คำบรรยาย", images=[b"one"])
        self.run_meta()
        landing(self.store, "", msg_id=28, group="89", images=[b"two"], when=NOW + timedelta(minutes=1))
        ocr = FakeOcr("x")
        self.run_meta(ocr=ocr)
        self.assertEqual((self.rows()[0]["text"], ocr.calls), ("คำบรรยาย", 0))

    def test_retention_runs_when_labelling_fails(self):
        landing(self.store, "", msg_id=29, text="old", when=NOW - timedelta(days=31))
        landing(self.store, "", msg_id=30, text="needs a label")
        rep, _, _ = self.run_meta(c=Config({"CLIENT_DENYLIST": "x"}))  # no AI_GATEWAY_TOKEN
        self.assertEqual(rep["labelling"], "error ConfigError")
        self.assertFalse([k for k in self.store.list("meta/landing") if "tg-abcd1234-29" in k])

    def test_m6_second_run_writes_nothing(self):
        landing(self.store, "", msg_id=8, text="x")
        self.run_meta()
        rep, gw, _ = self.run_meta()
        self.assertEqual((rep["landed"], rep["labelled"]), (0, 0))
        self.assertEqual(gw.bodies, [])

    def test_dry_writes_nothing(self):
        landing(self.store, "", msg_id=9, text="x")
        rep, gw, _ = self.run_meta(mode="dry")
        self.assertEqual(rep["pending"], 1)
        self.assertEqual(self.store.list("meta/raw"), [])
        self.assertEqual(gw.bodies, [])

    def test_m8_logs_and_report_hold_no_text_or_url(self):
        landing(self.store, "", msg_id=12, text=SECRET_TEXT, urls=["https://facebook.com/groups/1/posts/2"],
                images=[b"x"])
        rep, _, out = self.run_meta(gw=FakeGateway(status=502))
        blob = out + json.dumps(rep, ensure_ascii=False, default=str)
        for needle in (SECRET_TEXT, "facebook.com", "groups/1"):
            self.assertNotIn(needle, blob)

    def test_m9_landing_older_than_30_days_is_deleted(self):
        landing(self.store, "", msg_id=13, text="old", images=[b"x"], when=NOW - timedelta(days=31))
        landing(self.store, "", msg_id=14, text="new", images=[b"y"], when=NOW - timedelta(days=29))
        rep, _, _ = self.run_meta()
        left = self.store.list("meta/landing")
        self.assertFalse([k for k in left if "tg-abcd1234-13" in k])
        self.assertTrue([k for k in left if "tg-abcd1234-14" in k])
        self.assertEqual(rep["retention"]["stale_landing"], 0)
        self.assertGreaterEqual(rep["retention"]["landing_deleted"], 2)

    def test_m10_curated_and_raw_go_after_365_days(self):
        landing(self.store, "", msg_id=15, text="old", when=NOW - timedelta(days=366))
        landing(self.store, "", msg_id=16, text="keep", when=NOW - timedelta(days=364))
        self.run_meta(now=NOW - timedelta(days=360))  # both land while young enough
        self.assertEqual(len(self.rows()), 2)
        self.run_meta(now=NOW)
        self.assertEqual([r["text"] for r in self.rows()], ["keep"])
        raw = self.store.list("meta/raw")
        self.assertFalse([k for k in raw if "tg-abcd1234-15" in k])
        self.assertTrue([k for k in raw if "tg-abcd1234-16" in k])

    def test_m11_del_removes_everything_and_a_replay_is_refused(self):
        cid = landing(self.store, "", msg_id=17, text="ลบฉัน", images=[b"x"],
                      urls=["https://facebook.com/a/posts/1"])
        self.run_meta()
        saved = {k: self.store.read(k) for k in self.store.list("meta/landing/meta_capture")}
        landing(self.store, "", msg_id=18, kind="delete", target=cid)
        rep, _, _ = self.run_meta()
        self.assertEqual(rep["deleted"], 1)
        self.assertEqual(self.rows(), [])
        self.assertFalse([k for k in self.store.list("meta") if cid in k and "meta_capture_del" not in k])
        for k, v in saved.items():  # replay the same landing files
            self.store.put_new(k, v)
        self.run_meta()
        self.assertEqual(self.rows(), [])

    def test_m7_mart_lists_digest_items_only(self):
        landing(self.store, "", msg_id=19, text="ข่าวด่วน #g กลุ่ม A", urls=["https://facebook.com/groups/1/posts/9"])
        landing(self.store, "", msg_id=20, text="ทั่วไป")
        gw = FakeGateway()

        class Mixed(FakeGateway):
            def __call__(self, request):
                body = json.loads(request.content)
                posts = json.loads(body["messages"][1]["content"])
                items = [{"i": p["i"], "label": "fact_report", "route": "alert_digest" if "ด่วน" in p["text"] else "store",
                          "domain": "agri", "topic": "t"} for p in posts]
                return httpx.Response(200, json={"content": json.dumps({"items": items})})

        self.run_meta(gw=Mixed())
        items = meta.digest_items(self.conn, NOW - timedelta(hours=1), NOW + timedelta(hours=1), cfg_denylist())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["group_label"], "กลุ่ม A")
        self.assertNotIn("author_name", items[0])
        from x_lakehouse import digest
        from x_lakehouse.marts import ensure_marts

        ensure_marts(self.conn)
        facts = digest.build_facts(self.conn, digest.Window("midday", NOW - timedelta(hours=1), NOW + timedelta(hours=1)),
                                   cfg_denylist())
        self.assertEqual([m["group_label"] for m in facts.meta], ["กลุ่ม A"])
        self.assertIn("จาก Meta", digest.render(facts, None))
        self.assertEqual(gw.bodies, [])


class DigestRenderTests(unittest.TestCase):
    def facts(self, signals=0, meta_items=()):
        from x_lakehouse import digest

        w = digest.windows(NOW, "midday")[0]
        f = digest.Facts(w, totals={"signals": signals, "alerts": 0, "urgent": 0, "opportunities": 0,
                                    "strong_sources": 0})
        f.meta = list(meta_items)
        return digest, f

    def test_m7_section_shows_group_not_author_and_only_when_there_are_items(self):
        digest, f = self.facts(meta_items=[
            {"platform": "facebook", "surface": "group", "group_label": "ตลาดผลไม้", "text": "ราคาขึ้น",
             "link": "https://facebook.com/groups/1/posts/2", "route": "alert_digest"}] * 7)
        text = digest.render(f, None)
        self.assertIn("จาก Meta", text)
        self.assertIn("กลุ่ม ตลาดผลไม้", text)
        self.assertEqual(text.count("ราคาขึ้น"), 5)  # max 5
        _, empty = self.facts()
        self.assertNotIn("จาก Meta", digest.render(empty, None))


def cfg_denylist():
    return ("บริษัทลูกค้าลับ",)


if __name__ == "__main__":
    unittest.main()
