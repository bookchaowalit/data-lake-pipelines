#!/usr/bin/env python3
"""Acceptance tests A1-A12 for the daily sheet summaries (docs/specs/sheet-summaries.md).

The sheet is the in-memory ``MemorySheets``; the gateway is a stateful fake served through
httpx.MockTransport behind the real ``GatewayClient``. Rows, handles and links are fake test data.
"""

from __future__ import annotations

import contextlib
import io
import json
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from sync.sheet_summaries import (  # noqa: E402
    LATEST_TAB,
    MAX_BATCHES,
    MERGE_PROMPT_VERSION,
    SHORT_NOTE,
    OUTPUT_HEADER,
    OUTPUT_TAB,
    DenylistError,
    GatewayClient,
    MemorySheets,
    _service_account,
    google_access_token,
    link_source,
    load_denylist,
    run,
    select_tabs,
)

NOW = datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc)
TOKEN = "gw-test-token-0123456789"
SECRET_TEXT = "ข้อความลับในแถวทดสอบ"
X_HEADER = ["collected_at", "requesting_chief", "watch_type", "watch_value", "post_id", "post_url",
            "author_handle", "posted_at", "text", "likes", "reposts", "replies", "views", "tag",
            "query_id", "score", "top"]


def x_row(i: int, *, score: int = 1, top: str = "", text: str = "โพสต์ทดสอบ") -> list[str]:
    return ["2026-10-26T10:30:00Z", "Chief Property", "keyword", "คอนโด", f"9{i:04d}",
            f"https://x.com/fake_user/status/9{i:04d}", "@fake_user", "2026-10-26T09:00:00Z",
            f"{text} {i}", "1", "0", "0", "10", "property", "q1", str(score), top]


def finding_rows(n: int, *, link_prefix: str = "https://example.test/item") -> list[list[str]]:
    header = ["found_at", "title", "summary", "source_url", "deadline"]
    return [header] + [
        ["2026-10-25", f"หัวข้อ {i}", f"รายละเอียด {i}", f"{link_prefix}/{i}", ""] for i in range(n)
    ]


def sheet(**extra: list[list[str]]) -> MemorySheets:
    tabs: dict[str, list[list[str]]] = {
        "README": [["about"], ["ไม่ต้องสรุป"]],
        "X Watchlist": [["chief", "keyword"], ["Chief Property", "คอนโด"]],
        "X Query Plan": [["q"], ["x"]],
        "Retired Bots Archive": [["bot"], ["old"]],
        "Chief Property": finding_rows(3),
        "X Feed": [X_HEADER] + [x_row(i) for i in range(4)],
    }
    tabs.update(extra)
    return MemorySheets(tabs)


def model_answer(links: list[str], *, summary: str = "สรุปภาษาไทยสั้น ๆ สำหรับแท็บนี้ มีรายการใหม่ที่ควรดู") -> str:
    return json.dumps({
        "summary_th": summary,
        "top_items": [{"title": "รายการเด่น", "why": "สำคัญ", "link": link, "link_source": "website"} for link in links[:2]],
        "candidate_tasks": [{"title": "ตามต่อ", "value": "สูง", "deadline": None, "link": links[0] if links else None,
                             "suggested_owner_role": "Chief Property"}],
        "deadlines": [],
    }, ensure_ascii=False)


class FakeGateway:
    """Answers with links taken from the request (or ``invent`` links); records every call."""

    def __init__(self, *, decision: str = "allow", invent: bool = False, answer_text: str | None = None,
                 failures: list[httpx.Response] | None = None, generate_status: str = "success",
                 finish_reason: str = "stop", merge_answer: str | None = None,
                 answers_by_call: dict[int, str] | None = None, summary: str | None = None) -> None:
        self.decision = decision
        self.invent = invent
        self.answer_text = answer_text
        self.failures = list(failures or [])
        self.generate_status = generate_status
        self.finish_reason = finish_reason
        self.merge_answer = merge_answer
        self.summary = summary
        self.answers_by_call = dict(answers_by_call or {})
        self.generate_bodies: list[dict[str, Any]] = []
        self.status_calls = 0

    def handle(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        if request.method == "GET" and request.url.path == "/api/ai/status":
            self.status_calls += 1
            return httpx.Response(200, json={
                "budget": {"decision": self.decision},
                "limits": {"max_context_chars": 48000, "max_output_tokens": 4096,
                           "default_token_budget": 4096, "max_token_budget": 32768},
            })
        assert request.method == "POST" and request.url.path == "/api/ai/generate"
        body = json.loads(request.content)
        self.generate_bodies.append(body)
        if self.failures:
            return self.failures.pop(0)
        prompt = "\n".join(m["content"] for m in body["messages"])
        links = ["https://invented.example/nowhere"] if self.invent else sorted(
            set(re.findall(r'https://[^\s"]+', prompt)))
        content = self.answer_text if self.answer_text is not None else (
            model_answer(links, summary=self.summary) if self.summary else model_answer(links))
        if body["prompt_version"] == MERGE_PROMPT_VERSION and self.merge_answer is not None:
            content = self.merge_answer
        content = self.answers_by_call.get(len(self.generate_bodies), content)
        return httpx.Response(200, json={
            "contract": "solo-empire.ai-gateway.v1", "run_id": f"run-{len(self.generate_bodies)}",
            "status": self.generate_status, "model": "gpt-6-luna", "content": content,
            "finish_reason": self.finish_reason,
            "usage": {"input_tokens": 1200, "output_tokens": 300, "total_tokens": 1500, "source": "provider"},
            "estimated_cost_usd": 0.00027,
        })


def client(fake: FakeGateway) -> GatewayClient:
    http = httpx.Client(transport=httpx.MockTransport(fake.handle), base_url="https://gateway.test")
    return GatewayClient(http, TOKEN, sleep=lambda _seconds: None)


def output_rows(sheets: MemorySheets) -> list[dict[str, str]]:
    values = sheets.tabs[OUTPUT_TAB]
    return [dict(zip(values[0], row)) for row in values[1:]]


DENYLIST = ("OakClientCo", "freelance-client-x")


def quiet_run(*args: Any, **kwargs: Any) -> Any:
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return run(*args, **kwargs)


class TabSelectionTests(unittest.TestCase):
    def test_skip_list_and_new_chief_tab(self) -> None:  # A2
        titles = ["README", "X Watchlist", "X Query Plan", "Retired Bots Archive", OUTPUT_TAB, LATEST_TAB,
                  "All Findings", "Chief Property", "Chief Legal & Trust", "Datasets", "X Feed", "Scratch"]
        self.assertEqual(
            select_tabs(titles),
            ["All Findings", "Chief Property", "Chief Legal & Trust", "Datasets", "X Feed"],
        )

    def test_headers_are_read_by_name(self) -> None:  # A1
        odd = [["notes", "url", "when"], ["ข้อสังเกต", "https://example.test/odd/1", "2026-10-25"]]
        sheets = sheet(**{"Chief Legal & Trust": odd})
        fake = FakeGateway()
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        legal = [b for b in fake.generate_bodies if "Chief Legal & Trust" in b["messages"][1]["content"]]
        self.assertEqual(len(legal), 1)
        prompt = legal[0]["messages"][1]["content"]
        self.assertIn('"url": "https://example.test/odd/1"', prompt)
        self.assertIn('"notes": "ข้อสังเกต"', prompt)
        tabs = {row["tab"] for row in output_rows(sheets)}
        self.assertEqual(tabs, {"Chief Property", "X Feed", "Chief Legal & Trust"})
        self.assertNotIn("README", "".join(b["messages"][1]["content"] for b in fake.generate_bodies))


class WatermarkTests(unittest.TestCase):
    def test_second_run_without_new_rows_makes_no_calls(self) -> None:  # A3
        sheets = sheet()
        fake = FakeGateway()
        quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        first_calls = len(fake.generate_bodies)
        self.assertEqual(first_calls, 2)
        self.assertEqual(sheets.tabs[OUTPUT_TAB][0], OUTPUT_HEADER)

        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(len(fake.generate_bodies), first_calls)
        latest = output_rows(sheets)[-2:]
        self.assertEqual({row["status"] for row in latest}, {"no_change"})
        self.assertEqual({row["window"] for row in latest if row["tab"] == "X Feed"}, {"after_row=5;to_row=5"})

    def test_only_new_rows_are_sent(self) -> None:
        sheets = sheet()
        fake = FakeGateway()
        quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        sheets.tabs["X Feed"].append(x_row(77))
        quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(len(fake.generate_bodies), 3)
        prompt = fake.generate_bodies[-1]["messages"][1]["content"]
        self.assertIn("status/90077", prompt)
        self.assertNotIn("status/90001", prompt)
        x_rows = [row for row in output_rows(sheets) if row["tab"] == "X Feed"]
        self.assertEqual(x_rows[-1]["window"], "after_row=5;to_row=6")
        self.assertEqual(x_rows[-1]["rows_considered"], "1")

    def test_source_tabs_are_never_written(self) -> None:
        sheets = sheet()
        before = json.dumps({k: v for k, v in sheets.tabs.items()}, ensure_ascii=False)
        quiet_run(sheets, client(FakeGateway()), DENYLIST, now=NOW)
        written = {title for title, _ in sheets.writes}
        self.assertEqual(written, {OUTPUT_TAB, LATEST_TAB})
        after = {k: v for k, v in sheets.tabs.items() if k not in (OUTPUT_TAB, LATEST_TAB)}
        self.assertEqual(json.dumps(after, ensure_ascii=False), before)

    def test_latest_tab_holds_one_row_per_tab(self) -> None:
        sheets = sheet()
        quiet_run(sheets, client(FakeGateway()), DENYLIST, now=NOW)
        quiet_run(sheets, client(FakeGateway()), DENYLIST, now=NOW)
        latest = sheets.tabs[LATEST_TAB]
        self.assertEqual(latest[0], OUTPUT_HEADER)
        self.assertEqual(sorted(row[0] for row in latest[1:]), ["Chief Property", "X Feed"])

    def test_dry_run_writes_nothing(self) -> None:  # spec 12
        sheets = sheet()
        result = quiet_run(sheets, client(FakeGateway()), DENYLIST, now=NOW, dry_run=True)
        self.assertEqual(sheets.writes, [])
        self.assertNotIn(OUTPUT_TAB, sheets.tabs)
        self.assertEqual(len(result.rows), 2)


class LinkValidationTests(unittest.TestCase):
    def test_invented_link_is_rejected(self) -> None:  # A4
        sheets = sheet()
        result = quiet_run(sheets, client(FakeGateway(invent=True)), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 1)
        rows = output_rows(sheets)
        self.assertEqual({row["status"] for row in rows}, {"error"})
        self.assertTrue(all(row["error"] == "invented_link" for row in rows))
        self.assertTrue(all(row["summary_th"] == "" for row in rows))

    def test_link_source_is_recomputed(self) -> None:
        sheets = sheet()
        quiet_run(sheets, client(FakeGateway()), DENYLIST, now=NOW)
        x_items = json.loads([r for r in output_rows(sheets) if r["tab"] == "X Feed"][0]["top_items"])
        self.assertTrue(x_items)
        self.assertEqual({item["link_source"] for item in x_items}, {"x_post"})
        web_items = json.loads([r for r in output_rows(sheets) if r["tab"] == "Chief Property"][0]["top_items"])
        self.assertEqual({item["link_source"] for item in web_items}, {"website"})

    def test_schema_violation_is_rejected(self) -> None:
        sheets = sheet()
        bad = json.dumps({"summary_th": "สรุป", "top_items": [{"title": "x"}] * 6, "candidate_tasks": [], "deadlines": []})
        result = quiet_run(sheets, client(FakeGateway(answer_text=bad)), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 1)
        self.assertEqual({row["error"] for row in output_rows(sheets)}, {"invalid_output"})

    def test_truncated_answer_is_reported(self) -> None:
        sheets = sheet()
        result = quiet_run(sheets, client(FakeGateway(answer_text="", finish_reason="length")), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 1)
        self.assertEqual({row["error"] for row in output_rows(sheets)}, {"output_truncated"})

    def test_non_json_is_rejected(self) -> None:
        sheets = sheet()
        quiet_run(sheets, client(FakeGateway(answer_text="ไม่ใช่ JSON")), DENYLIST, now=NOW)
        self.assertEqual({row["error"] for row in output_rows(sheets)}, {"invalid_output"})


class DenylistTests(unittest.TestCase):
    def test_denylisted_rows_never_reach_the_gateway(self) -> None:  # A5
        rows = finding_rows(2)
        rows.append(["2026-10-25", "งานของ oakclientco", "ลูกค้า", "https://example.test/client", ""])
        sheets = sheet(**{"Chief Property": rows})
        fake = FakeGateway()
        quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        sent = "".join(b["messages"][1]["content"] for b in fake.generate_bodies).lower()
        self.assertNotIn("oakclientco", sent)
        self.assertNotIn("https://example.test/client", sent)
        prop = [r for r in output_rows(sheets) if r["tab"] == "Chief Property"][0]
        self.assertEqual(prop["rows_considered"], "2")

    def test_answer_mentioning_a_client_is_not_written(self) -> None:
        sheets = sheet()
        leaky = model_answer([], summary="สรุปที่พูดถึง OakClientCo โดยไม่ตั้งใจ")
        quiet_run(sheets, client(FakeGateway(answer_text=leaky)), DENYLIST, now=NOW)
        self.assertNotIn("OakClientCo", json.dumps(sheets.tabs[OUTPUT_TAB], ensure_ascii=False))
        self.assertEqual({row["error"] for row in output_rows(sheets)}, {"client_term_in_output"})

    def test_lookalike_and_header_matches_are_filtered(self) -> None:
        rows = finding_rows(1)
        rows.append(["2026-10-25", "งานของ Oak\u200bClient\uff23o", "x", "https://example.test/zw", ""])
        rows.append(["2026-10-25", "งาน Oak\u034fClient\ufe0fCo\U000e0041", "x", "https://example.test/ignorable", ""])
        rows.append(["2026-10-25", "งาน OakClientCo\u0301 ต่อ", "x", "https://example.test/combining", ""])
        header_tab = [["found_at", "OakClientCo notes"], ["2026-10-25", "ข้อมูล"]]
        sheets = sheet(**{"Chief Property": rows, "Chief Career": header_tab, "Chief OakClientCo": finding_rows(1)})
        fake = FakeGateway()
        quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        sent = "".join(b["messages"][1]["content"] for b in fake.generate_bodies)
        self.assertNotIn("https://example.test/zw", sent)
        self.assertNotIn("https://example.test/ignorable", sent)
        self.assertNotIn("https://example.test/combining", sent)
        self.assertNotIn("OakClientCo", sent)
        self.assertNotIn("Chief OakClientCo", {row["tab"] for row in output_rows(sheets)})
        career = [r for r in output_rows(sheets) if r["tab"] == "Chief Career"][0]
        self.assertEqual(career["status"], "no_change")

    def test_missing_or_empty_denylist_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            empty = Path(tmp) / "denylist.txt"
            empty.write_text("# no terms yet\n\n", encoding="utf-8")
            good = Path(tmp) / "good.txt"
            good.write_text("# clients\nOakClientCo\n  freelance-client-x  \n", encoding="utf-8")
            with self.assertRaises(DenylistError):
                load_denylist(None)
            with self.assertRaises(DenylistError):
                load_denylist(str(Path(tmp) / "missing.txt"))
            with self.assertRaises(DenylistError):
                load_denylist(str(empty))
            self.assertEqual(load_denylist(str(good)), ("oakclientco", "freelance-client-x"))

    def test_run_refuses_an_empty_denylist(self) -> None:
        fake = FakeGateway()
        with self.assertRaises(DenylistError):
            quiet_run(sheet(), client(fake), (), now=NOW)
        self.assertEqual(fake.status_calls, 0)


class GatewayErrorTests(unittest.TestCase):
    def test_budget_exceeded_is_a_result(self) -> None:  # A6
        sheets = sheet()
        result = quiet_run(sheets, client(FakeGateway(generate_status="budget_exceeded")), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual({row["status"] for row in output_rows(sheets)}, {"budget_exceeded"})
        self.assertTrue(all(row["summary_th"] for row in output_rows(sheets)))

    def _stopped(self, failures: list[httpx.Response]) -> tuple[FakeGateway, MemorySheets, Any]:
        sheets = sheet()
        fake = FakeGateway(failures=failures)
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        return fake, sheets, result

    def test_budget_deny_stops_without_retry(self) -> None:
        deny = httpx.Response(429, json={"error": "AI monthly budget guard denied the request", "code": "monthly_budget_exceeded"})
        fake, sheets, result = self._stopped([deny])
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(len(fake.generate_bodies), 1)
        rows = output_rows(sheets)
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["status"] for row in rows}, {"error"})
        self.assertEqual({row["error"] for row in rows}, {"gateway_stop:monthly_budget_exceeded"})

    def test_5xx_retries_once_then_stops(self) -> None:
        down = [httpx.Response(502, json={"error": "x", "code": "provider_error"}) for _ in range(5)]
        fake, sheets, result = self._stopped(down)
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(len(fake.generate_bodies), 2)
        self.assertEqual({row["error"] for row in output_rows(sheets)}, {"gateway_stop:http_502"})

    def test_5xx_then_success_continues(self) -> None:
        fake, sheets, result = self._stopped([httpx.Response(503, json={"error": "x", "code": "upstream"})])
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(len(fake.generate_bodies), 3)
        self.assertEqual({row["status"] for row in output_rows(sheets)}, {"ok"})

    def test_rate_limited_retries_once(self) -> None:
        limited = [httpx.Response(429, headers={"retry-after": "1"}, json={"error": "x", "code": "rate_limited"})] * 2
        fake, sheets, result = self._stopped(limited)
        self.assertEqual(len(fake.generate_bodies), 2)
        self.assertEqual({row["error"] for row in output_rows(sheets)}, {"gateway_stop:rate_limited"})

    def test_400_marks_only_that_tab(self) -> None:
        fake, sheets, result = self._stopped([httpx.Response(400, json={"error": "bad", "code": "bad_request"})])
        self.assertEqual(result.exit_code, 1)
        statuses = sorted(row["status"] for row in output_rows(sheets))
        self.assertEqual(statuses, ["error", "ok"])

    def test_preflight_aborts_unless_allow(self) -> None:  # A7
        for decision in ("deny", "unavailable", "unconfigured"):
            sheets = sheet()
            fake = FakeGateway(decision=decision)
            result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
            self.assertEqual(result.exit_code, 1)
            self.assertEqual(fake.generate_bodies, [])
            self.assertEqual({row["error"] for row in output_rows(sheets)}, {f"budget_{decision}"})

    def test_request_follows_the_contract(self) -> None:
        fake = FakeGateway()
        quiet_run(sheet(), client(fake), DENYLIST, now=NOW)
        body = fake.generate_bodies[0]
        self.assertEqual(body["route"], "sheet-summaries")
        self.assertEqual(body["prompt_version"], "sheet-summaries-v1")
        self.assertEqual(body["profile"], "fast")
        self.assertEqual(body["privacy_class"], "internal")
        self.assertEqual(body["response_format"], {"type": "json_object"})
        self.assertEqual(body["reasoning_effort"], "low")
        estimated = sum(len(m["content"].encode("utf-8")) + 32 for m in body["messages"])
        self.assertLessEqual(estimated + body["max_output_tokens"], body["token_budget"])
        self.assertLessEqual(body["token_budget"], 32768)


class CapTests(unittest.TestCase):
    def test_x_feed_over_cap_keeps_top_and_score(self) -> None:  # A9
        rows = [x_row(i, score=i % 7) for i in range(260)]
        rows[5] = x_row(5, score=0, top="TRUE")
        sheets = sheet(**{"X Feed": [X_HEADER] + rows, "Chief Property": finding_rows(1)})
        fake = FakeGateway()
        quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        prompt = [b for b in fake.generate_bodies if "X Feed" in b["messages"][1]["content"]][0]["messages"][1]["content"]
        self.assertIn("status/90005", prompt)
        self.assertNotIn('"score": "0"', prompt.replace('"score": "0", "top": "TRUE"', ""))

    def test_small_gateway_budget_shrinks_the_input(self) -> None:
        fake = FakeGateway()
        original = fake.handle

        def small_limits(request: httpx.Request) -> httpx.Response:
            response = original(request)
            if request.url.path == "/api/ai/status":
                data = response.json()
                data["limits"]["max_token_budget"] = 4096
                return httpx.Response(200, json=data)
            return response

        sheets = sheet(**{"X Feed": [X_HEADER] + [x_row(i, text="ยาว" * 20) for i in range(80)]})
        http = httpx.Client(transport=httpx.MockTransport(small_limits), base_url="https://gateway.test")
        result = quiet_run(sheets, GatewayClient(http, TOKEN, sleep=lambda _s: None), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        for body in fake.generate_bodies:
            estimated = sum(len(m["content"].encode("utf-8")) + 32 for m in body["messages"])
            self.assertLessEqual(estimated + body["max_output_tokens"], 4096)
        x_row_out = [r for r in output_rows(sheets) if r["tab"] == "X Feed"][0]
        self.assertLess(int(x_row_out["rows_considered"]), 80)
        self.assertEqual(x_row_out["window"], "after_row=1;to_row=81")

    def test_first_run_uses_last_seven_days(self) -> None:
        old = ["2026-10-01", "เก่า", "เก่ามาก", "https://example.test/old", ""]
        rows = finding_rows(2)
        rows.insert(1, old)
        sheets = sheet(**{"Chief Property": rows})
        fake = FakeGateway()
        quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        sent = "".join(b["messages"][1]["content"] for b in fake.generate_bodies)
        self.assertNotIn("https://example.test/old", sent)


def long_x_feed(n: int) -> MemorySheets:
    """An X Feed far larger than one request; Chief Property has no rows, so every call is X Feed."""

    return sheet(**{"X Feed": [X_HEADER] + [x_row(i, score=i % 9, text="ยาว" * 40) for i in range(n)],
                    "Chief Property": finding_rows(0)})


def x_calls(fake: FakeGateway) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    bodies = [b for b in fake.generate_bodies if "Tab: X Feed" in b["messages"][1]["content"]]
    batches = [b for b in bodies if b["prompt_version"] != MERGE_PROMPT_VERSION]
    merges = [b for b in bodies if b["prompt_version"] == MERGE_PROMPT_VERSION]
    return batches, merges


class BatchTests(unittest.TestCase):
    def test_large_tab_is_split_into_batches_and_merged(self) -> None:  # A10
        sheets = long_x_feed(200)
        fake = FakeGateway()
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        batches, merges = x_calls(fake)
        self.assertGreater(len(batches), 1)
        self.assertLessEqual(len(batches), MAX_BATCHES)
        self.assertEqual(len(merges), 1)
        self.assertIs(fake.generate_bodies[-1], merges[0])
        # The merge sees only the validated batch answers, never the raw rows.
        self.assertNotIn("ยาวยาว", merges[0]["messages"][1]["content"])
        for body in fake.generate_bodies:
            estimated = sum(len(m["content"].encode("utf-8")) + 32 for m in body["messages"])
            self.assertLessEqual(estimated + body["max_output_tokens"], body["token_budget"])
        rows = [r for r in output_rows(sheets) if r["tab"] == "X Feed"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "ok")
        self.assertEqual(rows[0]["rows_considered"], "200")
        self.assertEqual(rows[0]["window"], "after_row=1;to_row=201")
        calls = len(batches) + 1
        self.assertEqual(rows[0]["tokens_in"], str(1200 * calls))
        self.assertEqual(rows[0]["cost_usd"], f"{0.00027 * calls:.6f}")
        self.assertEqual(rows[0]["run_id"], f"run-{calls}")
        # Batches run in priority order: the first holds the top-scored rows.
        first = batches[0]["messages"][1]["content"]
        self.assertIn('"score": "8"', first)
        self.assertNotIn('"score": "0"', first)

    def test_batches_are_capped(self) -> None:  # A10
        fake = FakeGateway()
        original = fake.handle

        def small_limits(request: httpx.Request) -> httpx.Response:
            response = original(request)
            if request.url.path == "/api/ai/status":
                data = response.json()
                data["limits"]["max_token_budget"] = 8192
                return httpx.Response(200, json=data)
            return response

        sheets = long_x_feed(200)
        http = httpx.Client(transport=httpx.MockTransport(small_limits), base_url="https://gateway.test")
        result = quiet_run(sheets, GatewayClient(http, TOKEN, sleep=lambda _s: None), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        batches, merges = x_calls(fake)
        self.assertEqual(len(batches), MAX_BATCHES)
        self.assertGreaterEqual(len(merges), 1)
        row = [r for r in output_rows(sheets) if r["tab"] == "X Feed"][0]
        self.assertLess(int(row["rows_considered"]), 200)
        self.assertEqual(row["window"], "after_row=1;to_row=201")

    def test_answers_too_large_for_one_merge_are_merged_in_rounds(self) -> None:  # A10
        sheets = long_x_feed(200)
        fake = FakeGateway(summary="สรุปยาว" * 280)  # ~6 KB per answer: one merge holds about four
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        batches, merges = x_calls(fake)
        self.assertGreater(len(merges), 1)
        self.assertIs(fake.generate_bodies[-1], merges[-1])
        for body in merges:
            estimated = sum(len(m["content"].encode("utf-8")) + 32 for m in body["messages"])
            self.assertLessEqual(estimated + body["max_output_tokens"], body["token_budget"])
        row = [r for r in output_rows(sheets) if r["tab"] == "X Feed"][0]
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["rows_considered"], "200")
        self.assertEqual(row["tokens_in"], str(1200 * (len(batches) + len(merges))))

    def test_a_malformed_merge_answer_is_retried_once(self) -> None:  # A11
        sheets = long_x_feed(200)
        probe = FakeGateway()
        quiet_run(long_x_feed(200), client(probe), DENYLIST, now=NOW)
        merge_call = len(probe.generate_bodies)  # the single merge comes last
        fake = FakeGateway(answers_by_call={merge_call: "not json"})
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        _batches, merges = x_calls(fake)
        self.assertEqual(len(merges), 2)
        row = [r for r in output_rows(sheets) if r["tab"] == "X Feed"][0]
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["tokens_in"], str(1200 * len(fake.generate_bodies)))  # the failed try counts

    def test_merge_with_an_invented_link_is_rejected(self) -> None:  # A11
        sheets = long_x_feed(200)
        fake = FakeGateway(merge_answer=model_answer(["https://invented.example/nowhere"]))
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 1)
        self.assertEqual(len(x_calls(fake)[1]), 2)  # tried twice, then the tab fails
        row = [r for r in output_rows(sheets) if r["tab"] == "X Feed"][0]
        self.assertEqual((row["status"], row["error"]), ("error", "invented_link"))
        self.assertEqual(row["summary_th"], "")

    def test_a_bad_batch_is_dropped_and_the_rest_merged(self) -> None:  # A12
        sheets = long_x_feed(200)
        fake = FakeGateway(answers_by_call={2: "not json"})
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        batches, merges = x_calls(fake)
        self.assertEqual(len(merges), 1)
        dropped = batches[1]["messages"][1]["content"]
        merged = merges[0]["messages"][1]["content"]
        dropped_links = set(re.findall(r'https://[^\s"]+', dropped))
        self.assertTrue(dropped_links)
        self.assertFalse(dropped_links & set(re.findall(r'https://[^\s"]+', merged)))
        row = [r for r in output_rows(sheets) if r["tab"] == "X Feed"][0]
        self.assertEqual(row["status"], "ok")
        self.assertLess(int(row["rows_considered"]), 200)

    def test_a_client_term_in_a_batch_answer_is_never_written(self) -> None:  # A12
        sheets = long_x_feed(200)
        leak = model_answer([], summary="สรุปที่มีชื่อ OakClientCo หลุดมา")
        fake = FakeGateway(answers_by_call={2: leak})
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 0)
        _batches, merges = x_calls(fake)
        self.assertTrue(merges)
        self.assertNotIn("OakClientCo", "".join(m["messages"][1]["content"] for m in merges))
        self.assertNotIn("OakClientCo", json.dumps(sheets.tabs, ensure_ascii=False))

    def test_a_client_term_in_the_merge_answer_fails_the_tab(self) -> None:  # A12
        sheets = long_x_feed(200)
        fake = FakeGateway(merge_answer=model_answer([], summary="สรุปที่มีชื่อ OakClientCo หลุดมา"))
        result = quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        self.assertEqual(result.exit_code, 1)
        row = [r for r in output_rows(sheets) if r["tab"] == "X Feed"][0]
        self.assertEqual((row["status"], row["error"]), ("error", "client_term_in_output"))
        self.assertNotIn("OakClientCo", json.dumps(sheets.tabs, ensure_ascii=False))

    def test_partials_ask_for_short_answers_and_the_final_merge_does_not(self) -> None:  # A10
        sheets = long_x_feed(200)
        fake = FakeGateway(summary="สรุปยาว" * 280)
        quiet_run(sheets, client(fake), DENYLIST, now=NOW)
        batches, merges = x_calls(fake)
        self.assertTrue(all(b["messages"][0]["content"].endswith(SHORT_NOTE) for b in batches + merges[:-1]))
        self.assertFalse(merges[-1]["messages"][0]["content"].endswith(SHORT_NOTE))

    def test_a_single_batch_keeps_the_plain_prompt(self) -> None:
        fake = FakeGateway()
        quiet_run(sheet(), client(fake), DENYLIST, now=NOW)
        self.assertTrue(all(not b["messages"][0]["content"].endswith(SHORT_NOTE) for b in fake.generate_bodies))
        self.assertTrue(all(b["prompt_version"] != MERGE_PROMPT_VERSION for b in fake.generate_bodies))


class GoogleAuthTests(unittest.TestCase):
    def test_service_account_jwt_exchange(self) -> None:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption()).decode()
        account = {"client_email": "fake@test.iam.gserviceaccount.com", "private_key": pem,
                   "private_key_id": "k1", "token_uri": "https://oauth2.test/token"}
        seen: dict[str, str] = {}

        def token(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            seen["body"] = request.content.decode()
            return httpx.Response(200, json={"access_token": "ya29.fake"})

        http = httpx.Client(transport=httpx.MockTransport(token))
        self.assertEqual(google_access_token(account, http), "ya29.fake")
        self.assertEqual(seen["url"], "https://oauth2.test/token")
        self.assertIn("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer", seen["body"])

    def test_bad_service_account_value_is_never_echoed(self) -> None:
        secret = "-----BEGIN PRIVATE KEY-----MIIEsecretmaterial"
        for raw in (secret, '{"private_key": "' + secret + '"', "\ufeff not-json " + secret):
            with self.assertRaises(RuntimeError) as caught:
                _service_account(raw)
            self.assertNotIn("secretmaterial", str(caught.exception))
            self.assertIsNone(caught.exception.__cause__)
            self.assertTrue(caught.exception.__suppress_context__)

    def test_link_source(self) -> None:
        self.assertEqual(link_source("https://x.com/a/status/1"), "x_post")
        self.assertEqual(link_source("https://mobile.twitter.com/a/status/1"), "x_post")
        self.assertEqual(link_source("https://x.com/a"), "website")
        self.assertEqual(link_source("https://example.com/status/1"), "website")


class LoggingTests(unittest.TestCase):
    def test_logs_carry_no_content_or_token(self) -> None:  # A8
        rows = finding_rows(2)
        rows.append(["2026-10-25", SECRET_TEXT, SECRET_TEXT, "https://example.test/secret", ""])
        sheets = sheet(**{"Chief Property": rows})
        out, err = io.StringIO(), io.StringIO()
        failing = FakeGateway(failures=[httpx.Response(500, json={"error": SECRET_TEXT, "code": "x"})] * 9)
        for fake in (FakeGateway(), failing):
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                run(sheets if fake is failing else sheet(**{"Chief Property": rows}), client(fake), DENYLIST, now=NOW)
        logs = out.getvalue() + err.getvalue()
        self.assertTrue(logs.strip())
        for needle in (SECRET_TEXT, TOKEN, "สรุปภาษาไทย", "https://example.test", "หัวข้อ"):
            self.assertNotIn(needle, logs)
        for line in logs.strip().splitlines():
            record = json.loads(line)
            self.assertLessEqual(set(record), {"run_id", "tab", "status", "tokens_in", "tokens_out",
                                               "cost_usd", "latency_ms", "error", "event"})


if __name__ == "__main__":
    unittest.main()
