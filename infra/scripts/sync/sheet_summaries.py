#!/usr/bin/env python3
"""Daily Thai AI summaries for the "Chief Findings Warehouse" Google Sheet.

Spec: docs/specs/sheet-summaries.md. Operator docs: docs/sheet-summaries.md.

Reads each Chief/data tab, sends only rows not yet summarized (first run: last 7 days) to the
owner's AI gateway (``solo-empire.ai-gateway.v1``, route ``sheet-summaries``, profile ``fast`` =
GPT-6 Luna), validates the JSON answer (schema + no invented links + no client terms) and appends
one row per tab to ``AI Summaries``; ``AI Summaries Latest`` holds the newest row per tab. Source
tabs are never written. Rows that match the client denylist never leave the host, and the run fails
closed without a denylist. Logs carry run id, tab, status, tokens, cost and latency only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import unicodedata
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, unquote, urlsplit

import httpx
import jsonschema

SPREADSHEET_ID = "1zajA2iEfa9TS5JNjV1Y7LOurrTmiqrP-8ygn9clvBdI"
GATEWAY_URL = "https://api.bookchaowalit.com"
HEARTBEAT_URL = "https://solo-empire-monitor.bookchaowalit.workers.dev/heartbeat/sheet-summaries"
BANGKOK = timezone(timedelta(hours=7))

OUTPUT_TAB = "AI Summaries"
LATEST_TAB = "AI Summaries Latest"
OUTPUT_HEADER = [
    "tab", "summary_date", "window", "rows_considered", "summary_th", "top_items", "candidate_tasks",
    "deadlines", "model", "run_id", "tokens_in", "tokens_out", "cost_usd", "status", "error",
]
FINDING_TABS = (
    "All Findings", "Chief Market Strategist", "Chief Intelligence Officer", "Chief Oppotunities",
    "Chief Business Builder", "Chief Finance", "Chief SME Operations", "Chief Property",
    "Chief Health & Lifestyle", "Chief Career", "Chief of Staff", "Chief Social Media",
)
DATA_TABS = ("Datasets", "X Feed")
SKIP_TABS = frozenset({"README", "X Watchlist", "X Query Plan", "Retired Bots Archive", OUTPUT_TAB, LATEST_TAB})

ROUTE = "sheet-summaries"
PROMPT_VERSION = "sheet-summaries-v1"
MERGE_PROMPT_VERSION = "sheet-summaries-merge-v1"
MAX_ROWS = 200
# A tab larger than one request is summarized in up to this many batches (highest priority rows
# first), then one merge call combines the validated batch answers. X Feed gets ~1,400 rows a day.
MAX_BATCHES = 8
MAX_CELL_CHARS = 500
FIRST_RUN_DAYS = 7
MAX_OUTPUT_TOKENS = 4096
# GPT-6 Luna reasons before it answers; at the default effort it spent a 2048-token cap on
# reasoning and returned no content (finish_reason=length). "low" keeps the answer within the cap.
REASONING_EFFORT = "low"
# The gateway appends its Thai language policy (~650 bytes UTF-8) to the system message.
GATEWAY_OVERHEAD_BYTES = 768
# "date logged" sits ahead of the generic "*date*" match so the Chief tabs order by Date Logged, not
# Date Found: a finding logged late is still newer than the watermark.
DATE_COLUMNS = ("collected_at", "posted_at", "found_at", "date", "created_at", "updated_at", "timestamp",
                "date logged")
SUCCESS_STATUSES = frozenset({"ok", "budget_exceeded", "no_change"})
# A watermark keeps at most this many row hashes per list; the window cell must stay under the
# 50,000-character Sheets cell limit with room to spare.
MAX_SEEN = 300
MAX_WINDOW_CHARS = 30000
STOP_CODES_NO_RETRY = frozenset({"monthly_budget_exceeded", "budget_unavailable", "budget_not_configured"})
URL_RE = re.compile(r"https?://[^\s\"'<>]+")
TRUE_VALUES = frozenset({"true", "1", "yes", "y", "top", "✓", "x"})
LOG_KEYS = ("event", "run_id", "tab", "status", "tokens_in", "tokens_out", "cost_usd", "latency_ms", "error")

SYSTEM_PROMPT = """You summarize one tab of a research spreadsheet for its owner.
Write in Thai. Use only facts and links that appear in the rows; never invent a link, name or number.
Reply with one JSON object only, no prose, with exactly these keys:
{"summary_th": "3-6 Thai sentences on what is new and important",
 "top_items": [{"title": "", "why": "", "link": "", "link_source": "x_post|website"}],
 "candidate_tasks": [{"title": "", "value": "", "deadline": "YYYY-MM-DD or null", "link": "url or null", "suggested_owner_role": ""}],
 "deadlines": [{"date": "YYYY-MM-DD", "what": "", "link": "url or null"}]}
top_items: at most 5, each link copied exactly from a row. candidate_tasks: at most 5 suggestions for the owner's backlog.
deadlines: only dated items found in the rows; use [] when there are none."""

MERGE_PROMPT = """You merge partial summaries of one tab of a research spreadsheet into one summary for its owner.
Each partial covers a different batch of the tab's new rows; the first partials cover the most important rows.
Write in Thai. Use only facts and links that appear in the partial summaries; never invent a link, name or number.
Reply with one JSON object only, no prose, with exactly these keys (every item keeps all of its fields):
{"summary_th": "3-6 Thai sentences covering all partials",
 "top_items": [{"title": "", "why": "", "link": "", "link_source": "x_post|website"}],
 "candidate_tasks": [{"title": "", "value": "", "deadline": "YYYY-MM-DD or null", "link": "url or null", "suggested_owner_role": ""}],
 "deadlines": [{"date": "YYYY-MM-DD", "what": "", "link": "url or null"}]}
top_items and candidate_tasks: at most 5 each, links copied exactly from the partials. deadlines: [] when there are none."""

# Appended to batch calls and to merges that are not the last one, so the partial answers stay small
# enough for several to share one merge call (Thai is 3 bytes a character against a byte budget).
RETRY_MERGE_CODES = frozenset({"invalid_output", "output_truncated", "invented_link"})
SHORT_NOTE = """
This answer is a partial that will be merged with others: keep it short. summary_th at most 3 sentences;
every title, why, value and what at most 15 words."""

_STR = {"type": "string"}
_NULLABLE_STR = {"type": ["string", "null"]}
OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["summary_th", "top_items", "candidate_tasks", "deadlines"],
    "properties": {
        "summary_th": {"type": "string", "minLength": 10, "maxLength": 2000},
        "top_items": {
            "type": "array", "maxItems": 5,
            "items": {"type": "object", "required": ["title", "why", "link"],
                      "properties": {"title": _STR, "why": _STR, "link": {"type": "string", "minLength": 8},
                                     "link_source": _STR}},
        },
        "candidate_tasks": {
            "type": "array", "maxItems": 5,
            "items": {"type": "object", "required": ["title", "value", "suggested_owner_role"],
                      "properties": {"title": _STR, "value": _STR, "deadline": _NULLABLE_STR,
                                     "link": _NULLABLE_STR, "suggested_owner_role": _STR}},
        },
        "deadlines": {
            "type": "array", "maxItems": 20,
            "items": {"type": "object", "required": ["date", "what"],
                      "properties": {"date": _STR, "what": _STR, "link": _NULLABLE_STR}},
        },
    },
}


class DenylistError(RuntimeError):
    """The client denylist is missing or empty; the run must not read the sheet."""


class GatewayStop(RuntimeError):
    """The gateway refused or failed in a way that stops the whole run."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class TabError(RuntimeError):
    """One tab failed (bad request, invalid or unsafe answer); the run continues."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


# --------------------------------------------------------------------------------------------
# Sheets
# --------------------------------------------------------------------------------------------


class Sheets(Protocol):
    def titles(self) -> list[str]: ...
    def read(self, titles: Sequence[str]) -> dict[str, list[list[str]]]: ...
    def ensure_tab(self, title: str, header: list[str]) -> None: ...
    def append(self, title: str, rows: list[list[str]]) -> None: ...
    def replace(self, title: str, rows: list[list[str]]) -> None: ...


class MemorySheets:
    """In-memory sheet for tests and ``--fixture`` dry runs; records every write."""

    def __init__(self, tabs: dict[str, list[list[str]]]) -> None:
        self.tabs = {title: [list(row) for row in rows] for title, rows in tabs.items()}
        self.writes: list[tuple[str, int]] = []

    def titles(self) -> list[str]:
        return list(self.tabs)

    def read(self, titles: Sequence[str]) -> dict[str, list[list[str]]]:
        return {title: [list(row) for row in self.tabs.get(title, [])] for title in titles}

    def ensure_tab(self, title: str, header: list[str]) -> None:
        if title not in self.tabs:
            self.tabs[title] = [list(header)]
            self.writes.append((title, 1))

    def append(self, title: str, rows: list[list[str]]) -> None:
        self.tabs[title].extend(list(row) for row in rows)
        self.writes.append((title, len(rows)))

    def replace(self, title: str, rows: list[list[str]]) -> None:
        self.tabs[title] = [list(row) for row in rows]
        self.writes.append((title, len(rows)))


def _a1(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


class GoogleSheets:
    """Sheets API v4 over httpx with a service-account token (scope ``spreadsheets``)."""

    API = "https://sheets.googleapis.com/v4/spreadsheets"

    def __init__(self, spreadsheet_id: str, access_token: str, http: httpx.Client) -> None:
        self.base = f"{self.API}/{spreadsheet_id}"
        self.http = http
        self.headers = {"Authorization": f"Bearer {access_token}"}

    def _request(self, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
        response = self.http.request(method, url, headers=self.headers, timeout=60, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(f"sheets_http_{response.status_code}")
        return response.json() if response.content else {}

    def titles(self) -> list[str]:
        data = self._request("GET", self.base, params={"fields": "sheets.properties.title"})
        return [sheet["properties"]["title"] for sheet in data.get("sheets", [])]

    def read(self, titles: Sequence[str]) -> dict[str, list[list[str]]]:
        if not titles:
            return {}
        params = [("ranges", _a1(title)) for title in titles] + [("valueRenderOption", "FORMATTED_VALUE")]
        data = self._request("GET", f"{self.base}/values:batchGet", params=params)
        ranges = data.get("valueRanges", [])
        return {title: [[str(cell) for cell in row] for row in ranges[i].get("values", [])]
                for i, title in enumerate(titles)}

    def ensure_tab(self, title: str, header: list[str]) -> None:
        if title in self.titles():
            return
        self._request("POST", f"{self.base}:batchUpdate",
                      json={"requests": [{"addSheet": {"properties": {"title": title}}}]})
        self._request("PUT", f"{self.base}/values/{quote(_a1(title) + '!A1')}",
                      params={"valueInputOption": "RAW"}, json={"values": [header]})

    def append(self, title: str, rows: list[list[str]]) -> None:
        self._request("POST", f"{self.base}/values/{quote(_a1(title) + '!A1')}:append",
                      params={"valueInputOption": "RAW", "insertDataOption": "INSERT_ROWS"},
                      json={"values": rows})

    def replace(self, title: str, rows: list[list[str]]) -> None:
        self._request("POST", f"{self.base}/values/{quote(_a1(title))}:clear", json={})
        self._request("PUT", f"{self.base}/values/{quote(_a1(title) + '!A1')}",
                      params={"valueInputOption": "RAW"}, json={"values": rows})


def google_access_token(service_account: dict[str, Any], http: httpx.Client) -> str:
    """Exchange a service-account key for an access token (JWT bearer grant, scope spreadsheets)."""

    from google.auth import crypt, jwt  # optional extra "sheets"

    issued = int(time.time())
    token_uri = service_account.get("token_uri", "https://oauth2.googleapis.com/token")
    claims = {
        "iss": service_account["client_email"],
        "scope": "https://www.googleapis.com/auth/spreadsheets",
        "aud": token_uri,
        "iat": issued,
        "exp": issued + 3600,
    }
    signer = crypt.RSASigner.from_service_account_info(service_account)
    assertion = jwt.encode(signer, claims).decode("ascii")
    response = http.post(token_uri, timeout=30, data={
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion})
    if response.status_code != 200:
        raise RuntimeError(f"google_token_http_{response.status_code}")
    return str(response.json()["access_token"])


# --------------------------------------------------------------------------------------------
# Gateway
# --------------------------------------------------------------------------------------------


class Gateway(Protocol):
    def status(self) -> dict[str, Any]: ...
    def generate(self, body: dict[str, Any]) -> dict[str, Any]: ...


class GatewayClient:
    """``solo-empire.ai-gateway.v1`` client: one retry at most, then :class:`GatewayStop`."""

    def __init__(self, http: httpx.Client, token: str, *, sleep: Callable[[float], None] = time.sleep) -> None:
        self.http = http
        self.headers = {"Authorization": f"Bearer {token}"}
        self.sleep = sleep

    def status(self) -> dict[str, Any]:
        return self._call("GET", "/api/ai/status", None)

    def generate(self, body: dict[str, Any]) -> dict[str, Any]:
        return self._call("POST", "/api/ai/generate", body)

    def _call(self, method: str, path: str, body: dict[str, Any] | None) -> dict[str, Any]:
        for attempt in (1, 2):
            try:
                response = self.http.request(method, path, headers=self.headers, json=body, timeout=120)
            except httpx.HTTPError:
                if attempt == 1:
                    self.sleep(5)
                    continue
                raise GatewayStop("network_error") from None
            if response.status_code == 200:
                return response.json()
            code = _error_code(response)
            if response.status_code == 400:
                raise TabError(f"gateway_400:{code or 'bad_request'}")
            if response.status_code in (401, 403):
                raise GatewayStop(f"http_{response.status_code}")
            if code in STOP_CODES_NO_RETRY:
                raise GatewayStop(code)
            if response.status_code == 429:
                if attempt == 1:
                    self.sleep(min(_retry_after(response), 30.0))
                    continue
                raise GatewayStop(code or "http_429")
            if response.status_code >= 500 and attempt == 1:
                self.sleep(5)
                continue
            raise GatewayStop(f"http_{response.status_code}")
        raise GatewayStop("retry_exhausted")  # pragma: no cover - loop always returns or raises


def _error_code(response: httpx.Response) -> str:
    try:
        code = response.json().get("code")
    except (ValueError, AttributeError):
        return ""
    return code if isinstance(code, str) and re.fullmatch(r"[a-z0-9_]{1,64}", code) else ""


def _retry_after(response: httpx.Response) -> float:
    try:
        return max(1.0, float(response.headers.get("retry-after", "5")))
    except ValueError:
        return 5.0


class FakeGateway:
    """``--fake-gateway``: answers from the rows themselves, no network, zero cost."""

    def status(self) -> dict[str, Any]:
        return {"budget": {"decision": "allow"},
                "limits": {"max_context_chars": 48000, "max_output_tokens": 4096, "max_token_budget": 32768}}

    def generate(self, body: dict[str, Any]) -> dict[str, Any]:
        prompt = body["messages"][-1]["content"]
        links = list(dict.fromkeys(URL_RE.findall(prompt)))[:3]
        answer = {
            "summary_th": "(ตัวอย่างจาก --fake-gateway) แท็บนี้มีแถวใหม่ตามช่วงที่ระบุ ยังไม่ได้เรียกโมเดลจริง",
            "top_items": [{"title": "แถวตัวอย่าง", "why": "ลิงก์จริงจากแถว", "link": link, "link_source": "website"}
                          for link in links],
            "candidate_tasks": [],
            "deadlines": [],
        }
        return {"run_id": "fake-" + uuid.uuid4().hex[:8], "status": "success", "model": "fake",
                "content": json.dumps(answer, ensure_ascii=False),
                "usage": {"input_tokens": 0, "output_tokens": 0}, "estimated_cost_usd": 0}


# --------------------------------------------------------------------------------------------
# Selection, guard, prompt, validation
# --------------------------------------------------------------------------------------------


def select_tabs(titles: Iterable[str]) -> list[str]:
    """Known finding/data tabs plus any ``Chief *`` tab, in sheet order, minus the skip list."""

    known = set(FINDING_TABS) | set(DATA_TABS)
    return [t for t in titles if t not in SKIP_TABS and (t in known or t.startswith("Chief "))]


def load_denylist(path: str | None) -> tuple[str, ...]:
    """Client terms, lower-cased; any problem raises :class:`DenylistError` (fail closed)."""

    if not path:
        raise DenylistError("CLIENT_DENYLIST_PATH is not set")
    file = Path(path)
    if not file.is_file():
        raise DenylistError("client denylist file is missing")
    terms = tuple(
        normalize_text(line)
        for line in file.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    )
    if not terms:
        raise DenylistError("client denylist has no terms")
    return terms


def normalize_text(text: str) -> str:
    """Case-folded NFKC form of a term or text (the plain comparison)."""

    return unicodedata.normalize("NFKC", text).casefold().strip()


def skeleton(text: str) -> str:
    """Letters and digits only, after NFKD and case folding.

    Drops every format, combining, invisible, space and punctuation character on both sides, so
    zero-width joiners, variation selectors, combining marks or NFKC composition cannot split or
    merge a client term. It is stricter (more rows match), never looser, than the plain form.
    """

    return "".join(c for c in unicodedata.normalize("NFKD", text.casefold()) if c.isalnum())


def _contains_term(text: str, denylist: Sequence[str]) -> bool:
    plain = normalize_text(text)
    bare = skeleton(text)
    return any(term in plain or (skeleton(term) and skeleton(term) in bare) for term in denylist)


@dataclass(frozen=True)
class Row:
    number: int  # 1-based sheet row
    cells: dict[str, str]


def parse_rows(values: list[list[str]]) -> tuple[list[str], list[Row]]:
    if not values:
        return [], []
    header = [h.strip() or f"col_{i + 1}" for i, h in enumerate(values[0])]
    rows = []
    for offset, raw in enumerate(values[1:], start=2):
        cells = {header[i]: str(v).strip() for i, v in enumerate(raw[: len(header)]) if str(v).strip()}
        if cells:
            rows.append(Row(offset, cells))
    return header, rows


def _date_column(header: list[str]) -> str | None:
    lowered = {h.lower(): h for h in header}
    for name in DATE_COLUMNS:
        if name in lowered:
            return lowered[name]
    return next((h for h in header if "date" in h.lower()), None)


def _row_date(value: str) -> datetime | None:
    match = re.match(r"(\d{4}-\d{2}-\d{2})", value)
    if not match:
        return None
    try:
        return datetime.fromisoformat(match.group(1)).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _row_time(value: str) -> datetime | None:
    """A ``collected_at`` / ``summary_date`` value with its time of day; a naive value is Asia/Bangkok."""

    text = value.strip()
    if not re.match(r"\d{4}-\d{2}-\d{2}", text):
        return None
    try:
        stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=BANGKOK)


def _is_true(value: str) -> bool:
    return value.strip().lower() in TRUE_VALUES


def _score(value: str) -> float:
    try:
        return float(value)
    except ValueError:
        return 0.0


def _chiefs(value: str) -> list[str]:
    return [name.strip() for name in re.split(r"[;,]", value) if name.strip()]


def _priority(tab: str, rows: list[Row]) -> list[Row]:
    """X Feed: every ``top`` row before any other; within each group the requesting Chiefs (first one
    named on the row) take turns, best ``score`` first, so one busy Chief cannot fill the request.
    Other tabs: newest rows first."""

    if tab != "X Feed":
        return sorted(rows, key=lambda r: r.number, reverse=True)
    ordered: list[Row] = []
    for top in (True, False):
        queues: dict[str, list[Row]] = {}
        for row in sorted((r for r in rows if _is_true(r.cells.get("top", "")) == top),
                          key=lambda r: (_score(r.cells.get("score", "")), r.number), reverse=True):
            queues.setdefault((_chiefs(row.cells.get("requesting_chief", "")) or [""])[0], []).append(row)
        while queues:
            for chief in list(queues):
                ordered.append(queues[chief].pop(0))
                if not queues[chief]:
                    del queues[chief]
    return ordered


def _row_line(row: Row) -> str:
    cells = {k: (v if len(v) <= MAX_CELL_CHARS else v[: MAX_CELL_CHARS - 1] + "…") for k, v in row.cells.items()}
    return json.dumps({"row": row.number, **cells}, ensure_ascii=False)


def _owners_line(owners: Sequence[str]) -> str:
    if not owners:
        return ""
    return "suggested_owner_role must be exactly one of: " + " | ".join(owners) + "\n"


def _user_message(tab: str, lines: list[str], owners: Sequence[str] = ()) -> str:
    return (f"Tab: {tab}\n{_owners_line(owners)}New rows ({len(lines)}), one JSON object per line:\n"
            + "\n".join(lines))


def _bytes(text: str) -> int:
    return len(text.encode("utf-8")) + 32


@dataclass
class Limits:
    max_context_chars: int
    max_token_budget: int
    max_output_tokens: int

    @classmethod
    def from_status(cls, status: dict[str, Any]) -> Limits:
        limits = status.get("limits") or {}
        return cls(
            max_context_chars=int(limits.get("max_context_chars") or 48000),
            max_token_budget=int(limits.get("max_token_budget") or limits.get("default_token_budget") or 4096),
            max_output_tokens=int(limits.get("max_output_tokens") or 4096),
        )

    @property
    def output_tokens(self) -> int:
        return max(256, min(MAX_OUTPUT_TOKENS, self.max_output_tokens, self.max_token_budget // 4))


def _body(system: str, user: str, prompt_version: str, limits: Limits) -> dict[str, Any]:
    output = limits.output_tokens
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    estimated = sum(_bytes(m["content"]) for m in messages)
    return {
        "route": ROUTE,
        "prompt_version": prompt_version,
        "profile": "fast",
        "privacy_class": "internal",
        "response_format": {"type": "json_object"},
        "reasoning_effort": REASONING_EFFORT,
        "max_output_tokens": output,
        "token_budget": min(limits.max_token_budget, max(256, estimated + output + GATEWAY_OVERHEAD_BYTES)),
        "messages": messages,
    }


def _room(limits: Limits) -> tuple[int, int]:
    """Input bytes and characters one request may use, gateway overhead and output reserved."""

    return (limits.max_token_budget - limits.output_tokens - GATEWAY_OVERHEAD_BYTES,
            limits.max_context_chars - GATEWAY_OVERHEAD_BYTES)


def build_batches(tab: str, rows: list[Row], limits: Limits,
                  owners: Sequence[str] = ()) -> list[tuple[dict[str, Any], list[Row]]]:
    """Pack the highest-priority rows into at most MAX_BATCHES requests that fit the gateway limits.

    Batch 1 holds the most important rows. A row that does not fit the current batch opens the next
    one; rows left over once every batch is full are not sent.
    """

    byte_room, char_room = _room(limits)
    system = SYSTEM_PROMPT + SHORT_NOTE  # sized for the longer prompt; a single batch drops the note
    base_bytes = _bytes(system) + _bytes(_user_message(tab, [], owners))
    base_chars = len(system) + len(_user_message(tab, [], owners))
    batches: list[list[Row]] = []
    used_bytes = used_chars = 0
    for row in _priority(tab, rows)[:MAX_ROWS]:
        line = _row_line(row)
        cost, chars = len(line.encode("utf-8")) + 1, len(line) + 1
        if base_bytes + cost > byte_room or base_chars + chars > char_room:
            continue  # too large even alone
        if not batches or used_bytes + cost > byte_room or used_chars + chars > char_room:
            if len(batches) == MAX_BATCHES:
                continue  # a smaller row may still fit the last batch
            batches.append([])
            used_bytes, used_chars = base_bytes, base_chars
        batches[-1].append(row)
        used_bytes += cost
        used_chars += chars
    if not batches:
        raise TabError("row_too_large")
    result = []
    system = SYSTEM_PROMPT if len(batches) == 1 else system
    for chosen in batches:
        chosen.sort(key=lambda r: r.number)
        user = _user_message(tab, [_row_line(r) for r in chosen], owners)
        result.append((_body(system, user, PROMPT_VERSION, limits), chosen))
    return result


def build_request(tab: str, rows: list[Row], limits: Limits) -> tuple[dict[str, Any], list[Row]]:
    """The first (highest-priority) batch only."""

    return build_batches(tab, rows, limits)[0]


def _answer_links(answer: dict[str, Any]) -> set[str]:
    links = {item["link"] for item in answer["top_items"]}
    return links | {item["link"] for item in answer["candidate_tasks"] + answer["deadlines"] if item.get("link")}


def build_merge(tab: str, answers: list[dict[str, Any]], limits: Limits,
                *, first: bool = True, owners: Sequence[str] = ()) -> tuple[dict[str, Any] | None, int]:
    """One merge request holding as many answers as fit, from the first; returns the body and that
    count; (None, 1) when not even the first two fit together. Only a merge of every answer starting
    from the first is final (full length); any other merge is asked for a short answer."""

    byte_room, char_room = _room(limits)
    for count in range(len(answers), 1, -1):
        lines = [json.dumps(a, ensure_ascii=False) for a in answers[:count]]
        user = (f"Tab: {tab}\n{_owners_line(owners)}Partial summaries ({count}), one JSON object per line:\n"
                + "\n".join(lines))
        system = MERGE_PROMPT if first and count == len(answers) else MERGE_PROMPT + SHORT_NOTE
        body = _body(system, user, MERGE_PROMPT_VERSION, limits)
        if (sum(_bytes(m["content"]) for m in body["messages"]) <= byte_room
                and sum(len(m["content"]) for m in body["messages"]) <= char_room):
            return body, count
    return None, 1


def link_source(url: str) -> str:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower().removeprefix("www.").removeprefix("mobile.")
    return "x_post" if host in {"x.com", "twitter.com"} and "/status/" in parts.path else "website"


def validate_answer(content: str, allowed_links: set[str], denylist: Sequence[str]) -> dict[str, Any]:
    try:
        answer = json.loads(content)
        jsonschema.validate(answer, OUTPUT_SCHEMA)
    except (ValueError, jsonschema.ValidationError):
        raise TabError("invalid_output") from None
    links = [item["link"] for item in answer["top_items"]]
    links += [item.get("link") for item in answer["candidate_tasks"] + answer["deadlines"] if item.get("link")]
    if any(link not in allowed_links for link in links):
        raise TabError("invented_link")
    if _contains_term(json.dumps(answer, ensure_ascii=False), denylist):
        raise TabError("client_term_in_output")
    for item in answer["top_items"]:
        item["link_source"] = link_source(item["link"])
    return answer


# --------------------------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------------------------


@dataclass
class RunResult:
    rows: list[list[str]] = field(default_factory=list)
    exit_code: int = 0


def _log(**fields: Any) -> None:
    print(json.dumps({k: fields[k] for k in LOG_KEYS if k in fields}, ensure_ascii=True), flush=True)


@dataclass(frozen=True)
class Watermark:
    """Where a tab's summaries stopped; kept in the ``window`` cell after ``after_row;to_row``.

    ``post_id`` (X Feed): the last summarized row's ``post_id`` and ``collected_at``. ``date_hash``
    (tabs with a date column): the newest date consumed and the hashes of the rows on that date, plus
    the hashes of rows without a readable date. ``hash`` (no date column): the hashes of the last
    MAX_SEEN rows. ``legacy``: an old row-number window, resolved once by :func:`_migrate`. Row
    numbers are never used to pick rows, so pruning, deleting or inserting rows cannot skip or repeat
    one.
    """

    kind: str
    last_id: str = ""
    last_at: str = ""
    date_col: str = ""
    last_date: str = ""
    seen: tuple[str, ...] = ()
    seen_nodate: tuple[str, ...] = ()
    to_row: int = 0  # legacy only
    summary_date: str = ""  # legacy only: the summary_date of the history row it came from


KEY_FIELDS = {
    "post_id": ("last_id", "last_at"),
    "date_hash": ("date_col", "last_date", "seen", "seen_nodate"),
    "hash": ("seen",),
}
HASH_LISTS = frozenset({"seen", "seen_nodate"})


def format_window(after: int, last: int, mark: Watermark) -> str:
    """``after_row=<a>;to_row=<b>`` (informational, and what a rolled-back version reads) followed by
    the key fields, each value URL-quoted so ``;`` and ``=`` inside it cannot break parsing. Hash
    lists are trimmed from the oldest end if the cell would pass MAX_WINDOW_CHARS."""

    while True:
        parts = [f"after_row={after}", f"to_row={last}", f"kind={mark.kind}"]
        for name in KEY_FIELDS[mark.kind]:
            value = getattr(mark, name)
            parts.append(f"{name}={quote(','.join(value) if name in HASH_LISTS else value, safe='')}")
        text = ";".join(parts)
        if len(text) <= MAX_WINDOW_CHARS or not (mark.seen or mark.seen_nodate):
            return text
        mark = replace(mark, seen=mark.seen[len(mark.seen) // 2:],
                       seen_nodate=mark.seen_nodate[len(mark.seen_nodate) // 2:])


def parse_window(window: str, summary_date: str = "") -> Watermark | None:
    fields: dict[str, str] = {}
    for part in window.split(";"):
        name, _sep, value = part.partition("=")
        fields[name] = value
    kind = fields.get("kind", "")
    if kind in KEY_FIELDS:
        values: dict[str, Any] = {}
        for name in KEY_FIELDS[kind]:
            text = unquote(fields.get(name, ""))
            values[name] = tuple(h for h in text.split(",") if h) if name in HASH_LISTS else text
        return Watermark(kind, **values)
    if fields.get("to_row", "").isdigit():
        return Watermark("legacy", to_row=int(fields["to_row"]), summary_date=summary_date)
    return None


def _watermarks(history: list[list[str]]) -> dict[str, Watermark]:
    marks: dict[str, Watermark] = {}
    if not history:
        return marks
    header = history[0]
    for raw in history[1:]:
        record = dict(zip(header, raw))
        if record.get("status") not in SUCCESS_STATUSES:
            continue
        mark = parse_window(record.get("window", ""), record.get("summary_date", ""))
        if mark and record.get("tab"):
            marks[record["tab"]] = mark  # later rows win
    return marks


def row_hash(row: Row) -> str:
    """First 12 hex characters of sha256 over the row's cells (never its row number)."""

    cells = json.dumps(row.cells, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(cells.encode("utf-8")).hexdigest()[:12]


def _cursor_kind(header: list[str]) -> tuple[str, str]:
    if "post_id" in header and "collected_at" in header:
        return "post_id", "collected_at"
    column = _date_column(header)
    return ("date_hash", column) if column else ("hash", "")


def _row_day(row: Row, column: str) -> str | None:
    stamp = _row_date(row.cells.get(column, "")) if column else None
    return stamp.date().isoformat() if stamp else None


def _after_time(rows: list[Row], column: str, cutoff: datetime) -> list[Row]:
    """Rows whose ``column`` time is after ``cutoff``. A row without a readable time is new only when
    it sits after the last readable row at or before the cutoff (it was appended after it)."""

    stamps = [_row_time(r.cells.get(column, "")) for r in rows]
    boundary = max((i for i, s in enumerate(stamps) if s is not None and s <= cutoff), default=-1)
    return [r for i, (r, s) in enumerate(zip(rows, stamps)) if (s > cutoff if s is not None else i > boundary)]


def _first_run_rows(column: str, rows: list[Row], now: datetime) -> list[Row]:
    if not column:
        return rows[-MAX_ROWS:]
    since = (now - timedelta(days=FIRST_RUN_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    recent = []
    for row in rows:
        stamp = _row_date(row.cells.get(column, ""))
        if stamp is None or stamp >= since:
            recent.append(row)
    return recent


def _new_rows(kind: str, column: str, rows: list[Row], mark: Watermark) -> list[Row] | None:
    """Rows after ``mark``; None when an X Feed anchor is gone and its time cannot be read."""

    if kind == "post_id":
        ids = [r.cells.get("post_id", "") for r in rows]
        if mark.last_id and mark.last_id in ids:
            return rows[ids.index(mark.last_id) + 1:]
        cutoff = _row_time(mark.last_at)
        return None if cutoff is None else _after_time(rows, column, cutoff)
    if kind == "hash":
        seen = set(mark.seen)
        return [r for r in rows if row_hash(r) not in seen]
    seen, nodate = set(mark.seen), set(mark.seen_nodate)
    new = []
    for row in rows:
        day = _row_day(row, column)
        if day is None:
            if row_hash(row) not in nodate:
                new.append(row)
        elif day > mark.last_date or (day == mark.last_date and row_hash(row) not in seen):
            new.append(row)
    return new


def _migrate(tab: str, kind: str, column: str, rows: list[Row], mark: Watermark) -> list[Row]:
    """Resolve a legacy ``to_row`` watermark once (spec item 3a); never the 7-day fallback.

    The row at ``to_row`` is trusted as the old anchor only when it is no newer than the legacy run;
    otherwise the tab was pruned or shifted and the legacy ``summary_date`` is the cutoff.
    """

    after = [r for r in rows if r.number > mark.to_row]
    if kind == "hash":
        return after  # a tab shorter than to_row: nothing new, the seen list is seeded
    cutoff = _row_time(mark.summary_date)
    anchor = next((r for r in rows if r.number == mark.to_row), None)
    if anchor is not None and cutoff is not None:
        if kind == "post_id":
            stamp = _row_time(anchor.cells.get(column, ""))
            trusted = stamp is not None and stamp <= cutoff
        else:
            day = _row_day(anchor, column)
            trusted = day is not None and day <= cutoff.astimezone(BANGKOK).date().isoformat()
        if trusted:
            return after
    _log(event="migration_fallback", tab=tab)
    if cutoff is None:
        return []
    if kind == "post_id":
        return _after_time(rows, column, cutoff)
    # Dates carry no time of day: rows from the legacy run's own day are included, so one repeat
    # is possible.
    since = cutoff.astimezone(BANGKOK).date().isoformat()
    return [r for r in rows if (day := _row_day(r, column)) is not None and day >= since]


def _consumed(kind: str, column: str, rows: list[Row], prev: Watermark | None) -> Watermark:
    """The watermark once every row now in the tab is consumed (rows past MAX_BATCHES included)."""

    if kind == "post_id":
        anchor = next((r for r in reversed(rows) if r.cells.get("post_id")), None)
        if anchor is None:
            return prev if prev is not None and prev.kind == kind else Watermark(kind)
        return Watermark(kind, last_id=anchor.cells["post_id"], last_at=anchor.cells.get(column, ""))
    hashed = [(row_hash(r), _row_day(r, column)) for r in rows]
    if kind == "hash":
        return Watermark(kind, seen=tuple(h for h, _day in hashed)[-MAX_SEEN:])
    days = [day for _h, day in hashed if day]
    if prev is not None and prev.kind == kind and prev.last_date:
        days.append(prev.last_date)  # a prune of every row of the last date must not move it back
    last_date = max(days, default="")
    return Watermark(kind, date_col=column, last_date=last_date,
                     seen=tuple(h for h, day in hashed if day and day == last_date)[-MAX_SEEN:],
                     seen_nodate=tuple(h for h, day in hashed if day is None)[-MAX_SEEN:])


def select_new_rows(tab: str, header: list[str], rows: list[Row], mark: Watermark | None,
                    now: datetime) -> tuple[list[Row], Watermark]:
    """The rows not yet summarized, in sheet order, and the watermark once they are (spec item 3).

    With nothing new the previous watermark is returned unchanged (``no_change`` carries it).
    """

    kind, column = _cursor_kind(header)
    if mark is not None and kind == "date_hash" and mark.kind == kind and mark.date_col in header:
        column = mark.date_col
    if mark is None or (mark.kind == "post_id" and kind == "post_id" and not (mark.last_id or mark.last_at)):
        window = _first_run_rows(column, rows, now)
    elif mark.kind == "legacy":
        window = _migrate(tab, kind, column, rows, mark)
    elif mark.kind != kind:
        _log(event="cursor_reset", tab=tab)  # the tab's columns changed: start from what is there now
        return [], _consumed(kind, column, rows, None)
    else:
        found = _new_rows(kind, column, rows, mark)
        if found is None:
            _log(event="cursor_reset", tab=tab)
            return [], _consumed(kind, column, rows, None)
        if not found:
            return [], mark
        window = found
    return window, _consumed(kind, column, rows, mark if mark is not None and mark.kind == kind else None)


def _output_row(tab: str, now: datetime, window: str, considered: int, status: str, *, error: str = "",
                answer: dict[str, Any] | None = None, response: dict[str, Any] | None = None) -> list[str]:
    usage = (response or {}).get("usage") or {}
    cost = (response or {}).get("estimated_cost_usd")
    dumps = (lambda v: json.dumps(v, ensure_ascii=False)) if answer else (lambda _v: "")
    return [
        tab, now.astimezone(BANGKOK).isoformat(timespec="seconds"), window, str(considered),
        answer["summary_th"] if answer else "",
        dumps(answer["top_items"] if answer else None),
        dumps(answer["candidate_tasks"] if answer else None),
        dumps(answer["deadlines"] if answer else None),
        str((response or {}).get("model") or ""), str((response or {}).get("run_id") or ""),
        "" if usage.get("input_tokens") is None else str(usage["input_tokens"]),
        "" if usage.get("output_tokens") is None else str(usage["output_tokens"]),
        "" if cost is None else f"{float(cost):.6f}",
        status, error,
    ]


def _call(body: dict[str, Any], allowed: set[str], gateway: Gateway, denylist: Sequence[str],
          spent: list[dict[str, Any]]) -> dict[str, Any]:
    """One validated answer; every response, kept or not, is added to ``spent`` for usage and cost."""

    if any(_contains_term(m["content"], denylist) for m in body["messages"]):
        raise TabError("client_term_in_input")
    response = gateway.generate(body)
    spent.append(response)
    if response.get("finish_reason") == "length":
        raise TabError("output_truncated")
    return validate_answer(str(response.get("content", "")), allowed, denylist)


def _summarize(tab: str, rows: list[Row], limits: Limits, gateway: Gateway, denylist: Sequence[str],
               owners: Sequence[str] = ()) -> tuple[dict[str, Any], dict[str, Any], int]:
    """One answer for the tab: a single call, or batch calls plus merge calls.

    A batch whose answer is invalid, truncated, has an invented link or a client term is dropped and
    never written. A merge with a malformed answer is tried once more; a failing merge fails the
    tab. Returns the answer, the combined response (usage and cost of every call) and the rows it
    covers.
    """

    answers: list[tuple[dict[str, Any], int]] = []
    responses: list[dict[str, Any]] = []
    first_error: TabError | None = None
    for body, sent in build_batches(tab, rows, limits, owners):
        allowed = {link for r in sent for v in r.cells.values() for link in URL_RE.findall(v)}
        try:
            answer = _call(body, allowed, gateway, denylist, responses)
        except TabError as error:
            if error.code == "client_term_in_input":
                raise
            first_error = first_error or error
            continue
        answers.append((answer, len(sent)))
    if not answers:
        raise first_error or TabError("invalid_output")
    # Merge in rounds: each merge call takes as many answers as fit, most important first, until
    # one answer is left. Answers that cannot share a call with any other are dropped from the end.
    while len(answers) > 1:
        merged_round: list[tuple[dict[str, Any], int]] = []
        i = 0
        while i < len(answers):
            merge_body, count = build_merge(tab, [a for a, _n in answers[i:]], limits, first=i == 0,
                                           owners=owners)
            if merge_body is None:
                merged_round.append(answers[i])
                i += 1
                continue
            group = answers[i:i + count]
            allowed = set().union(*(_answer_links(a) for a, _n in group))
            try:
                answer = _call(merge_body, allowed, gateway, denylist, responses)
            except TabError as error:
                # One more try for a malformed merge answer; a client term is never retried.
                if error.code not in RETRY_MERGE_CODES:
                    raise
                answer = _call(merge_body, allowed, gateway, denylist, responses)
            merged_round.append((answer, sum(n for _a, n in group)))
            i += count
        answers = merged_round if len(merged_round) < len(answers) else answers[:-1]
    answer, covered = answers[0]
    if len(responses) == 1:
        return answer, responses[0], covered
    combined = dict(responses[-1])
    combined["usage"] = {key: sum(int(((r.get("usage") or {}).get(key)) or 0) for r in responses)
                         for key in ("input_tokens", "output_tokens")}
    combined["estimated_cost_usd"] = sum(float(r.get("estimated_cost_usd") or 0) for r in responses)
    if any(r.get("status") == "budget_exceeded" for r in responses):
        combined["status"] = "budget_exceeded"
    return answer, combined, covered


def assign_owners(answer: dict[str, Any], tab: str, owners: Sequence[str], rows: list[Row]) -> dict[str, Any]:
    """Make every ``suggested_owner_role`` a real Chief tab name; the model sometimes invents a job
    title. Keep a listed Chief (the first one when it names several); else the Chief that requested
    the task's linked row; else the tab itself when it is a Chief tab; else "" (unassigned)."""

    known = set(owners)
    by_link: dict[str, list[str]] = {}
    for row in rows:
        chiefs = _chiefs(row.cells.get("requesting_chief", "") or row.cells.get("Source Chief", ""))
        for value in row.cells.values():
            for link in URL_RE.findall(value):
                by_link.setdefault(link, chiefs)
    for task in answer["candidate_tasks"]:
        candidates = _chiefs(task.get("suggested_owner_role", "")) + by_link.get(task.get("link") or "", []) + [tab]
        task["suggested_owner_role"] = next((c for c in candidates if c in known), "")
    return answer


def run(sheets: Sheets, gateway: Gateway, denylist: Sequence[str], *, now: datetime,
        dry_run: bool = False, only_tabs: Sequence[str] = ()) -> RunResult:
    denylist = tuple(normalize_text(term) for term in denylist if normalize_text(term))
    if not denylist:
        raise DenylistError("client denylist has no terms")
    titles = sheets.titles()
    tabs = [t for t in select_tabs(titles) if (not only_tabs or t in only_tabs) and not _contains_term(t, denylist)]
    owners = sorted(t for t in titles if t.startswith("Chief ") and not _contains_term(t, denylist))
    history = sheets.read([OUTPUT_TAB])[OUTPUT_TAB] if OUTPUT_TAB in titles else []
    marks = _watermarks(history)
    result = RunResult()

    def stop_all(remaining: Sequence[str], code: str) -> None:
        for tab in remaining:
            result.rows.append(_output_row(tab, now, "", 0, "error", error=code))
            _log(event="tab", tab=tab, status="error", error=code)
        result.exit_code = 1

    stopped = False
    limits = Limits(48000, 4096, 4096)
    try:
        status = gateway.status()
        decision = (status.get("budget") or {}).get("decision")
        if decision != "allow":
            stop_all(tabs, f"budget_{decision or 'missing'}")
            stopped = True
        limits = Limits.from_status(status)
    except GatewayStop as stop:
        stop_all(tabs, f"gateway_stop:{stop.code}")
        stopped = True

    if not stopped:
        values = sheets.read(tabs)
        for index, tab in enumerate(tabs):
            header, rows = parse_rows(values.get(tab, []))
            window_rows, mark = select_new_rows(tab, header, rows, marks.get(tab), now)
            last = max([1] + [r.number for r in rows])
            window = format_window(window_rows[0].number - 1 if window_rows else last, last, mark)
            # Check each row as it would be sent (header names included), not only the cell values.
            safe = [r for r in window_rows if not _contains_term(_row_line(r), denylist)]
            if not safe:
                result.rows.append(_output_row(tab, now, window, 0, "no_change"))
                _log(event="tab", tab=tab, status="no_change")
                continue
            started = time.monotonic()
            try:
                answer, response, sent = _summarize(tab, safe, limits, gateway, denylist, owners)
                answer = assign_owners(answer, tab, owners, safe)
            except GatewayStop as stop:
                stop_all(tabs[index:], f"gateway_stop:{stop.code}")
                break
            except TabError as error:
                result.rows.append(_output_row(tab, now, window, len(safe), "error", error=error.code))
                _log(event="tab", tab=tab, status="error", error=error.code)
                result.exit_code = 1
                continue
            row_status = "budget_exceeded" if response.get("status") == "budget_exceeded" else "ok"
            row = _output_row(tab, now, window, sent, row_status, answer=answer, response=response)
            result.rows.append(row)
            _log(event="tab", run_id=row[9], tab=tab, status=row_status, tokens_in=row[10], tokens_out=row[11],
                 cost_usd=row[12], latency_ms=round((time.monotonic() - started) * 1000))

    if dry_run:
        return result
    if result.rows:
        sheets.ensure_tab(OUTPUT_TAB, OUTPUT_HEADER)
        sheets.append(OUTPUT_TAB, result.rows)
        latest: dict[str, list[str]] = {}
        for raw in (history[1:] if history else []) + result.rows:
            if raw and raw[0]:
                latest[raw[0]] = raw
        sheets.ensure_tab(LATEST_TAB, OUTPUT_HEADER)
        sheets.replace(LATEST_TAB, [OUTPUT_HEADER] + [latest[t] for t in sorted(latest)])
    return result


# --------------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------------


def _service_account(raw: str) -> dict[str, Any]:
    """Key JSON inline or a path to it; errors never echo the value (it holds a private key)."""

    text = raw.strip().lstrip("\ufeff")
    try:
        if not text.startswith("{"):
            if len(text) > 1024 or not os.path.isfile(text):
                raise ValueError
            text = Path(text).read_text(encoding="utf-8")
        account = json.loads(text)
        if not isinstance(account, dict) or "client_email" not in account or "private_key" not in account:
            raise ValueError
        return account
    except (OSError, ValueError):
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid key JSON or a readable file") from None


def _post_heartbeat(ok: bool) -> None:
    token = os.environ.get("HEARTBEAT_TOKEN", "").strip()
    if not token:
        return
    try:
        httpx.post(HEARTBEAT_URL, timeout=15, json={"ok": ok, "failed": [] if ok else ["sheet-summaries"]},
                   headers={"Authorization": f"Bearer {token}", "User-Agent": "sheet-summaries/1"})
    except httpx.HTTPError:
        _log(event="heartbeat", status="error")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily Thai AI summaries for the Chief Findings Warehouse sheet.")
    parser.add_argument("--dry-run", action="store_true", help="print the would-be AI Summaries rows; write nothing")
    parser.add_argument("--fixture", help="read tabs from this JSON file ({tab: [[cells]]}) instead of Google")
    parser.add_argument("--fake-gateway", action="store_true", help="answer from the rows without calling the gateway")
    parser.add_argument("--tab", action="append", default=[], help="only this tab (repeatable)")
    args = parser.parse_args(argv)
    if args.fixture and not args.dry_run:
        parser.error("--fixture is for --dry-run only")

    denylist = load_denylist(os.environ.get("CLIENT_DENYLIST_PATH"))
    with httpx.Client() as google_http, httpx.Client(base_url=os.environ.get("AI_GATEWAY_URL", GATEWAY_URL)) as gw_http:
        sheets: Sheets
        if args.fixture:
            sheets = MemorySheets(json.loads(Path(args.fixture).read_text(encoding="utf-8")))
        else:
            account = _service_account(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
            sheets = GoogleSheets(os.environ.get("SHEET_SUMMARIES_SPREADSHEET_ID", SPREADSHEET_ID),
                                  google_access_token(account, google_http), google_http)
        gateway: Gateway = FakeGateway() if args.fake_gateway else GatewayClient(gw_http, os.environ["AI_GATEWAY_TOKEN"])
        result = run(sheets, gateway, denylist, now=datetime.now(timezone.utc), dry_run=args.dry_run,
                     only_tabs=args.tab)
    if args.dry_run:
        print(json.dumps({"status": "dry-run", "header": OUTPUT_HEADER, "rows": result.rows}, ensure_ascii=False, indent=2))
    else:
        _post_heartbeat(result.exit_code == 0)
    return result.exit_code


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from data_lake.observability import run_with_reporting

    raise SystemExit(run_with_reporting("sheet-summaries", main))
