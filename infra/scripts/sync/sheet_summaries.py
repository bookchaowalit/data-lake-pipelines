#!/usr/bin/env python3
"""Daily Thai AI summaries for the "Chief Findings Warehouse" Google Sheet.

Spec: docs/specs/sheet-summaries.md. Operator docs: docs/sheet-summaries.md.

Reads each Chief/data tab, sends only rows added since the last run (first run: last 7 days) to the
owner's AI gateway (``solo-empire.ai-gateway.v1``, route ``sheet-summaries``, profile ``fast`` =
GPT-6 Luna), validates the JSON answer (schema + no invented links + no client terms) and appends
one row per tab to ``AI Summaries``; ``AI Summaries Latest`` holds the newest row per tab. Source
tabs are never written. Rows that match the client denylist never leave the host, and the run fails
closed without a denylist. Logs carry run id, tab, status, tokens, cost and latency only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import unicodedata
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote, urlsplit

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
MAX_ROWS = 200
MAX_CELL_CHARS = 500
FIRST_RUN_DAYS = 7
MAX_OUTPUT_TOKENS = 4096
# GPT-6 Luna reasons before it answers; at the default effort it spent a 2048-token cap on
# reasoning and returned no content (finish_reason=length). "low" keeps the answer within the cap.
REASONING_EFFORT = "low"
# The gateway appends its Thai language policy (~650 bytes UTF-8) to the system message.
GATEWAY_OVERHEAD_BYTES = 768
DATE_COLUMNS = ("collected_at", "posted_at", "found_at", "date", "created_at", "updated_at", "timestamp")
SUCCESS_STATUSES = frozenset({"ok", "budget_exceeded", "no_change"})
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


def _is_true(value: str) -> bool:
    return value.strip().lower() in TRUE_VALUES


def _score(value: str) -> float:
    try:
        return float(value)
    except ValueError:
        return 0.0


def _priority(tab: str, rows: list[Row]) -> list[Row]:
    if tab == "X Feed":
        return sorted(rows, key=lambda r: (_is_true(r.cells.get("top", "")), _score(r.cells.get("score", "")), r.number),
                      reverse=True)
    return sorted(rows, key=lambda r: r.number, reverse=True)


def _row_line(row: Row) -> str:
    cells = {k: (v if len(v) <= MAX_CELL_CHARS else v[: MAX_CELL_CHARS - 1] + "…") for k, v in row.cells.items()}
    return json.dumps({"row": row.number, **cells}, ensure_ascii=False)


def _user_message(tab: str, lines: list[str]) -> str:
    return f"Tab: {tab}\nNew rows ({len(lines)}), one JSON object per line:\n" + "\n".join(lines)


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


def build_request(tab: str, rows: list[Row], limits: Limits) -> tuple[dict[str, Any], list[Row]]:
    """Fit the highest-priority rows into the gateway limits; returns the body and the rows sent."""

    output = limits.output_tokens
    input_room = limits.max_token_budget - output - GATEWAY_OVERHEAD_BYTES
    char_room = limits.max_context_chars - GATEWAY_OVERHEAD_BYTES
    system_cost = _bytes(SYSTEM_PROMPT)
    chosen: list[Row] = []
    used_bytes = system_cost + _bytes(_user_message(tab, []))
    used_chars = len(SYSTEM_PROMPT) + len(_user_message(tab, []))
    for row in _priority(tab, rows)[:MAX_ROWS]:
        line = _row_line(row)
        cost = len(line.encode("utf-8")) + 1
        if used_bytes + cost > input_room or used_chars + len(line) + 1 > char_room:
            continue
        chosen.append(row)
        used_bytes += cost
        used_chars += len(line) + 1
    if not chosen:
        raise TabError("row_too_large")
    chosen.sort(key=lambda r: r.number)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": _user_message(tab, [_row_line(r) for r in chosen])},
    ]
    estimated = sum(_bytes(m["content"]) for m in messages)
    body = {
        "route": ROUTE,
        "prompt_version": PROMPT_VERSION,
        "profile": "fast",
        "privacy_class": "internal",
        "response_format": {"type": "json_object"},
        "reasoning_effort": REASONING_EFFORT,
        "max_output_tokens": output,
        "token_budget": min(limits.max_token_budget, max(256, estimated + output + GATEWAY_OVERHEAD_BYTES)),
        "messages": messages,
    }
    return body, chosen


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


def _watermarks(history: list[list[str]]) -> dict[str, int]:
    marks: dict[str, int] = {}
    if not history:
        return marks
    header = history[0]
    for raw in history[1:]:
        record = dict(zip(header, raw))
        if record.get("status") not in SUCCESS_STATUSES:
            continue
        match = re.search(r"to_row=(\d+)", record.get("window", ""))
        if match and record.get("tab"):
            marks[record["tab"]] = int(match.group(1))  # later rows win
    return marks


def _window_rows(header: list[str], rows: list[Row], watermark: int | None, now: datetime) -> tuple[int, list[Row]]:
    last = rows[-1].number if rows else 1
    if watermark is not None and watermark <= last:
        return watermark, [r for r in rows if r.number > watermark]
    column = _date_column(header)
    if column is None:
        return 1, rows[-MAX_ROWS:]
    since = now - timedelta(days=FIRST_RUN_DAYS)
    recent = []
    for row in rows:
        stamp = _row_date(row.cells.get(column, ""))
        if stamp is None or stamp >= since.replace(hour=0, minute=0, second=0, microsecond=0):
            recent.append(row)
    return 1, recent


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


def run(sheets: Sheets, gateway: Gateway, denylist: Sequence[str], *, now: datetime,
        dry_run: bool = False, only_tabs: Sequence[str] = ()) -> RunResult:
    denylist = tuple(normalize_text(term) for term in denylist if normalize_text(term))
    if not denylist:
        raise DenylistError("client denylist has no terms")
    titles = sheets.titles()
    tabs = [t for t in select_tabs(titles) if (not only_tabs or t in only_tabs) and not _contains_term(t, denylist)]
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
            after, window_rows = _window_rows(header, rows, marks.get(tab), now)
            last = max([after] + [r.number for r in rows])
            window = f"after_row={after};to_row={last}"
            # Check each row as it would be sent (header names included), not only the cell values.
            safe = [r for r in window_rows if not _contains_term(_row_line(r), denylist)]
            if not safe:
                result.rows.append(_output_row(tab, now, f"after_row={last};to_row={last}" if after == last else window,
                                               0, "no_change"))
                _log(event="tab", tab=tab, status="no_change")
                continue
            started = time.monotonic()
            try:
                body, sent = build_request(tab, safe, limits)
                if any(_contains_term(m["content"], denylist) for m in body["messages"]):
                    raise TabError("client_term_in_input")
                allowed = {link for r in sent for v in r.cells.values() for link in URL_RE.findall(v)}
                response = gateway.generate(body)
                if response.get("finish_reason") == "length":
                    raise TabError("output_truncated")
                answer = validate_answer(str(response.get("content", "")), allowed, denylist)
            except GatewayStop as stop:
                stop_all(tabs[index:], f"gateway_stop:{stop.code}")
                break
            except TabError as error:
                result.rows.append(_output_row(tab, now, window, len(safe), "error", error=error.code))
                _log(event="tab", tab=tab, status="error", error=error.code)
                result.exit_code = 1
                continue
            row_status = "budget_exceeded" if response.get("status") == "budget_exceeded" else "ok"
            row = _output_row(tab, now, window, len(sent), row_status, answer=answer, response=response)
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
