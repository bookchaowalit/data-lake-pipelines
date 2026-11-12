"""Google Sheets access. Reading uses the read-only scope; only serve_sheet.py asks for write access,
and every write goes through :func:`assert_writable` first."""

from __future__ import annotations

import json
import time
import urllib.parse
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import httpx

from .config import Config
from .schemas import SERVE_TABS, is_protected

API = "https://sheets.googleapis.com/v4/spreadsheets"
READ_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"
WRITE_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


class SheetError(RuntimeError):
    pass


def access_token(service_account_json: str, scope: str, http: httpx.Client) -> str:
    from google.auth import crypt, jwt  # extra "sheets"

    info = json.loads(service_account_json)
    now = int(time.time())
    assertion = jwt.encode(
        crypt.RSASigner.from_service_account_info(info),
        {"iss": info["client_email"], "scope": scope, "aud": "https://oauth2.googleapis.com/token",
         "iat": now, "exp": now + 600},
    )
    if isinstance(assertion, bytes):
        assertion = assertion.decode()
    r = http.post("https://oauth2.googleapis.com/token",
                  data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion})
    body = r.json()
    if "access_token" not in body:
        raise SheetError(f"Google token exchange failed: {body.get('error')}")  # never echo the key
    return str(body["access_token"])


def _a1(tab: str) -> str:
    return "'" + tab.replace("'", "''") + "'"


@dataclass
class TabData:
    tab: str
    header: list[str]
    rows: list[tuple[int, dict[str, Any]]]  # (1-based sheet row, {header: cell})


class SheetReader:
    def __init__(self, cfg: Config, http: httpx.Client) -> None:
        self.cfg = cfg
        self.http = http
        self._token = access_token(cfg.require("GOOGLE_SERVICE_ACCOUNT_JSON"), READ_SCOPE, http)

    def _get(self, url: str, params: Any = None) -> dict[str, Any]:
        r = self.http.get(url, params=params, headers={"Authorization": f"Bearer {self._token}"})
        if r.status_code >= 400:
            raise SheetError(f"Sheets API HTTP {r.status_code}")
        return r.json()

    def titles(self) -> list[str]:
        meta = self._get(f"{API}/{self.cfg.sheet_id}", {"fields": "sheets.properties.title"})
        return [s["properties"]["title"] for s in meta["sheets"]]

    def read(self, tabs: Sequence[str]) -> dict[str, TabData]:
        params: list[tuple[str, str]] = [("ranges", _a1(t)) for t in tabs]
        params += [("valueRenderOption", "UNFORMATTED_VALUE"), ("dateTimeRenderOption", "FORMATTED_STRING")]
        body = self._get(f"{API}/{self.cfg.sheet_id}/values:batchGet", params)
        out: dict[str, TabData] = {}
        for tab, vr in zip(tabs, body.get("valueRanges", [])):
            values = vr.get("values", [])
            header = [str(h).strip() for h in values[0]] if values else []
            rows = []
            for i, row in enumerate(values[1:], start=2):
                if not any(str(c).strip() for c in row):
                    continue  # fully blank line
                rows.append((i, {h: (row[j] if j < len(row) else "") for j, h in enumerate(header) if h}))
            out[tab] = TabData(tab, header, rows)
        return out


WRITABLE_TABS = frozenset(SERVE_TABS.values())


def assert_writable(tab: str) -> None:
    """The only tabs this package may write are its own serving tabs."""
    if is_protected(tab) or tab not in WRITABLE_TABS:
        raise SheetError(f"refusing to write tab {tab!r}: only {sorted(WRITABLE_TABS)} are writable")


class SheetWriter:
    """Writes the serving tabs only. Row 1 (the header) is written once when the tab is created and
    never again; refreshes clear and rewrite rows 2+."""

    def __init__(self, cfg: Config, http: httpx.Client) -> None:
        self.cfg = cfg
        self.http = http
        self._token = access_token(cfg.require("GOOGLE_SERVICE_ACCOUNT_JSON"), WRITE_SCOPE, http)

    def _call(self, method: str, url: str, **kw: Any) -> dict[str, Any]:
        r = self.http.request(method, url, headers={"Authorization": f"Bearer {self._token}"}, **kw)
        if r.status_code >= 400:
            raise SheetError(f"Sheets API HTTP {r.status_code}")
        return r.json() if r.content else {}

    def titles(self) -> list[str]:
        meta = self._call("GET", f"{API}/{self.cfg.sheet_id}", params={"fields": "sheets.properties.title"})
        return [s["properties"]["title"] for s in meta["sheets"]]

    def header(self, tab: str) -> list[str]:
        body = self._call("GET", f"{API}/{self.cfg.sheet_id}/values/{urllib.parse.quote(_a1(tab) + '!1:1')}")
        values = body.get("values", [])
        return [str(h) for h in values[0]] if values else []

    def create_tab(self, tab: str, header: list[str]) -> None:
        assert_writable(tab)
        self._call("POST", f"{API}/{self.cfg.sheet_id}:batchUpdate",
                   json={"requests": [{"addSheet": {"properties": {"title": tab}}}]})
        self._call("PUT", f"{API}/{self.cfg.sheet_id}/values/{urllib.parse.quote(_a1(tab) + '!A1')}",
                   params={"valueInputOption": "RAW"}, json={"values": [header]})

    def _grid(self, tab: str) -> tuple[int, int, int]:
        meta = self._call("GET", f"{API}/{self.cfg.sheet_id}",
                          params={"fields": "sheets.properties(sheetId,title,gridProperties)"})
        for sh in meta["sheets"]:
            props = sh["properties"]
            if props["title"] == tab:
                g = props.get("gridProperties", {})
                return int(props["sheetId"]), int(g.get("rowCount", 0)), int(g.get("columnCount", 0))
        raise SheetError(f"tab {tab!r} not found")

    def replace_body(self, tab: str, rows: list[list[Any]]) -> None:
        """Grow the grid if needed, write rows 2.. first, then clear only the old rows left below.

        Writing before clearing means a failed write leaves the previous view in place.
        """
        assert_writable(tab)
        width = max((len(r) for r in rows), default=0)
        sheet_id, n_rows, n_cols = self._grid(tab)
        need_rows, need_cols = len(rows) + 1, max(width, n_cols)
        if need_rows > n_rows or need_cols > n_cols:
            self._call("POST", f"{API}/{self.cfg.sheet_id}:batchUpdate", json={"requests": [{
                "updateSheetProperties": {
                    "properties": {"sheetId": sheet_id,
                                   "gridProperties": {"rowCount": max(need_rows, n_rows), "columnCount": need_cols}},
                    "fields": "gridProperties.rowCount,gridProperties.columnCount"}}]})
            n_rows = max(need_rows, n_rows)
        if rows:
            self._call("PUT", f"{API}/{self.cfg.sheet_id}/values/{urllib.parse.quote(_a1(tab) + '!A2')}",
                       params={"valueInputOption": "RAW"}, json={"values": rows})
        if n_rows > need_rows:
            stale = _a1(tab) + f"!A{need_rows + 1}:ZZ"
            self._call("POST", f"{API}/{self.cfg.sheet_id}/values/{urllib.parse.quote(stale)}:clear")
