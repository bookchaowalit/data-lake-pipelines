"""Sheet cells -> typed values. A value that does not fit its column becomes null and is recorded in
the row's ``_coerce_errors`` JSON; the row itself is never dropped."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .schemas import SHEET_TZ, Dataset, Kind

TZ = ZoneInfo(SHEET_TZ)
_TS = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$")
_INT = re.compile(r"^[+-]?\d+$")
_SAFE_INT = 2**53


class CoerceError(ValueError):
    pass


def _int64(n: int) -> int:
    if not -(2**63) <= n < 2**63:
        raise CoerceError("outside int64")
    return n


def raw_text(value: Any) -> str:
    """The cell as text, the same way whatever the Sheet returned (used for row keys)."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, float) and value.is_integer() and abs(value) < _SAFE_INT:
        return str(int(value))
    return str(value).strip()


def coerce(value: Any, kind: Kind) -> Any:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    if kind == "str":
        if isinstance(value, (int, float)) and not isinstance(value, bool) and abs(value) >= _SAFE_INT:
            raise CoerceError("number beyond 2^53: the Sheet already lost digits, store the id as text")
        return raw_text(value)
    if kind == "int":
        if isinstance(value, bool):
            raise CoerceError("boolean in an integer column")
        if isinstance(value, int):
            return _int64(value)
        if isinstance(value, float):
            if value.is_integer():
                return _int64(int(value))
            raise CoerceError("not a whole number")
        text = str(value).strip().replace(",", "")
        if _INT.match(text):
            return _int64(int(text))
        raise CoerceError("not an integer")
    if kind == "float":
        if isinstance(value, bool):
            raise CoerceError("boolean in a number column")
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value).strip().replace(",", ""))
        except ValueError:
            raise CoerceError("not a number") from None
    if kind == "bool":
        if isinstance(value, bool):
            return value
        text = str(value).strip().upper()
        if text in ("TRUE", "FALSE"):
            return text == "TRUE"
        raise CoerceError("not TRUE/FALSE")
    if kind == "ts":
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=TZ).astimezone(timezone.utc)
        text = str(value).strip()
        m = _TS.match(text)
        if m:
            y, mo, d, h, mi, s = (int(g) if g else 0 for g in m.groups())
            try:
                return datetime(y, mo, d, h, mi, s, tzinfo=TZ).astimezone(timezone.utc)
            except ValueError:
                raise CoerceError("not a real calendar date/time") from None
        try:  # ISO 8601 with an explicit offset (future writers)
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            raise CoerceError("not YYYY-MM-DD HH:MM") from None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=TZ)
        return parsed.astimezone(timezone.utc)
    if kind == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        try:
            return date.fromisoformat(str(value).strip()[:10])
        except ValueError:
            raise CoerceError("not YYYY-MM-DD") from None
    raise AssertionError(kind)


def row_key(dataset: Dataset, cells: dict[str, Any]) -> str:
    raw = "\x1f".join(raw_text(cells.get(c)) for c in dataset.key)
    return hashlib.sha256(f"{dataset.name}\x1e{raw}".encode()).hexdigest()


@dataclass
class TypedRow:
    values: dict[str, Any]
    row_key: str
    sheet_row: int | None
    errors: dict[str, dict[str, str]] = field(default_factory=dict)
    extra: dict[str, str] = field(default_factory=dict)


def type_row(dataset: Dataset, cells: dict[str, Any], sheet_row: int | None = None) -> TypedRow:
    values: dict[str, Any] = {}
    errors: dict[str, dict[str, str]] = {}
    for name, kind in dataset.columns:
        try:
            values[name] = coerce(cells.get(name), kind)
        except CoerceError as exc:
            values[name] = None
            errors[name] = {"raw": raw_text(cells.get(name))[:200], "reason": str(exc)}
    for name in dataset.required_columns:
        if values.get(name) is None and name not in errors:
            errors[name] = {"raw": "", "reason": "key column is blank"}
    extra = {k: raw_text(v) for k, v in cells.items() if k and k not in dataset.kinds and raw_text(v)}
    return TypedRow(values, row_key(dataset, cells), sheet_row, errors, extra)


def dump_json(obj: dict[str, Any]) -> str | None:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True) if obj else None
