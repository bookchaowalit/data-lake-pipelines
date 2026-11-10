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
# Phone numbers in free text become "[phone]" (Dataset.mask_phone). One pattern, valid both as a Python
# ``re`` and a PostgreSQL ARE, so ingest() (SQL) masks exactly what type_row (Python) masks: Thai
# mobiles (0[689]x-xxx-xxxx), Thai landlines (0[2-7]-xxx-xxxx) and "+<country> ..." numbers. ASCII
# digits only, and never inside a longer number (X ids, counts and dates are not touched).
PHONE_PATTERN = (r"(?<![0-9A-Za-z+])\+[0-9]{1,3}(?:[ .-]?[0-9]{2,4}){2,4}(?![0-9])"
                 r"|(?<![0-9])0[689][0-9][ .-]?[0-9]{3}[ .-]?[0-9]{4}(?![0-9])"
                 r"|(?<![0-9])0[2-7][ .-]?[0-9]{3}[ .-]?[0-9]{4}(?![0-9])")
PHONE_MASK = "[phone]"
_PHONE = re.compile(PHONE_PATTERN)


def mask_phones(text: str) -> str:
    return _PHONE.sub(PHONE_MASK, text)


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
            raw = raw_text(cells.get(name))
            errors[name] = {"raw": mask_phones(raw) if name in dataset.mask_phone else raw,
                            "reason": str(exc)}  # full value: the only copy
    for name in dataset.mask_phone:
        if isinstance(values.get(name), str):
            values[name] = mask_phones(values[name])
    for name in dataset.empty_ok:
        if values.get(name) is None and name not in errors:
            values[name] = ""  # a key part whose blank is a real value (x_bookmarks.folder_id)
    for name in dataset.required_columns:
        if values.get(name) is None and name not in errors:
            errors[name] = {"raw": "", "reason": "key column is blank"}
    # Unknown columns of a private dataset are counted, never kept: their text could be anything.
    extra = {k: ("[dropped]" if dataset.private else raw_text(v))
             for k, v in cells.items() if k and k not in dataset.kinds and raw_text(v)}
    return TypedRow(values, row_key(dataset, cells), sheet_row, errors, extra)


def dump_json(obj: dict[str, Any]) -> str | None:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True) if obj else None
