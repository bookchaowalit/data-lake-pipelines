"""Sanity checks on what the bots sent, run by the cron after each drain (warn only: rows are kept).

The inbox accepts any row with the right columns. Two bot faults went unseen for days that way (spec
docs/specs/x-intake-checks.md): a UTC+9 machine clock that stamped x_feed rows 2 h late (from 2026-11-17),
and a re-send that overwrote its own queue so a morning run delivered 84 rows instead of 464 (2026-11-24).
These checks read the inbox rows received recently and report, as fixed-vocabulary lines for the cron's
Telegram ops alert:

* future   - a row whose ``collected_at`` is later than the moment the inbox received it (plus a tolerance):
             the sender's clock is wrong. One line per (run_id, dataset).
* missing  - an expected bot run (a daily ICT window of inbox arrival) brought fewer x_feed rows than the
             floor, or none. Checked only for windows that closed since the previous cron run (lookback), so
             each window is reported once.
* naive    - ``collected_at`` written without a zone, read as ICT. Logged as counts only (every x_feed row
             is naive today; an alert each run would be noise), so a change shows in the log.

Settings (all optional): X_LAKE_INTAKE_CHECKS=off, X_LAKE_INTAKE_WINDOWS ("06:00-08:15,18:00-20:30", ICT),
X_LAKE_INTAKE_MIN_ROWS (100), X_LAKE_INTAKE_LOOKBACK_HOURS (6), X_LAKE_INTAKE_FUTURE_MINUTES (5).
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Any, Iterable

from .coerce import TZ, coerce

STAMPED = ("x_feed", "x_news", "x_post_engagers", "x_spaces")  # raw datasets whose rows carry collected_at
VOLUME_DATASET = "x_feed"
_ZONE = re.compile(r"(Z|[+-]\d{2}:?\d{2})$", re.IGNORECASE)


@dataclass(frozen=True)
class Settings:
    windows: tuple[tuple[time, time], ...] = ((time(6, 0), time(8, 15)), (time(18, 0), time(20, 30)))
    min_rows: int = 100
    lookback: timedelta = timedelta(hours=6)
    future: timedelta = timedelta(minutes=5)

    @classmethod
    def from_cfg(cls, cfg: Any) -> "Settings":
        d = cls()
        raw = cfg.get("X_LAKE_INTAKE_WINDOWS", "").strip()
        windows = d.windows
        if raw:
            windows = tuple(
                (time.fromisoformat(a.strip()), time.fromisoformat(b.strip()))
                for a, b in (part.split("-", 1) for part in raw.split(",") if part.strip())
            )
        return cls(
            windows=windows,
            min_rows=int(cfg.get("X_LAKE_INTAKE_MIN_ROWS", "") or d.min_rows),
            lookback=timedelta(hours=float(cfg.get("X_LAKE_INTAKE_LOOKBACK_HOURS", "") or 6)),
            future=timedelta(minutes=float(cfg.get("X_LAKE_INTAKE_FUTURE_MINUTES", "") or 5)),
        )


def naive(stamp: Any) -> bool:
    return isinstance(stamp, str) and bool(stamp.strip()) and not _ZONE.search(stamp.strip())


def closed_windows(now: datetime, s: Settings) -> list[tuple[datetime, datetime]]:
    """Expected-run windows (ICT) that closed in (now - lookback, now]."""
    local = now.astimezone(TZ)
    out = []
    for day in (local.date() - timedelta(days=1), local.date()):
        for a, b in s.windows:
            start, end = datetime.combine(day, a, TZ), datetime.combine(day, b, TZ)
            if now - s.lookback < end <= now:
                out.append((start, end))
    return sorted(out)


def evaluate(rows: Iterable[tuple[str, str, Any, datetime]], now: datetime, s: Settings = Settings()) -> dict[str, Any]:
    """``rows``: (run_id, dataset, collected_at as sent, received_at) for recent inbox rows. Stamps are
    checked only for rows received in the look-back (so one bad run is reported once); arrivals count for
    every closed window. Returns issues (alert lines) and counts (log only)."""
    future: dict[tuple[str, str], list[float]] = defaultdict(list)
    naive_n: dict[str, int] = defaultdict(int)
    arrivals: list[datetime] = []
    for run_id, dataset, sent, received in rows:
        if dataset == VOLUME_DATASET:
            arrivals.append(received)
        if dataset not in STAMPED or sent in (None, "") or received <= now - s.lookback:
            continue
        if naive(sent):
            naive_n[dataset] += 1
        at = coerce(sent, "ts")
        if isinstance(at, datetime) and at > received + s.future:
            future[(run_id, dataset)].append((at - received).total_seconds() / 60)
    issues = [
        f"intake {run} {ds}: {len(leads)} rows stamped up to {round(max(leads))} min after arrival (sender clock?)"
        for (run, ds), leads in sorted(future.items())
    ]
    windows = []
    for start, end in closed_windows(now, s):
        n = sum(1 for at in arrivals if start <= at < end)
        windows.append({"window": f"{start:%Y-%m-%d %H:%M}-{end:%H:%M}", "x_feed_rows": n})
        if n < s.min_rows:
            what = "no x_feed run" if n == 0 else f"x_feed {n} rows (< {s.min_rows})"
            issues.append(f"intake {start:%Y-%m-%d %H:%M}-{end:%H:%M}: {what}")
    return {"issues": issues, "future_rows": sum(len(v) for v in future.values()), "naive": dict(naive_n),
            "windows": windows}


def check(conn: Any, now: datetime, s: Settings = Settings(), inbox: str | None = None) -> dict[str, Any]:
    """Evaluates the inbox rows received in the last day (covers every window that can be closed now)."""
    from .inbox import INBOX

    with conn.cursor() as c:
        c.execute(f"select run_id, dataset, row->>'collected_at', received_at from {inbox or INBOX} "
                  "where received_at >= %s and received_at <= %s", (now - timedelta(days=1), now))
        return evaluate(c.fetchall(), now, s)
