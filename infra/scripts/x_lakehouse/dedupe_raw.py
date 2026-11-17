"""One-off repair: drop resent copies of x_home_timeline rows from raw (backup first).

Until 2026-11-17 Chief Data stamped ``collected_at`` with the send time instead of the fetch time, so
one fetch sent in two or three runs landed as two or three raw rows with different keys. A copy is a
row whose ``post_id`` and engagement counts (likes, reposts, replies, quotes, views) equal another
row's; the copy with the earliest ``collected_at`` is kept. A real re-pull whose counts moved is kept.

The raw layer is append-only, so this is the one sanctioned rewrite, and it runs in this order:

1. take the drainer's advisory lock (no cron drain can write a part meanwhile);
2. refuse when a row to drop was drained less than 25 h ago (the run's validate step would count it
   as missing from raw) or when an earlier repair is half done;
3. copy every part of each partition that changes to ``<prefix>/private/_backup/<run>/raw/...`` and
   read each copy back byte for byte;
4. per partition: manifest entry, one new part holding the kept rows, read back, then delete the
   old parts.

Dry run by default (counts only, no row content); ``--apply`` writes. Restore = copy the backup parts
back and delete the new part named in ``<backup>/plan.json``.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .lake import PART, LakeError, Store, _join, dataset_root, parquet_bytes, raw_dir
from .schemas import DATASETS

DATASET = "x_home_timeline"
SAME_FETCH = ("post_id", "likes", "reposts", "replies", "quotes", "views")
BATCH_PREFIX = "dedupe-"
VALIDATE_WINDOW = timedelta(hours=25)  # validate checks rows drained in the last 24 h


def _parts(store: Store, prefix: str) -> dict[str, list[str]]:
    by_day: dict[str, list[str]] = defaultdict(list)
    for key in store.list(raw_dir(prefix, DATASETS[DATASET]) + "/"):
        if PART.search(key):
            by_day[key.rsplit("/", 2)[-2].removeprefix("date=")].append(key)
    return dict(by_day)


def plan(store: Store, prefix: str) -> dict[str, Any]:
    """Which rows go, per partition. Reads every part; row content never leaves this function."""
    parts = _parts(store, prefix)
    tables: dict[str, pa.Table] = {}
    for key in (k for keys in parts.values() for k in keys):
        tables[key] = pq.read_table(io.BytesIO(store.read(key)))
    # A repair is half done while a part its plan.json says it replaces still exists next to its new part.
    present = set(tables)
    half_done: list[str] = []
    root = dataset_root(prefix, DATASETS[DATASET])
    for key in store.list(_join(root, "_backup") + "/"):
        if key.endswith("/plan.json"):
            old = json.loads(store.read(key))
            for day, d in old.get("days", {}).items():
                if old["new_parts"].get(day) in present and present & set(d["parts"]):
                    half_done.append(day)
    # Earliest collected_at per fetch, across every partition (a fetch sent after midnight lands a day later).
    best: dict[tuple[Any, ...], tuple[Any, str]] = {}
    rows: list[
        tuple[str, str, tuple[Any, ...], Any]
    ] = []  # (part key, row key, fetch, collected_at)
    for key, t in tables.items():
        cols = {
            c: t.column(c).to_pylist()
            for c in (*SAME_FETCH, "collected_at", "_row_key")
        }
        for i, rk in enumerate(cols["_row_key"]):
            fetch = tuple(cols[c][i] for c in SAME_FETCH)
            at = cols["collected_at"][i]
            rows.append((key, rk, fetch, at))
            cur = best.get(fetch)
            if cur is None or (at, rk) < cur:
                best[fetch] = (at, rk)
    keep = {rk for _, rk in best.values()}
    drop: dict[str, set[str]] = defaultdict(set)  # part key -> row keys to drop
    for key, rk, _, _ in rows:
        if rk not in keep:
            drop[key].add(rk)
    days = {}
    for day, keys in sorted(parts.items()):
        gone = sum(len(drop.get(k, ())) for k in keys)
        total = sum(tables[k].num_rows for k in keys)
        days[day] = {
            "parts": sorted(keys),
            "rows": total,
            "drop": gone,
            "keep": total - gone,
        }
    return {
        "dataset": DATASET,
        "days": days,
        "half_done": half_done,
        "rows": sum(d["rows"] for d in days.values()),
        "drop": sum(d["drop"] for d in days.values()),
        "_drop_keys": {k: sorted(v) for k, v in drop.items()},
        "_tables": tables,
    }


def recently_drained(conn: Any, row_keys: set[str], now: datetime) -> int:
    """Row keys to drop that the private inbox drained inside the validate window."""
    from .private import P_INBOX

    if not row_keys:
        return 0
    with conn.cursor() as c:
        # identifiers are module constants; values are bound parameters
        # nosemgrep: python.sqlalchemy.security.sqlalchemy-execute-raw-query.sqlalchemy-execute-raw-query
        c.execute(
            f"select count(*) from {P_INBOX} where dataset = %s and drained_at >= %s and row_key = any(%s)",
            (DATASET, now - VALIDATE_WINDOW, list(row_keys)),
        )
        n = int(c.fetchone()[0])
    conn.commit()
    return n


def apply(
    store: Store, prefix: str, p: dict[str, Any], *, now: datetime
) -> dict[str, Any]:
    """Backup, then rewrite each partition that loses rows. ``p`` is a fresh :func:`plan`."""
    if p["half_done"]:
        raise LakeError(
            f"an earlier repair is half done ({len(p['half_done'])} parts); restore or finish it first"
        )
    run = f"{BATCH_PREFIX}{now:%Y%m%dT%H%M%SZ}"
    root = dataset_root(prefix, DATASETS[DATASET])
    backup = _join(root, "_backup", run)
    tables: dict[str, pa.Table] = p["_tables"]
    changed = {day: d for day, d in p["days"].items() if d["drop"]}
    # 1. Backup every part of every partition that changes, and prove each copy.
    for d in changed.values():
        for key in d["parts"]:
            data = store.read(key)
            copy = _join(backup, key[len(root) :])
            store.put_new(copy, data)
            if store.read(copy) != data:
                raise LakeError(f"backup copy differs: {copy}")
    new_parts: dict[str, str] = {}
    for day, d in changed.items():
        part_dir = _join(raw_dir(prefix, DATASETS[DATASET]), f"date={day}")
        nxt = 1 + max(int(PART.search(k).group(1)) for k in d["parts"])  # type: ignore[union-attr]
        new_parts[day] = _join(part_dir, f"part-{nxt:04d}.parquet")
    store.put_new(
        _join(backup, "plan.json"),
        json.dumps(
            {
                "run": run,
                "dataset": DATASET,
                "same_fetch": SAME_FETCH,
                "new_parts": new_parts,
                "days": {
                    day: {k: v for k, v in d.items()} for day, d in changed.items()
                },
            },
            indent=1,
        ).encode(),
    )
    # 2. Per partition: manifest entry, new part, read back, then delete the old parts.
    for day, d in changed.items():
        drop = set().union(*(p["_drop_keys"].get(k, []) for k in d["parts"]))
        kept = pa.concat_tables(
            [tables[k] for k in d["parts"]], promote_options="default"
        )
        kept = kept.filter(
            pc.invert(
                pc.is_in(kept.column("_row_key"), value_set=pa.array(sorted(drop)))
            )
        )
        if kept.num_rows != d["keep"]:
            raise LakeError(f"{day}: kept {kept.num_rows} rows, planned {d['keep']}")
        # Rows keep their own _batch_id/_ingested_at (provenance); the manifest names this repair.
        kept = kept.cast(DATASETS[DATASET].arrow_schema())
        key = new_parts[day]
        entry = {
            "dataset": DATASET,
            "batch_id": run,
            "source": "dedupe",
            "written_at": now.isoformat(),
            "contract": f"{DATASET}.v1",
            "key": key,
            "rows": kept.num_rows,
            "date": day,
            "replaces": d["parts"],
            "backup": backup,
        }
        store.put_new(
            _join(
                root,
                "_manifests",
                DATASET,
                run,
                f"date={day}-{key.rsplit('/', 1)[-1][:-8]}.json",
            ),
            json.dumps(entry, indent=1).encode(),
        )
        store.put_new(key, parquet_bytes(kept))
        if pq.read_table(io.BytesIO(store.read(key))).num_rows != d["keep"]:
            raise LakeError(
                f"{day}: new part read back with the wrong row count; old parts left in place"
            )
        for old in d["parts"]:
            store.delete(old)
    return {
        "run": run,
        "backup": backup,
        "partitions": len(changed),
        "dropped": p["drop"],
    }


def summary(p: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset": p["dataset"],
        "rows": p["rows"],
        "drop": p["drop"],
        "half_done": len(p["half_done"]),
        "days": {
            day: {k: d[k] for k in ("rows", "drop", "keep")}
            | {"parts": len(d["parts"])}
            for day, d in p["days"].items()
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="write the backup and the rewritten partitions",
    )
    args = ap.parse_args(argv)
    from . import inbox
    from . import curated as cur
    from .config import Config
    from .lake import open_store

    cfg = Config()
    now = datetime.now(timezone.utc)
    store, prefix = open_store(cfg)
    conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
    try:
        if not inbox.try_lock(conn):
            print(
                json.dumps(
                    {
                        "status": "locked",
                        "note": "a drain is running; try again after it",
                    }
                )
            )
            return 1
        p = plan(store, prefix)
        drop_keys = set().union(*p["_drop_keys"].values()) if p["_drop_keys"] else set()
        recent = recently_drained(conn, drop_keys, now)
        out: dict[str, Any] = summary(p) | {"recently_drained": recent}
        if args.apply:
            if recent:
                out["status"] = "refused"
                out["note"] = (
                    f"{recent} rows to drop were drained in the last {VALIDATE_WINDOW}; rerun later"
                )
                print(json.dumps(out, indent=1))
                return 1
            out |= apply(store, prefix, p, now=now)
            out["status"] = "applied"
        else:
            out["status"] = "dry_run"
        print(json.dumps(out, indent=1))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
