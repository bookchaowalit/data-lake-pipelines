"""Raw layer: append-only, immutable Parquet on R2 (or a local directory for tests).

Layout::

    <prefix>/raw/<dataset>/date=YYYY-MM-DD/part-NNNN.parquet     zstd, explicit Arrow schema
    <prefix>/_manifests/<dataset>/<batch_id>/date=D-part-NNNN.json   one entry per part, written
                                                                    before the part (rollback list)

Idempotent and resumable: before writing a partition, the writer reads the ``_row_key`` column of
the parts already there and writes only rows whose key is new, as the next part number. A part is
never overwritten or deleted; a rerun after a crash writes only what is missing.
One writer at a time (backfill and the daily append must not overlap).
"""

from __future__ import annotations

import io
import json
import os
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from .coerce import TZ, TypedRow, dump_json
from .config import Config
from .schemas import Dataset

PART = re.compile(r"part-(\d{4,})\.parquet$")
# Per-part manifest entry ``.../<batch_id>/date=YYYY-MM-DD-part-NNNN.json``: it reserves that part
# number even when its part was never published (crash between manifest and part).
MANIFEST_PART = re.compile(r"/date=(\d{4}-\d{2}-\d{2})-part-(\d{4,})\.json$")


class LakeError(RuntimeError):
    pass


class Store(Protocol):
    def list(self, prefix: str) -> list[str]: ...
    def read(self, key: str) -> bytes: ...
    def put_new(self, key: str, data: bytes) -> None: ...
    def delete(self, key: str) -> None: ...  # meta.py only (Meta retention); X raw parts are never deleted


class LocalStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def list(self, prefix: str) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        return sorted(str(p.relative_to(self.root)) for p in base.rglob("*") if p.is_file())

    def read(self, key: str) -> bytes:
        return (self.root / key).read_bytes()

    def put_new(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            raise LakeError(f"refusing to overwrite {key}")
        # Write a temp file, then publish it with a hard link: atomic, and it fails rather than
        # overwrite. A crash leaves only a .tmp file, never a truncated part under the final name.
        tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            with open(tmp, "xb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            try:
                os.link(tmp, path)
            except FileExistsError:
                raise LakeError(f"refusing to overwrite {key}") from None
        finally:
            tmp.unlink(missing_ok=True)

    def delete(self, key: str) -> None:
        (self.root / key).unlink()


class R2Store:
    """R2 through its S3 API (pyarrow's S3 filesystem; no extra dependency)."""

    def __init__(self, cfg: Config, bucket: str) -> None:
        from pyarrow import fs

        self.bucket = bucket
        self.fs = fs.S3FileSystem(
            access_key=cfg.require("R2_ACCESS_KEY_ID"),
            secret_key=cfg.require("R2_SECRET_ACCESS_KEY"),
            endpoint_override=f"https://{cfg.require('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
            region="auto",
        )

    def list(self, prefix: str) -> list[str]:
        from pyarrow import fs

        sel = fs.FileSelector(f"{self.bucket}/{prefix}", recursive=True, allow_not_found=True)
        n = len(self.bucket) + 1
        return sorted(i.path[n:] for i in self.fs.get_file_info(sel) if i.type == fs.FileType.File)

    def read(self, key: str) -> bytes:
        with self.fs.open_input_stream(f"{self.bucket}/{key}") as fh:
            return fh.read()

    def put_new(self, key: str, data: bytes) -> None:
        from pyarrow import fs

        path = f"{self.bucket}/{key}"
        if self.fs.get_file_info(path).type != fs.FileType.NotFound:
            raise LakeError(f"refusing to overwrite {key}")
        with self.fs.open_output_stream(path) as fh:
            fh.write(data)

    def delete(self, key: str) -> None:
        from pyarrow import fs

        path = f"{self.bucket}/{key}"
        if self.fs.get_file_info(path).type == fs.FileType.NotFound:
            raise FileNotFoundError(key)
        self.fs.delete_file(path)


def open_store(cfg: Config, uri: str | None = None) -> tuple[Store, str]:
    """(store, key prefix) for r2://bucket/prefix or file:///dir."""
    uri = uri or cfg.require("X_LAKE_URI")
    parts = urlsplit(uri)
    prefix = parts.path.strip("/")
    if parts.scheme == "r2":
        return R2Store(cfg, parts.netloc), prefix
    if parts.scheme == "file":
        return LocalStore(Path(parts.netloc + parts.path)), ""
    raise LakeError("X_LAKE_URI must be r2://<bucket>[/prefix] or file:///dir")


def _join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p)


def ingest_date(ts: Any, fallback: date) -> date:
    if isinstance(ts, datetime):
        return ts.astimezone(TZ).date()
    return fallback


def to_table(dataset: Dataset, rows: list[TypedRow], *, batch_id: str, source: str,
             ingested_at: datetime) -> pa.Table:
    schema = dataset.arrow_schema()
    cols: dict[str, list[Any]] = {name: [r.values.get(name) for r in rows] for name in dataset.names}
    cols["_row_key"] = [r.row_key for r in rows]
    cols["_batch_id"] = [batch_id] * len(rows)
    cols["_source"] = [source] * len(rows)
    cols["_sheet_row"] = [r.sheet_row for r in rows]
    cols["_ingested_at"] = [ingested_at] * len(rows)
    cols["_coerce_errors"] = [dump_json(r.errors) for r in rows]
    cols["_extra"] = [dump_json(r.extra) for r in rows]
    return pa.Table.from_pydict(cols, schema=schema)


def parquet_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd", compression_level=6, use_dictionary=True,
                   write_statistics=True)
    return buf.getvalue()


@dataclass
class PartitionPlan:
    dataset: str
    date: str
    incoming: int
    already_present: int = 0
    to_write: int = 0
    key: str = ""  # part key that was / would be written


@dataclass
class WriteReport:
    dataset: str
    batch_id: str
    partitions: list[PartitionPlan] = field(default_factory=list)

    @property
    def written(self) -> int:
        return sum(p.to_write for p in self.partitions)

    @property
    def skipped(self) -> int:
        return sum(p.already_present for p in self.partitions)


def write_raw(dataset: Dataset, rows: list[TypedRow], partition_ts: list[Any], *, store: Store | None,
              prefix: str, source: str, execute: bool, run_date: date,
              batch_id: str | None = None, now: datetime | None = None) -> WriteReport:
    """Plan (and with ``execute``) write one batch. ``partition_ts[i]`` dates rows[i].

    With ``store=None`` nothing is read or written: every row counts as new (offline dry run).
    """
    now = now or datetime.now(timezone.utc)
    batch_id = batch_id or f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    by_date: dict[str, list[TypedRow]] = defaultdict(list)
    seen: set[str] = set()
    undated: set[str] = set()
    for row, ts in zip(rows, partition_ts):
        if row.row_key in seen:  # same natural key twice in one batch: keep the first
            continue
        seen.add(row.row_key)
        if dataset.ingest_ts is None or not isinstance(ts, datetime):
            undated.add(row.row_key)
        by_date[ingest_date(ts, run_date).isoformat()].append(row)
    # A dataset without its own timestamp column (x_post_context) is dated by a lookup that can
    # change between runs (feed found / not found, run date), so its keys are checked against every
    # partition; so is any row whose timestamp is blank.
    known_elsewhere: set[str] = set()
    if undated and store is not None:
        for key in store.list(_join(prefix, "raw", dataset.name) + "/"):
            if PART.search(key):
                tbl = pq.read_table(io.BytesIO(store.read(key)), columns=["_row_key"])
                known_elsewhere.update(k for k in tbl.column("_row_key").to_pylist() if k in undated)

    reserved: dict[str, int] = {}  # day -> next part number not claimed by any manifest entry
    if store is not None:
        for key in store.list(_join(prefix, "_manifests", dataset.name) + "/"):
            if m := MANIFEST_PART.search(key):
                reserved[m.group(1)] = max(reserved.get(m.group(1), 0), int(m.group(2)) + 1)

    report = WriteReport(dataset.name, batch_id)
    for day in sorted(by_date):
        batch = by_date[day]
        part_dir = _join(prefix, "raw", dataset.name, f"date={day}")
        plan = PartitionPlan(dataset.name, day, len(batch))
        existing_keys: set[str] = set()
        next_part = 0
        if store is not None:
            for key in store.list(part_dir + "/"):
                m = PART.search(key)
                if not m:
                    continue
                next_part = max(next_part, int(m.group(1)) + 1)
                tbl = pq.read_table(io.BytesIO(store.read(key)), columns=["_row_key"])
                existing_keys.update(tbl.column("_row_key").to_pylist())
        next_part = max(next_part, reserved.get(day, 0))
        fresh = [r for r in batch if r.row_key not in existing_keys and r.row_key not in known_elsewhere]
        plan.already_present = len(batch) - len(fresh)
        plan.to_write = len(fresh)
        if fresh:
            plan.key = _join(part_dir, f"part-{next_part:04d}.parquet")
            if execute:
                if store is None:
                    raise LakeError("execute needs a store")
                table = to_table(dataset, fresh, batch_id=batch_id, source=source, ingested_at=now)
                # Intent first: the manifest entry exists before the part, so a crash can never leave
                # a part that no manifest lists (an entry whose part is missing is harmless).
                entry = {"dataset": dataset.name, "batch_id": batch_id, "source": source,
                         "written_at": now.isoformat(), "contract": f"{dataset.name}.v1",
                         "key": plan.key, "rows": len(fresh), "date": day}
                store.put_new(_join(prefix, "_manifests", dataset.name, batch_id,
                                    f"date={day}-part-{next_part:04d}.json"),
                              json.dumps(entry, indent=1).encode())
                store.put_new(plan.key, parquet_bytes(table))
        report.partitions.append(plan)
    return report


def _part_date(key: str) -> date | None:
    m = re.search(r"date=(\d{4}-\d{2}-\d{2})/", key)
    return date.fromisoformat(m.group(1)) if m else None


def read_dataset(store: Store, prefix: str, dataset: Dataset, since: date | None = None) -> pa.Table | None:
    """Committed parts of a raw dataset, optionally only partitions dated ``since`` or later.

    Adds ``_partition_date`` (the part's ``date=`` value) so callers can scope by where a row is stored.
    """
    keys = [k for k in store.list(_join(prefix, "raw", dataset.name) + "/") if PART.search(k)]
    if since is not None:
        keys = [k for k in keys if (d := _part_date(k)) is not None and d >= since]
    if not keys:
        return None
    tables = []
    for k in keys:
        t = pq.read_table(io.BytesIO(store.read(k)))
        tables.append(t.append_column("_partition_date", pa.array([_part_date(k)] * t.num_rows, pa.date32())))
    return pa.concat_tables(tables)


def lookup_latest(store: Store, prefix: str, dataset: Dataset, column: str, values: set[str],
                  days: set[date], lookback: int = 7) -> dict[str, dict[str, Any]]:
    """Latest raw row per ``column`` value, searching partitions ``day - lookback .. day``.

    Used by appends whose X Signals arrive in a later call than their X Feed rows.
    """
    if not values or not days:
        return {}
    wanted = {(d - timedelta(days=i)).isoformat() for d in days for i in range(lookback + 1)}
    out: dict[str, dict[str, Any]] = {}
    for key in store.list(_join(prefix, "raw", dataset.name) + "/"):
        m = re.search(r"date=(\d{4}-\d{2}-\d{2})/", key)
        if not PART.search(key) or not m or m.group(1) not in wanted:
            continue
        tbl = pq.read_table(io.BytesIO(store.read(key)), columns=dataset.names)
        for rec in tbl.to_pylist():
            v = rec.get(column)
            if v in values:
                ts = rec.get(dataset.ingest_ts) if dataset.ingest_ts else None
                prev = out.get(v)
                if prev is None or (ts is not None and (prev.get(dataset.ingest_ts) or ts) <= ts):
                    out[v] = rec
    return out
