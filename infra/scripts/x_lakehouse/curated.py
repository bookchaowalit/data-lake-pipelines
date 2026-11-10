"""Curated layer in Postgres (Supabase): deduped, joined, upserted. Never plain INSERT.

Each upsert loads the batch into a temp table with COPY, then
``INSERT ... SELECT ... ON CONFLICT (pk) DO UPDATE ... WHERE <row changed>`` so reruns are no-ops
and ``updated_at`` moves only when a value really changed. Tables with a ``version`` column
(claims: last_checked, x_signals: collected_at) also refuse an update from an older version, so a
replayed old batch cannot roll a row back. A ``protect`` rule (claims: checked_by = 'book') keeps a
row Book checked unless the incoming row is Book's too. Joined columns use
``coalesce(excluded.c, t.c)`` so a batch without the joined row keeps what is stored.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from .coerce import TZ, TypedRow
from .schemas import PG_SCHEMA, POSTGRES, CURATED, Curated, postgres_ddl


def connect(db_url: str) -> Any:
    import psycopg

    return psycopg.connect(db_url, autocommit=False, connect_timeout=15,
                           application_name="x_lakehouse")


def ensure_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(postgres_ddl())
    conn.commit()


def is_protected_value(curated: Curated, value: Any) -> bool:
    return bool(curated.protect) and str(value or "").strip().lower() == curated.protect[1]  # type: ignore[index]


def protected_pks(curated: Curated, pairs: list[tuple[tuple[Any, ...], Any]]) -> set[tuple[Any, ...]]:
    """Primary keys with at least one protected (pk, guard value) pair."""
    return {pk for pk, g in pairs if is_protected_value(curated, g)} if curated.protect else set()


def stored_protected(conn: Any, curated: Curated) -> set[tuple[Any, ...]]:
    if not curated.protect:
        return set()
    col, val = curated.protect
    with conn.cursor() as c:
        c.execute(f"select {', '.join(curated.pk)} from {curated.qualified} "
                  f"where lower(coalesce({col}, '')) = %s", (val,))
        return {tuple(r) for r in c.fetchall()}


def eligible(curated: Curated, rows: list[TypedRow], stored: set[tuple[Any, ...]] = frozenset()) -> list[TypedRow]:  # type: ignore[assignment]
    """Drop rows that may not replace a protected claim, BEFORE picking each key's newest row: once
    Book checked a claim (stored, or any row of this batch), only Book's rows are candidates."""
    if not curated.protect:
        return rows
    col = curated.protect[0]
    pk_of = [tuple(r.values.get(k) for k in curated.pk) for r in rows]
    prot = set(stored) | protected_pks(curated, [(pk, r.values.get(col)) for pk, r in zip(pk_of, rows)])
    return [r for pk, r in zip(pk_of, rows) if pk not in prot or is_protected_value(curated, r.values.get(col))]


def version_key(curated: Curated, values: dict[str, Any]) -> tuple[Any, bool] | None:
    """Rank of one version of a key (None: unversioned or no version value). See Curated.version_also."""
    if not curated.version:
        return None
    v = values.get(curated.version)
    also = values.get(curated.version_also) if curated.version_also else None
    top = max((x for x in (v, also) if x is not None), default=None)
    return None if top is None else (top, also is not None)


def version_sql(curated: Curated, alias: str) -> str:
    """SQL twin of version_key as a row value, for ``excluded`` / ``t`` comparisons."""
    v = f"{alias}.{curated.version}"
    if not curated.version_also:
        return v
    a = f"{alias}.{curated.version_also}"
    return f"row(greatest({v}, {a}), {a} is not null)"


def dedupe_latest(curated: Curated, rows: list[TypedRow]) -> list[TypedRow]:
    """One row per primary key: the highest ``version`` wins, then the later Sheet row / list position."""
    out: dict[tuple[Any, ...], TypedRow] = {}
    for r in rows:
        pk = tuple(r.values.get(c) for c in curated.pk)
        if any(v is None for v in pk):
            continue  # reported by validation as a blank key
        prev = out.get(pk)
        if prev is not None and curated.version:
            new_v, old_v = version_key(curated, r.values), version_key(curated, prev.values)
            if old_v is not None and (new_v is None or new_v < old_v):
                continue
        out[pk] = r
    return list(out.values())


def join_feed(rows: list[TypedRow], feed: dict[str, dict[str, Any]], curated: Curated) -> list[dict[str, Any]]:
    records = []
    for r in rows:
        rec = dict(r.values)
        src = feed.get(str(r.values.get("post_id")), {})
        for col, _kind, src_col in curated.joined:
            rec[col] = src.get(src_col)
        records.append(rec)
    return records


def upsert(conn: Any, curated: Curated, records: list[dict[str, Any]], batch_id: str) -> dict[str, int]:
    if not records:
        return {"rows": 0, "inserted": 0, "updated": 0}
    cols = [c for c, _ in curated.data_columns]
    joined = {c for c, _, _ in curated.joined}
    table = curated.qualified
    tmp = f"_stage_{curated.table}"
    col_list = ", ".join(cols)
    sets = [f"{c} = coalesce(excluded.{c}, t.{c})" if c in joined else f"{c} = excluded.{c}"
            for c in cols if c not in curated.pk]
    changed = " or ".join(
        (f"(excluded.{c} is not null and t.{c} is distinct from excluded.{c})" if c in joined
         else f"t.{c} is distinct from excluded.{c}")
        for c in cols if c not in curated.pk
    )
    with conn.cursor() as cur:
        cur.execute(f"create temp table {tmp} ("
                    + ", ".join(f"{c} {POSTGRES[k]}" for c, k in curated.data_columns)
                    + ") on commit drop")
        with cur.copy(f"copy {tmp} ({col_list}) from stdin") as cp:
            for rec in records:
                cp.write_row([rec.get(c) for c in cols])
        cur.execute(
            f"insert into {table} as t ({col_list}, _batch_id) "
            f"select {col_list}, %s from {tmp} "
            f"on conflict ({', '.join(curated.pk)}) do update set "
            + ", ".join(sets + ["_batch_id = excluded._batch_id", "updated_at = now()"])
            + (f" where ({changed})" if changed else " where true")
            + (f" and (t.{curated.version} is null or {version_sql(curated, 'excluded')} >= {version_sql(curated, 't')})"
               if curated.version else "")
            + (f" and not (lower(coalesce(t.{curated.protect[0]}, '')) = '{curated.protect[1]}'"
               f" and lower(coalesce(excluded.{curated.protect[0]}, '')) <> '{curated.protect[1]}')"
               if curated.protect else "")
            + " returning (xmax = 0) as inserted",
            (batch_id,),
        )
        flags = [row[0] for row in cur.fetchall()]
    conn.commit()
    return {"rows": len(records), "inserted": sum(flags), "updated": len(flags) - sum(flags)}


def retention_cut(curated: Curated, now: datetime) -> datetime | date | None:
    """Oldest ``retain_col`` value still kept: an instant for timestamps, an ICT date for dates."""
    if not curated.retain_col or not curated.retain_days:
        return None
    kinds = dict(curated.data_columns)
    if kinds[curated.retain_col] == "date":
        return now.astimezone(TZ).date() - timedelta(days=curated.retain_days)
    return now.astimezone(timezone.utc) - timedelta(days=curated.retain_days)


def within_retention(curated: Curated, records: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    """Drop records the retention prune would delete, so a replay or a Sheet catch-up cannot bring
    them back. A null ``retain_col`` is always kept (e.g. a bookmark that is still bookmarked)."""
    cut = retention_cut(curated, now)
    if cut is None:
        return records
    col = curated.retain_col
    return [r for r in records if r.get(col) is None or r[col] >= cut]


def prune_retention(conn: Any, tables: list[Curated], now: datetime | None = None) -> dict[str, int]:
    """Delete curated rows past each table's retention. Idempotent; raw files are never touched."""
    now = now or datetime.now(timezone.utc)
    out: dict[str, int] = {}
    with conn.cursor() as c:
        for t in tables:
            cut = retention_cut(t, now)
            if cut is None:
                continue
            c.execute(f"delete from {t.qualified} where {t.retain_col} is not null and {t.retain_col} < %s", (cut,))
            out[t.table] = c.rowcount
    conn.commit()
    return out


def count(conn: Any, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"select count(*) from {PG_SCHEMA}.{table}")
        return int(cur.fetchone()[0])


__all__ = ["CURATED", "connect", "count", "dedupe_latest", "eligible", "ensure_schema", "is_protected_value",
           "join_feed", "protected_pks", "prune_retention", "retention_cut", "stored_protected", "upsert",
           "version_key", "version_sql", "within_retention"]
