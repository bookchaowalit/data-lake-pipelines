"""Curated layer in Postgres (Supabase): deduped, joined, upserted. Never plain INSERT.

Each upsert loads the batch into a temp table with COPY, then
``INSERT ... SELECT ... ON CONFLICT (pk) DO UPDATE ... WHERE <row changed>`` so reruns are no-ops
and ``updated_at`` moves only when a value really changed. Tables with a ``version`` column
(claims: last_checked, x_signals: collected_at) also refuse an update from an older version, so a
replayed old batch cannot roll a row back. Joined columns use
``coalesce(excluded.c, t.c)`` so a batch without the joined row keeps what is stored.
"""

from __future__ import annotations

from typing import Any

from .coerce import TypedRow
from .schemas import PG_SCHEMA, POSTGRES, CURATED, Curated, postgres_ddl


def connect(db_url: str) -> Any:
    import psycopg

    return psycopg.connect(db_url, autocommit=False, connect_timeout=15,
                           application_name="x_lakehouse")


def ensure_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(postgres_ddl())
    conn.commit()


def dedupe_latest(curated: Curated, rows: list[TypedRow]) -> list[TypedRow]:
    """One row per primary key: the highest ``version`` wins, then the later Sheet row / list position."""
    out: dict[tuple[Any, ...], TypedRow] = {}
    for r in rows:
        pk = tuple(r.values.get(c) for c in curated.pk)
        if any(v is None for v in pk):
            continue  # reported by validation as a blank key
        prev = out.get(pk)
        if prev is not None and curated.version:
            new_v, old_v = r.values.get(curated.version), prev.values.get(curated.version)
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
    table = f"{PG_SCHEMA}.{curated.table}"
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
            + (f" and (t.{curated.version} is null or excluded.{curated.version} >= t.{curated.version})"
               if curated.version else "")
            + " returning (xmax = 0) as inserted",
            (batch_id,),
        )
        flags = [row[0] for row in cur.fetchall()]
    conn.commit()
    return {"rows": len(records), "inserted": sum(flags), "updated": len(flags) - sum(flags)}


def count(conn: Any, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"select count(*) from {PG_SCHEMA}.{table}")
        return int(cur.fetchone()[0])


__all__ = ["CURATED", "connect", "count", "dedupe_latest", "ensure_schema", "join_feed", "upsert"]
