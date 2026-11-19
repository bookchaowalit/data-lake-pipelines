#!/usr/bin/env python3
"""ADR-0016 P4 connector test: can Collate's PostgreSQL connector read MotherDuck?

Collate lists no MotherDuck or DuckDB connector. The only path is its PostgreSQL
connector against MotherDuck's Postgres endpoint, which runs DuckDB SQL and does
not support PostgreSQL's ``pg_*`` functions. Before anyone signs up for Collate,
this runs what that connector runs and says whether it can discover
``empire_gold.jobs``:

1. SQLAlchemy dialect (endpoint only), on the connector's own stack (psycopg2,
   SQLAlchemy 2.0; ``collate-probe.yml`` runs it in that environment): the
   version check on connect, the domains and enums its ``get_columns`` loads,
   and primary keys and unique constraints (an SQL error fails, an empty answer
   or NotImplementedError passes, as in the connector). The server version the
   dialect reads is what the connector uses to build its column query.
2. Catalog queries: the connector's own SQL, rendered for that server version.
   The SQL is not stored in this repository: OpenMetadata's ingestion code is
   under the Collate Community License, so the probe downloads ``queries.py`` at
   a pinned commit at run time, checks its sha256, and reads the SQL strings with
   ``ast`` (nothing from the file is executed).

``required`` steps are those whose SQL error makes the connector drop a table or
stop: databases, schemas, tables, schema comments, and per mart columns, foreign
keys and partition details (an empty answer passes). The rest are diagnostic:
the three test-connection queries (optional in the connector's own definition),
table comments (read with errors caught), owners, views, policies, procedures,
functions and indexes. The result is ``viable`` when every required step works
and the connector's own column query returns each approved jobs mart with
exactly its approved columns. Columns are judged by the connector's query only:
OpenMetadata replaces SQLAlchemy's ``get_columns``.

A dropped or refused connection is ``inconclusive`` (exit 2), never ``viable:
false``: only answers from the endpoint count as evidence. A pass is necessary,
not sufficient: Collate may run a newer connector than the pinned commit, so
``viable`` still needs Collate's own test connection and one ingestion run. A
fail settles P4 as not adopted (ADR-0016 1.1).

Targets: ``--duckdb <file>`` runs step 2 on a local DuckDB file (a dry run: the
endpoint is DuckDB SQL too) with an assumed server version; ``--pg-host`` (or
``MOTHERDUCK_PG_HOST``) runs both steps on the endpoint as the read-only
``reader`` account, ``MOTHERDUCK_READER_TOKEN`` as the password, with TLS
certificate and host name verification. The default output is the verdict and
step names; errors are exception types only, and the token never appears.

Exit codes: 0 viable, 1 not viable, 2 inconclusive or the probe could not run.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import sys
import textwrap
import urllib.request
from functools import partial
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.motherduck_load import (
    EXPORT_APPROVED,
    WAREHOUSE_DATABASE,
    WAREHOUSE_SCHEMA,
)  # noqa: E402

# OpenMetadata 2.0.8-release, ingestion/src/metadata/ingestion/source/database/postgres/queries.py.
QUERIES_COMMIT = "dd013e20428ae77fb837332fb646d9b01301ed81"
QUERIES_URL = (
    "https://raw.githubusercontent.com/open-metadata/OpenMetadata/"
    f"{QUERIES_COMMIT}/ingestion/src/metadata/ingestion/source/database/postgres/queries.py"
)
QUERIES_SHA256 = "1ecc25ad9dc298b2dc401a5d7795dfb624577cef8a6f4329959c878abeb19f30"
DRY_RUN_SERVER_VERSION = 160000

# (step, query key, required). Order follows the connector: test connection, then metadata.
CATALOG_STEPS: tuple[tuple[str, str, bool], ...] = (
    ("databases", "POSTGRES_GET_DATABASE", True),
    ("schemas", "POSTGRES_GET_SCHEMA_NAMES", True),
    ("tables", "POSTGRES_GET_TABLE_NAMES", True),
    ("schema_comments", "POSTGRES_SCHEMA_COMMENTS", True),
    # Diagnostic only: table comments are read with errors caught, and the three test queries
    # are optional in the connector's test-connection definition.
    ("table_comments", "POSTGRES_TABLE_COMMENTS", False),
    ("test_column_metadata", "TEST_COLUMN_METADATA", False),
    ("test_table_comments", "TEST_TABLE_COMMENTS", False),
    ("test_information_schema_columns", "TEST_INFORMATION_SCHEMA_COLUMNS", False),
    ("table_owners", "POSTGRES_TABLE_OWNERS", False),
    ("view_definitions", "POSTGRES_VIEW_DEFINITIONS", False),
    ("policy_tags", "POSTGRES_TEST_GET_TAGS", False),
    ("table_policies", "POSTGRES_GET_ALL_TABLE_PG_POLICY", False),
    ("stored_procedures", "POSTGRES_GET_STORED_PROCEDURES", False),
    ("functions", "POSTGRES_GET_FUNCTIONS", False),
)
# Per approved mart, after the table oid is known.
PER_TABLE_STEPS: tuple[tuple[str, str, bool], ...] = (
    ("columns", "POSTGRES_SQL_COLUMNS", True),
    # Required: the connector reads foreign keys and partition details for every table before
    # it yields it, and an SQL error there drops the table (an empty answer is fine).
    ("foreign_keys", "POSTGRES_FETCH_FK", True),
    ("partitions", "POSTGRES_PARTITION_DETAILS", True),
)
NEEDED = {key for _, key, _ in CATALOG_STEPS + PER_TABLE_STEPS} | {
    "POSTGRES_COL_IDENTITY"
}
# Stand-in for SQLAlchemy's get_table_oid (used only to feed the per-table queries).
TABLE_OID_SQL = (
    "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = :schema AND c.relname = :table_name"
)
BIND = re.compile(r"(?<![:\w]):([A-Za-z_]\w*)")


class ProbeError(RuntimeError):
    """The probe could not run (source download, query file, no target)."""


class Inconclusive(RuntimeError):
    """The endpoint stopped answering mid-probe: no verdict either way."""


def extract_queries(source: str) -> dict[str, str]:
    """Module-level SQL string constants (plain or ``textwrap.dedent``), read with ast only."""
    out: dict[str, str] = {}
    for node in ast.parse(source).body:
        if not (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            continue
        value = node.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            out[node.targets[0].id] = value.value
        elif (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Attribute)
            and value.func.attr == "dedent"
            and len(value.args) == 1
            and isinstance(value.args[0], ast.Constant)
            and isinstance(value.args[0].value, str)
        ):
            out[node.targets[0].id] = textwrap.dedent(value.args[0].value)
    if NEEDED - set(out):
        raise ProbeError("the pinned queries.py lacks queries the probe runs")
    return out


def load_queries(fetch: Callable[[str], bytes] | None = None) -> dict[str, str]:
    """Download the pinned queries.py, check its sha256, and extract the SQL."""

    def _get(url: str) -> bytes:
        with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310 - fixed https URL
            return response.read()

    data = (fetch or _get)(QUERIES_URL)
    if hashlib.sha256(data).hexdigest() != QUERIES_SHA256:
        raise ProbeError("queries.py does not match the pinned sha256")
    return extract_queries(data.decode("utf-8"))


def render(queries: dict[str, str], key: str, *, server_version: int) -> str:
    """The query as the connector sends it for that server version: format placeholders filled."""
    sql = queries[key]
    if key == "POSTGRES_SQL_COLUMNS":
        # OpenMetadata's get_columns: generated columns from 12, identity options from 10.
        generated = (
            "a.attgenerated as generated"
            if server_version >= 120000
            else "NULL as generated"
        )
        identity = (
            queries["POSTGRES_COL_IDENTITY"]
            if server_version >= 100000
            else "NULL as identity_options"
        )
        sql = sql.format(generated=generated, identity=identity)
    elif "{schema_name}" in sql or "{database_name}" in sql:
        sql = sql.replace("{schema_name}", WAREHOUSE_SCHEMA).replace(
            "{database_name}", WAREHOUSE_DATABASE
        )
    return sql


def _error(exc: BaseException) -> str:
    """Exception type only: driver messages can carry hosts or server text (public log)."""
    return type(exc).__name__


class Runner:
    """Runs one query with named binds on DuckDB (``$name``) or psycopg (``%(name)s``).

    ``fatal`` exception types mean the connection itself failed; they propagate.
    """

    def __init__(
        self,
        execute: Callable[[str, dict[str, Any] | None], list[tuple]],
        style: str,
        fatal: tuple[type[BaseException], ...] = (),
    ):
        self._execute = execute
        self._style = style
        self.fatal = fatal

    def rows(self, sql: str, params: dict[str, Any] | None = None) -> list[tuple]:
        names = set(BIND.findall(sql))
        if not names:
            # No parameters at all, so psycopg leaves % alone (LIKE 'pg\\_%').
            return self._execute(sql, None)
        bound = {k: v for k, v in (params or {}).items() if k in names}
        if self._style == "duckdb":
            sql = BIND.sub(lambda m: f"${m.group(1)}", sql)
        else:
            # psycopg treats every % as a placeholder once parameters are passed.
            sql = BIND.sub(lambda m: f"%({m.group(1)})s", sql.replace("%", "%%"))
        return self._execute(sql, bound)


def run_catalog(
    runner: Runner, queries: dict[str, str], *, server_version: int
) -> dict[str, Any]:
    steps: dict[str, dict[str, Any]] = {}

    def attempt(
        name: str, required: bool, sql: str, params: dict[str, Any]
    ) -> list[tuple] | None:
        try:
            rows = runner.rows(sql, params)
        except runner.fatal as exc:
            raise Inconclusive(_error(exc)) from None
        except Exception as exc:  # noqa: BLE001 - an error answer is a probe result
            steps[name] = {"required": required, "ok": False, "error": _error(exc)}
            return None
        steps[name] = {"required": required, "ok": True, "rows": len(rows)}
        return rows

    found: dict[str, Any] = {}
    for name, key, required in CATALOG_STEPS:
        rows = attempt(
            name,
            required,
            render(queries, key, server_version=server_version),
            {"schema": WAREHOUSE_SCHEMA},
        )
        if name == "schemas":
            found["schema_listed"] = rows is not None and WAREHOUSE_SCHEMA in {
                r[0] for r in rows
            }
        if name == "tables":
            found["tables_listed"] = sorted(
                {r[0] for r in rows or []} & set(EXPORT_APPROVED)
            )
    columns_ok: dict[str, bool] = {}
    for mart, approved in EXPORT_APPROVED.items():
        oid_rows = attempt(
            f"{mart}.oid",
            True,
            TABLE_OID_SQL,
            {"schema": WAREHOUSE_SCHEMA, "table_name": mart},
        )
        if not oid_rows:
            if oid_rows == []:
                steps[f"{mart}.oid"] = {
                    "required": True,
                    "ok": False,
                    "error": "table not found",
                }
            columns_ok[mart] = False
            continue
        oid = oid_rows[0][0]
        params = {
            "table_oid": oid,
            "table": oid,
            "table_name": mart,
            "schema_name": WAREHOUSE_SCHEMA,
        }
        for name, key, required in PER_TABLE_STEPS:
            rows = attempt(
                f"{mart}.{name}",
                required,
                render(queries, key, server_version=server_version),
                params,
            )
            if name == "columns":
                columns_ok[mart] = rows is not None and [r[0] for r in rows] == list(
                    approved
                )
    found["columns_match"] = columns_ok
    return {"server_version": server_version, "steps": steps, "found": found}


def server_version_num(info: tuple | None) -> int | None:
    """SQLAlchemy's server_version_info as PostgreSQL's server_version_num (16.2 -> 160002)."""
    if not info:
        return None
    major, minor, patch = (list(info) + [0, 0, 0])[:3]
    if major >= 10:
        return major * 10000 + minor
    return major * 10000 + minor * 100 + patch


def run_dialect(
    engine: Any, fatal: tuple[type[BaseException], ...] = ()
) -> tuple[dict[str, Any], int | None]:
    """Connect through SQLAlchemy's PostgreSQL dialect and reflect what the connector takes from it.

    Returns the stage result and the server version the dialect read (None if it could not).
    Columns are not reflected here: OpenMetadata replaces the dialect's get_columns with its
    own query, which the catalog stage runs.
    """
    from sqlalchemy import inspect

    steps: dict[str, dict[str, Any]] = {}
    try:
        inspector = inspect(engine)  # connects and runs the dialect's version check
    except fatal as exc:
        raise Inconclusive(_error(exc)) from None
    except Exception as exc:  # noqa: BLE001
        steps["dialect_connect"] = {"required": True, "ok": False, "error": _error(exc)}
        return {"steps": steps, "found": {}}, None
    steps["dialect_connect"] = {"required": True, "ok": True}
    version = server_version_num(getattr(engine.dialect, "server_version_info", None))

    def call(name: str, required: bool, fn: Callable[[], Any]) -> Any:
        try:
            value = fn()
        except fatal as exc:
            raise Inconclusive(_error(exc)) from None
        except Exception as exc:  # noqa: BLE001
            steps[name] = {"required": required, "ok": False, "error": _error(exc)}
            return None
        steps[name] = {"required": required, "ok": True}
        return value

    def constraint(name: str, fn: Callable[[], Any]) -> None:
        # The connector reads constraints before columns and skips only NotImplementedError.
        try:
            fn()
        except NotImplementedError:
            steps[name] = {"required": True, "ok": True, "note": "not implemented"}
        except fatal as exc:
            raise Inconclusive(_error(exc)) from None
        except Exception as exc:  # noqa: BLE001
            steps[name] = {"required": True, "ok": False, "error": _error(exc)}
        else:
            steps[name] = {"required": True, "ok": True}

    # The connector lists tables with its own query (catalog stage); this is diagnostic.
    tables = call(
        "table_names",
        False,
        partial(inspector.get_table_names, schema=WAREHOUSE_SCHEMA),
    )
    call(
        "view_names", False, partial(inspector.get_view_names, schema=WAREHOUSE_SCHEMA)
    )
    # Its get_columns loads the dialect's domains and enums before it returns any column.
    try:
        with engine.connect() as conn:
            call("domains", True, partial(engine.dialect._load_domains, conn))
            call("enums", True, partial(engine.dialect._load_enums, conn, schema="*"))
    except fatal as exc:
        raise Inconclusive(_error(exc)) from None
    for mart in EXPORT_APPROVED:
        constraint(
            f"{mart}.pk",
            partial(inspector.get_pk_constraint, mart, schema=WAREHOUSE_SCHEMA),
        )
        constraint(
            f"{mart}.unique",
            partial(inspector.get_unique_constraints, mart, schema=WAREHOUSE_SCHEMA),
        )
        call(
            f"{mart}.indexes",
            False,
            partial(inspector.get_indexes, mart, schema=WAREHOUSE_SCHEMA),
        )
    found = {"tables_listed": sorted(set(tables or []) & set(EXPORT_APPROVED))}
    return {"steps": steps, "found": found}, version


def verdict(stages: dict[str, dict[str, Any]]) -> dict[str, Any]:
    def names(required: bool) -> list[str]:
        return sorted(
            f"{stage}.{name}"
            for stage, result in stages.items()
            for name, info in result["steps"].items()
            if info["required"] is required and not info["ok"]
        )

    catalog = stages.get("catalog", {}).get("found", {})
    discovered = (
        bool(catalog.get("schema_listed"))
        and catalog.get("tables_listed") == sorted(EXPORT_APPROVED)
        and all(catalog.get("columns_match", {}).get(m) for m in EXPORT_APPROVED)
        and all(
            result["found"].get("tables_listed") == sorted(EXPORT_APPROVED)
            for stage, result in stages.items()
            if stage != "catalog" and result["found"]
        )
    )
    failed = names(True)
    return {
        "viable": not failed and discovered,
        "required_failed": failed,
        "optional_failed": names(False),
        "discovered_approved_marts": discovered,
    }


def probe_duckdb(
    path: str,
    *,
    queries: dict[str, str] | None = None,
    server_version: int = DRY_RUN_SERVER_VERSION,
) -> dict[str, Any]:
    import duckdb  # type: ignore

    queries = queries or load_queries()
    con = duckdb.connect(path, read_only=True)
    try:
        runner = Runner(
            lambda sql, params: con.execute(sql, params or []).fetchall(), "duckdb"
        )
        stages = {
            "catalog": run_catalog(runner, queries, server_version=server_version)
        }
    finally:
        con.close()
    return {
        "target": "duckdb",
        "assumed_server_version": server_version,
        "stages": stages,
        **verdict(stages),
    }


def probe_postgres(
    host: str, token: str, *, port: int = 5432, queries: dict[str, str] | None = None
) -> dict[str, Any]:
    """Both stages on the endpoint, with the connector's own stack: psycopg2 and SQLAlchemy 2.0.

    Run it in that environment (see ``collate-probe.yml``): the project lock carries psycopg 3
    and SQLAlchemy 2.1, whose start-up queries differ from the connector's.
    """
    import psycopg2  # type: ignore[import-untyped]
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    from sqlalchemy.exc import (
        InterfaceError as SAInterfaceError,
        OperationalError as SAOperationalError,
    )

    queries = queries or load_queries()
    # verify-full + the system CA store: the token goes only to a server that proves it is the host.
    tls = {"sslmode": "verify-full", "sslrootcert": "system"}
    fatal: tuple[type[BaseException], ...] = (
        psycopg2.OperationalError,
        psycopg2.InterfaceError,
        SAOperationalError,
        SAInterfaceError,
    )
    stages: dict[str, dict[str, Any]] = {}
    url = URL.create(
        "postgresql+psycopg2",
        username="postgres",
        password=token,
        host=host,
        port=port,
        database=WAREHOUSE_DATABASE,
        query=tls,
    )
    engine = create_engine(
        url, pool_pre_ping=False, connect_args={"connect_timeout": 20}
    )
    try:
        try:
            stages["dialect"], version = run_dialect(engine, fatal)
        except fatal as exc:
            raise Inconclusive(_error(exc)) from None
    finally:
        engine.dispose()
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user="postgres",
            password=token,
            dbname=WAREHOUSE_DATABASE,
            connect_timeout=20,
            **tls,
        )
    except fatal as exc:
        raise Inconclusive(_error(exc)) from None
    try:
        conn.autocommit = True
        if version is None:
            # The dialect could not read a version (already a required failure); render as the
            # libpq-reported version so the catalog answers are still recorded.
            version = conn.server_version

        def execute(sql: str, params: dict[str, Any] | None) -> list[tuple]:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall() if cur.description is not None else []

        runner = Runner(execute, "psycopg", fatal=fatal)
        stages["catalog"] = run_catalog(runner, queries, server_version=version)
    finally:
        conn.close()
    return {
        "target": "motherduck_postgres_endpoint",
        "server_version": version,
        "stages": stages,
        **verdict(stages),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    target = parser.add_mutually_exclusive_group()
    target.add_argument(
        "--duckdb", help="local DuckDB file holding empire_gold.jobs (dry run)"
    )
    target.add_argument(
        "--pg-host",
        default=os.environ.get("MOTHERDUCK_PG_HOST", ""),
        help="MotherDuck Postgres endpoint host, e.g. pg.<region>-aws.motherduck.com",
    )
    parser.add_argument(
        "--assume-server-version",
        type=int,
        default=DRY_RUN_SERVER_VERSION,
        help="dry run only: server_version_num to render the column query for",
    )
    parser.add_argument(
        "--json", action="store_true", help="print every step, not only the verdict"
    )
    args = parser.parse_args(argv)
    token = os.environ.get("MOTHERDUCK_READER_TOKEN", "").strip()
    try:
        if args.duckdb:
            result = probe_duckdb(
                args.duckdb, server_version=args.assume_server_version
            )
        elif args.pg_host and token:
            result = probe_postgres(args.pg_host, token)
        else:
            raise ProbeError("no target")
    except Inconclusive as exc:
        print(json.dumps({"status": "inconclusive", "error": str(exc)}))
        return 2
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        print(json.dumps({"status": "failed", "error": _error(exc)}))
        return 2
    keys = (
        "target",
        "viable",
        "required_failed",
        "optional_failed",
        "discovered_approved_marts",
    )
    summary = {"status": "verdict", **{k: result[k] for k in keys}}
    print(
        json.dumps(
            {"status": "verdict", **result} if args.json else summary,
            sort_keys=True,
            default=str,
        )
    )
    return 0 if result["viable"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
