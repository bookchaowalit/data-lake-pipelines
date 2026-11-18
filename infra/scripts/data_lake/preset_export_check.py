#!/usr/bin/env python3
"""Check a Preset dashboard export ZIP before it is committed (ADR-0016 P3).

Preset Starter has no API, so the jobs dashboard is kept in this repository as the
ZIP the UI exports (Dashboards → ⋯ → Export). This repository is public, so the
ZIP must not carry a credential or data outside the ADR-0016 §2 allow-list:

- No credential anywhere. Checked on the parsed YAML (every key and value, and
  JSON embedded in string values such as chart ``params``), plus a raw-text scan:
  no ``motherduck_token`` value, JWT, unmasked password, secret-named key with a
  value, or ``encrypted_extra``. (Preset masks a URI password, but a
  ``motherduck_token`` query parameter is exported as is.)
- Every database is exactly ``duckdb:///md:empire_gold``.
- A physical dataset is an approved jobs mart in schema ``jobs`` with only its
  approved columns. A SQL dataset is one of ``infra/bi/preset/jobs/datasets/*.sql``
  with the same SQL, so the dashboard shows what ``preset_parity.py`` checks.
- Every SQL fragment Preset builds queries from (calculated columns, dataset
  metrics, and ``sqlExpression`` / ``expression`` in charts) may name only the
  columns of its own dataset, plus a fixed list of SQL keywords and functions; no
  quoted or dotted identifiers, comments or statement separators. Charts must
  point at a dataset in the same export.
- YAML files only, at relative paths, at least one dashboard.

Exit 0 when every ZIP passes, 1 otherwise. Problems name the file and the rule,
never a matched value.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.motherduck_load import (
    EXPORT_APPROVED,
    WAREHOUSE_DATABASE,
    WAREHOUSE_SCHEMA,
)  # noqa: E402
from data_lake.preset_parity import DATASETS, dataset_sql  # noqa: E402

EXPORT_DIR = ROOT / "infra" / "bi" / "preset" / "jobs"
DATABASE_URI = f"duckdb:///md:{WAREHOUSE_DATABASE}"
MAX_ENTRY_BYTES = 5 * 1024 * 1024
RAW_SECRETS = {
    "motherduck_token value": re.compile(
        r"motherduck_token\W{0,6}(?!X+\b)[A-Za-z0-9._\-]{8,}", re.I
    ),
    "JWT": re.compile(r"eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\."),
}
SECRET_KEY = re.compile(
    r"(password|passwd|secret|token|api[_-]?key|private[_-]?key|credential|encrypted_extra|access[_-]?key)",
    re.I,
)
MASKED = re.compile(r"^X+$")
# Identifiers an expression may use besides its dataset's columns.
SQL_WORDS = frozenset(
    """
    sum count avg min max distinct case when then else end and or not null is in between like ilike
    as cast coalesce nullif round floor ceil abs greatest least date_trunc date_part extract year month
    week day quarter interval true false integer int bigint double float decimal varchar date timestamp
    lower upper length trim asc desc filter where over partition by order
    """.split()
)
# Keys whose string values Superset puts into the SQL it runs (adhoc metrics and filters, the
# legacy free-form WHERE/HAVING boxes and query_context extras).
EXPRESSION_KEYS = ("sqlExpression", "expression", "where", "having")


def _normal_sql(sql: str) -> str:
    """Comments dropped, whitespace collapsed and case folded outside quoted text only."""
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == ch:
                    if j + 1 < n and sql[j + 1] == ch:
                        j += 2
                        continue
                    break
                j += 1
            out.append(sql[i : j + 1])
            i = j + 1
        elif sql.startswith("--", i):
            while i < n and sql[i] != "\n":
                i += 1
        elif ch.isspace():
            if out and out[-1] != " ":
                out.append(" ")
            i += 1
        else:
            out.append(ch.lower())
            i += 1
    return "".join(out).strip().rstrip(";").strip()


APPROVED_SQL = {_normal_sql(dataset_sql(name)): name for name in DATASETS}


def _walk(value: Any, depth: int = 0) -> Iterator[tuple[str | None, Any]]:
    """Every (key, value) pair, descending into JSON held in string values."""
    if depth > 20:
        return
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key), item
            yield from _walk(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            yield None, item
            yield from _walk(item, depth + 1)
    elif isinstance(value, str) and value.lstrip()[:1] in ("{", "["):
        try:
            yield from _walk(json.loads(value), depth + 1)
        except ValueError:
            pass


def _empty(value: Any) -> bool:
    if value is None or value in ("", {}, []):
        return True
    if isinstance(value, str):
        text = value.strip()
        return not text or bool(MASKED.match(text)) or text in ("{}", "[]", "null")
    return False


def _secret_problems(path: str, text: str, doc: Any) -> list[str]:
    problems = [
        f"{path}: contains a {label}"
        for label, rx in RAW_SECRETS.items()
        if rx.search(text)
    ]
    for key, value in _walk(doc):
        if (
            key is not None
            and SECRET_KEY.search(key)
            and not _empty(value)
            and not isinstance(value, bool)
        ):
            problems.append(f"{path}: key {key!r} holds a value")
        if isinstance(value, str) and any(
            rx.search(value) for rx in RAW_SECRETS.values()
        ):
            problems.append(f"{path}: a value holds a credential")
    return sorted(set(problems))


def _expression_problem(expression: str, columns: set[str]) -> str | None:
    """None when the SQL fragment only names ``columns`` and allowed SQL words."""
    if any(mark in expression for mark in ('"', "`", "--", "/*", ";", "[")):
        return "uses quoted identifiers, comments or statement separators"
    stripped = re.sub(r"'(?:[^']|'')*'", " ", expression)  # string literals
    stripped = re.sub(r"\b\d+(?:\.\d+)?\b", " ", stripped)  # numbers
    if "." in stripped:
        return "uses a dotted (qualified) name"
    for word in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", stripped):
        if word not in columns and word.lower() not in SQL_WORDS:
            return f"names {word!r}, which is not a column of its dataset"
    return None


def _dataset(path: str, doc: dict[str, Any]) -> tuple[list[str], set[str]]:
    """Problems, and the columns the dataset's own expressions may name."""
    sql = str(doc.get("sql") or "").strip()
    listed = [c for c in doc.get("columns") or [] if isinstance(c, dict)]
    problems: list[str] = []
    if sql:
        if _normal_sql(sql) not in APPROVED_SQL:
            return [
                f"{path}: SQL dataset is not one of infra/bi/preset/jobs/datasets/*.sql"
            ], set()
        columns = {str(c.get("column_name")) for c in listed if not c.get("expression")}
    else:
        table = doc.get("table_name")
        if (
            doc.get("schema") != WAREHOUSE_SCHEMA
            or doc.get("catalog") not in (None, WAREHOUSE_DATABASE)
            or table not in EXPORT_APPROVED
        ):
            return [f"{path}: table is not an approved jobs mart"], set()
        columns = set(EXPORT_APPROVED[str(table)])
        for column in listed:
            if (
                not column.get("expression")
                and column.get("column_name") not in columns
            ):
                problems.append(
                    f"{path}: column {column.get('column_name')!r} is not approved for {table}"
                )
    fragments = [c.get("expression") for c in listed] + [
        m.get("expression") for m in doc.get("metrics") or [] if isinstance(m, dict)
    ]
    for fragment in fragments:
        if fragment and (problem := _expression_problem(str(fragment), columns)):
            problems.append(f"{path}: an expression {problem}")
    return problems, columns


def check_zip(zip_path: Path) -> list[str]:
    import yaml

    problems: list[str] = []
    docs: dict[str, tuple[str, dict[str, Any]]] = {}
    try:
        archive = zipfile.ZipFile(zip_path)
    except zipfile.BadZipFile:
        return [f"{zip_path.name}: not a ZIP file"]
    with archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            entry = PurePosixPath(info.filename)
            if entry.is_absolute() or ".." in entry.parts:
                problems.append(f"{info.filename}: unsafe path")
                continue
            if entry.suffix not in (".yaml", ".yml"):
                problems.append(
                    f"{info.filename}: only YAML files belong in a dashboard export"
                )
                continue
            if info.file_size > MAX_ENTRY_BYTES:
                problems.append(f"{info.filename}: larger than {MAX_ENTRY_BYTES} bytes")
                continue
            text = archive.read(info).decode("utf-8", errors="replace")
            try:
                doc = yaml.safe_load(text)
            except yaml.YAMLError:
                problems.append(f"{info.filename}: not valid YAML")
                continue
            problems += _secret_problems(info.filename, text, doc)
            if isinstance(doc, dict):
                kind = entry.parts[1] if len(entry.parts) > 2 else ""
                docs[info.filename] = (kind, doc)

    dataset_columns: dict[str, set[str]] = {}
    dataset_names: dict[str, str] = {}
    for path, (kind, doc) in docs.items():
        if kind == "databases":
            if str(doc.get("sqlalchemy_uri") or "") != DATABASE_URI:
                problems.append(
                    f"{path}: database is not exactly {DATABASE_URI} (put the token in Secure Extra)"
                )
        elif kind == "datasets":
            found, columns = _dataset(path, doc)
            problems += found
            dataset_columns[str(doc.get("uuid"))] = columns
            if name := APPROVED_SQL.get(_normal_sql(str(doc.get("sql") or ""))):
                dataset_names[str(doc.get("uuid"))] = name
    chart_datasets: dict[str, str] = {}
    for path, (kind, doc) in docs.items():
        if kind != "charts":
            continue
        dataset_uuid = str(doc.get("dataset_uuid"))
        chart_columns = dataset_columns.get(dataset_uuid)
        if chart_columns is None:
            problems.append(f"{path}: chart does not use a dataset in this export")
            continue
        chart_datasets[str(doc.get("uuid"))] = dataset_uuid
        for key, value in _walk(doc):
            if (
                key in EXPRESSION_KEYS
                and value
                and (problem := _expression_problem(str(value), chart_columns))
            ):
                problems.append(f"{path}: a chart {key} {problem}")
    dashboards = [
        (path, doc) for path, (kind, doc) in docs.items() if kind == "dashboards"
    ]
    if not dashboards:
        problems.append(f"{zip_path.name}: no dashboard in the export")
    shown: set[str] = set()
    for path, doc in dashboards:
        for key, value in _walk(doc):
            if key in EXPRESSION_KEYS and value:
                problems.append(f"{path}: the dashboard holds SQL ({key})")
            if isinstance(value, dict) and value.get("type") == "CHART":
                chart = str((value.get("meta") or {}).get("uuid"))
                if chart not in chart_datasets:
                    problems.append(
                        f"{path}: the dashboard shows a chart that is not in the export"
                    )
                    continue
                if name := dataset_names.get(chart_datasets[chart]):
                    shown.add(name)
    # The parity check only proves what these four datasets return: the dashboard must show each.
    for name in DATASETS:
        if dashboards and name not in shown:
            problems.append(
                f"{zip_path.name}: no dashboard chart uses the {name} dataset"
            )
    return problems


def committed_exports() -> list[Path]:
    return sorted(EXPORT_DIR.glob("*.zip"))


def export_fingerprint() -> str | None:
    """sha256 over the committed exports, or None when there is none or one fails the check."""
    zips = committed_exports()
    if not zips or any(check_zip(path) for path in zips):
        return None
    digest = hashlib.sha256()
    for path in zips:
        digest.update(
            path.name.encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest()
        )
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "zips",
        nargs="*",
        type=Path,
        help=f"default: every ZIP in {EXPORT_DIR.relative_to(ROOT)}",
    )
    args = parser.parse_args(argv)
    zips = args.zips or committed_exports()
    failed = False
    for zip_path in zips:
        problems = check_zip(zip_path)
        failed = failed or bool(problems)
        print(f"{'FAIL' if problems else 'ok'} {zip_path}")
        for problem in problems:
            print(f"  - {problem}")
    if not zips:
        print("no Preset exports to check")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
