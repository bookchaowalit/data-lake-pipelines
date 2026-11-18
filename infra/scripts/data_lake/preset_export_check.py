#!/usr/bin/env python3
"""Check a Preset dashboard export ZIP before it is committed (ADR-0016 P3).

Preset Starter has no API, so the jobs dashboard is kept in this repository as the
ZIP the UI exports (Dashboards → ⋯ → Export). This repository is public, so the
ZIP must not carry a credential or data outside the ADR-0016 §2 allow-list:

- no MotherDuck token, JWT, password or encrypted extra in any file (Preset masks
  a URI password, but a ``motherduck_token`` query parameter is exported as is);
- every database is ``duckdb:///md:empire_gold``;
- a physical dataset is one of the approved jobs marts in schema ``jobs`` and uses
  only its approved columns;
- a virtual (SQL) dataset is one of ``infra/bi/preset/jobs/datasets/*.sql``, with
  the same SQL, so the dashboard shows what ``preset_parity.py`` checks;
- the ZIP holds YAML files only, at relative paths, and at least one dashboard.

Exit 0 when every ZIP passes, 1 otherwise. Problems name the file and the rule,
never a matched secret.
"""

from __future__ import annotations

import argparse
import re
import sys
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

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
SECRET_PATTERNS = {
    "motherduck_token value": re.compile(
        r"motherduck_token\s*[=:]\s*[\"']?(?!X+\b)[A-Za-z0-9._\-]{8,}", re.I
    ),
    "JWT": re.compile(r"eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\."),
    "password value": re.compile(
        r"^\s*password\s*:\s*(?!\s*(?:null|''|\"\"|X+)?\s*$).+", re.I | re.M
    ),
    "encrypted extra": re.compile(
        r"^\s*(?:masked_)?encrypted_extra\s*:\s*(?!\s*(?:null|''|\"\"|\{\})?\s*$).+",
        re.I | re.M,
    ),
}


def _normal_sql(sql: str) -> str:
    lines = [line.split("--", 1)[0] for line in sql.splitlines()]
    return " ".join(" ".join(lines).split()).rstrip(";").strip().lower()


APPROVED_SQL = {_normal_sql(dataset_sql(name)): name for name in DATASETS}


def _check_database(path: str, doc: dict[str, Any]) -> list[str]:
    uri = str(doc.get("sqlalchemy_uri") or "")
    if uri.split("?", 1)[0] != DATABASE_URI:
        return [f"{path}: database is not {DATABASE_URI}"]
    if "?" in uri:
        return [
            f"{path}: database URI has query parameters; put the token in Secure Extra"
        ]
    return []


def _check_dataset(path: str, doc: dict[str, Any]) -> list[str]:
    sql = str(doc.get("sql") or "").strip()
    if sql:
        name = APPROVED_SQL.get(_normal_sql(sql))
        if name is None:
            return [
                f"{path}: SQL dataset is not one of infra/bi/preset/jobs/datasets/*.sql"
            ]
        return []
    table, schema = doc.get("table_name"), doc.get("schema")
    catalog = doc.get("catalog")
    problems = []
    if (
        schema != WAREHOUSE_SCHEMA
        or catalog not in (None, WAREHOUSE_DATABASE)
        or table not in EXPORT_APPROVED
    ):
        return [f"{path}: table is not an approved jobs mart"]
    approved = set(EXPORT_APPROVED[str(table)])
    for column in doc.get("columns") or []:
        name = column.get("column_name")
        if column.get("expression"):
            continue  # a calculated column reads the dataset's own (approved) columns
        if name not in approved:
            problems.append(f"{path}: column {name!r} is not approved for {table}")
    return problems


def check_zip(zip_path: Path) -> list[str]:
    import yaml

    problems: list[str] = []
    dashboards = 0
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
            problems += [
                f"{info.filename}: contains a {label}"
                for label, rx in SECRET_PATTERNS.items()
                if rx.search(text)
            ]
            try:
                doc = yaml.safe_load(text) or {}
            except yaml.YAMLError:
                problems.append(f"{info.filename}: not valid YAML")
                continue
            if not isinstance(doc, dict):
                continue
            kind = entry.parts[1] if len(entry.parts) > 2 else ""
            if kind == "databases":
                problems += _check_database(info.filename, doc)
            elif kind == "datasets":
                problems += _check_dataset(info.filename, doc)
            elif kind == "dashboards":
                dashboards += 1
    if not dashboards:
        problems.append(f"{zip_path.name}: no dashboard in the export")
    return problems


def committed_exports() -> list[Path]:
    return sorted(EXPORT_DIR.glob("*.zip"))


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
