#!/usr/bin/env python3
"""Validate the provider-neutral metadata catalog, dictionary, and lineage."""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import yaml

from _env import PROJECT_ROOT


CATALOG_DIR = PROJECT_ROOT / "systems" / "ai-data" / "metadata-catalog"
CATALOG_PATH = CATALOG_DIR / "catalog.yaml"
DICTIONARY_PATH = CATALOG_DIR / "data-dictionary.yaml"
LINEAGE_PATH = CATALOG_DIR / "lineage.yaml"
PRODUCT_CONTRACT_PATH = PROJECT_ROOT / "systems" / "architecture" / "data-product-contracts-v3.yaml"

ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,127}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ALLOWED_KINDS = {"source_document", "dataset", "database", "data_product", "consumer"}
ALLOWED_STATUSES = {"active", "implemented", "pilot", "planned", "deprecated"}


def _load(path: Path, errors: list[str]) -> dict:
    if not path.is_file():
        errors.append(f"missing metadata file: {path.relative_to(PROJECT_ROOT)}")
        return {}
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        errors.append(f"invalid YAML in {path.relative_to(PROJECT_ROOT)}: {exc}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"YAML root must be a mapping: {path.relative_to(PROJECT_ROOT)}")
        return {}
    return value


def _required(mapping: dict, fields: tuple[str, ...], label: str, errors: list[str]) -> None:
    for field in fields:
        if mapping.get(field) in (None, ""):
            errors.append(f"{label}: missing required field {field!r}")


def _check_id(value: object, label: str, errors: list[str]) -> None:
    if not isinstance(value, str) or not ID_RE.match(value):
        errors.append(f"{label}: invalid id {value!r}")


def _check_path(value: object, label: str, errors: list[str]) -> None:
    if not isinstance(value, str) or value.startswith("/") or ".." in value.split("/"):
        errors.append(f"{label}: path must be repository-relative without '..'")
        return
    if not (PROJECT_ROOT / value).is_file():
        errors.append(f"{label}: referenced file does not exist: {value}")


def validate() -> list[str]:
    errors: list[str] = []
    catalog = _load(CATALOG_PATH, errors)
    dictionary = _load(DICTIONARY_PATH, errors)
    lineage = _load(LINEAGE_PATH, errors)

    if catalog:
        _required(catalog, ("catalog_id", "catalog_version", "kind", "status", "updated", "assets"), "catalog", errors)
        if catalog.get("kind") != "metadata_catalog":
            errors.append("catalog: kind must be metadata_catalog")
        if catalog.get("visibility") != "public_metadata_only":
            errors.append("catalog: visibility must be public_metadata_only")
    if dictionary:
        _required(dictionary, ("dictionary_id", "dictionary_version", "kind", "status", "updated", "entries"), "dictionary", errors)
        if dictionary.get("kind") != "data_dictionary":
            errors.append("dictionary: kind must be data_dictionary")
    if lineage:
        _required(lineage, ("lineage_id", "lineage_version", "kind", "status", "updated", "edges"), "lineage", errors)
        if lineage.get("kind") != "logical_lineage":
            errors.append("lineage: kind must be logical_lineage")

    assets = catalog.get("assets", []) if isinstance(catalog.get("assets"), list) else []
    asset_ids: set[str] = set()
    asset_by_id: dict[str, dict] = {}
    for index, asset in enumerate(assets):
        label = f"catalog.assets[{index}]"
        if not isinstance(asset, dict):
            errors.append(f"{label}: entry must be a mapping")
            continue
        asset_id = asset.get("id")
        _check_id(asset_id, label, errors)
        if isinstance(asset_id, str):
            if asset_id in asset_ids:
                errors.append(f"{label}: duplicate asset id {asset_id!r}")
            asset_ids.add(asset_id)
            asset_by_id[asset_id] = asset
        _required(asset, ("id", "kind", "name", "description", "owner", "status", "data_classification", "visibility", "change_history"), label, errors)
        if asset.get("kind") not in ALLOWED_KINDS:
            errors.append(f"{label}: kind {asset.get('kind')!r} is not supported")
        if asset.get("status") not in ALLOWED_STATUSES:
            errors.append(f"{label}: status {asset.get('status')!r} is not supported")
        if not isinstance(asset.get("change_history"), list) or not asset.get("change_history"):
            errors.append(f"{label}: change_history must contain an initial record")
        else:
            history_ids: set[str] = set()
            for event_index, event in enumerate(asset["change_history"]):
                event_label = f"{label}.change_history[{event_index}]"
                if not isinstance(event, dict):
                    errors.append(f"{event_label}: entry must be a mapping")
                    continue
                _required(event, ("id", "date", "type", "summary", "actor", "evidence"), event_label, errors)
                if event.get("id") in history_ids:
                    errors.append(f"{event_label}: duplicate change id {event.get('id')!r}")
                history_ids.add(event.get("id"))
                event_date = event.get("date")
                if isinstance(event_date, (date, datetime)):
                    event_date = event_date.isoformat()
                if not isinstance(event_date, str) or not DATE_RE.match(event_date):
                    errors.append(f"{event_label}: date must use YYYY-MM-DD")
                _check_path(event.get("evidence"), f"{event_label}.evidence", errors)
        for reference_field in ("source_path", "schema_ref", "dictionary_ref", "lineage_ref", "contract_path"):
            if reference_field in asset:
                reference = str(asset[reference_field]).split("#", 1)[0]
                _check_path(reference, f"{label}.{reference_field}", errors)

    dictionary_entries = dictionary.get("entries", []) if isinstance(dictionary.get("entries"), list) else []
    dictionary_ids: set[str] = set()
    for index, entry in enumerate(dictionary_entries):
        label = f"dictionary.entries[{index}]"
        if not isinstance(entry, dict):
            errors.append(f"{label}: entry must be a mapping")
            continue
        entry_id = entry.get("id")
        _check_id(entry_id, label, errors)
        if isinstance(entry_id, str) and entry_id in dictionary_ids:
            errors.append(f"{label}: duplicate dictionary id {entry_id!r}")
        if isinstance(entry_id, str):
            dictionary_ids.add(entry_id)
        _required(entry, ("id", "asset_id", "grain", "source_ref", "columns"), label, errors)
        if entry.get("asset_id") not in asset_ids:
            errors.append(f"{label}: asset_id {entry.get('asset_id')!r} is not in catalog")
        source_ref = str(entry.get("source_ref", "")).split("#", 1)[0]
        _check_path(source_ref, f"{label}.source_ref", errors)
        columns = entry.get("columns")
        if not isinstance(columns, list) or not columns:
            errors.append(f"{label}: columns must be a non-empty list")
            continue
        column_ids: set[str] = set()
        for column_index, column in enumerate(columns):
            column_label = f"{label}.columns[{column_index}]"
            if not isinstance(column, dict):
                errors.append(f"{column_label}: entry must be a mapping")
                continue
            _required(column, ("name", "type", "nullable", "description", "classification", "retention"), column_label, errors)
            name = column.get("name")
            if name in column_ids:
                errors.append(f"{column_label}: duplicate column name {name!r}")
            column_ids.add(name)

    edges = lineage.get("edges", []) if isinstance(lineage.get("edges"), list) else []
    edge_ids: set[str] = set()
    for index, edge in enumerate(edges):
        label = f"lineage.edges[{index}]"
        if not isinstance(edge, dict):
            errors.append(f"{label}: entry must be a mapping")
            continue
        edge_id = edge.get("id")
        _check_id(edge_id, label, errors)
        if isinstance(edge_id, str) and edge_id in edge_ids:
            errors.append(f"{label}: duplicate lineage id {edge_id!r}")
        if isinstance(edge_id, str):
            edge_ids.add(edge_id)
        _required(edge, ("id", "from", "to", "relationship", "evidence"), label, errors)
        if edge.get("from") not in asset_ids:
            errors.append(f"{label}: from {edge.get('from')!r} is not in catalog")
        if edge.get("to") not in asset_ids:
            errors.append(f"{label}: to {edge.get('to')!r} is not in catalog")
        _check_path(edge.get("evidence"), f"{label}.evidence", errors)

    contracts = _load(PRODUCT_CONTRACT_PATH, errors)
    contract_products = {
        item.get("id"): item
        for item in contracts.get("products", [])
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    for asset_id, asset in asset_by_id.items():
        product_id = asset.get("contract_product_id")
        if not product_id:
            continue
        product = contract_products.get(product_id)
        if product is None:
            errors.append(f"catalog asset {asset_id}: contract_product_id {product_id!r} is missing from frozen contract")
            continue
        for field in ("schema_version", "repository"):
            if asset.get(field) is not None and asset.get(field) != product.get(field):
                errors.append(f"catalog asset {asset_id}: {field} does not match frozen contract")
        if asset.get("repository") != product.get("repository"):
            errors.append(f"catalog asset {asset_id}: repository does not match frozen contract")

    return errors


def main() -> int:
    print("=" * 70)
    print("Solo Empire — Metadata Catalog Guard")
    print("=" * 70)
    errors = validate()
    if errors:
        for error in errors:
            print(f"  FAIL: {error}")
        print(f"Result: FAIL ({len(errors)} errors)")
        return 1
    catalog = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    dictionary = yaml.safe_load(DICTIONARY_PATH.read_text(encoding="utf-8"))
    lineage = yaml.safe_load(LINEAGE_PATH.read_text(encoding="utf-8"))
    print(f"  ✅ catalog assets: {len(catalog['assets'])}")
    print(f"  ✅ dictionary entries: {len(dictionary['entries'])}")
    print(f"  ✅ dictionary columns: {sum(len(entry['columns']) for entry in dictionary['entries'])}")
    print(f"  ✅ lineage edges: {len(lineage['edges'])}")
    print("  ✅ frozen data-product contract references match")
    print("  ✅ all evidence paths and change records are present")
    print("Result: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
