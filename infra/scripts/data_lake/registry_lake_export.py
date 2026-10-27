#!/usr/bin/env python3
"""Export the Solo Empire registries and metadata catalog into the lake.

Reads two checkouts that stay the source of truth (Git):

* ``solo-empire`` ``repository-catalog/registries`` (data-sources.yaml, online-systems.yaml,
  business-capabilities.yaml);
* ``architecture-docs`` ``systems/ai-data/metadata-catalog`` (catalog.yaml, lineage.yaml).

Each becomes one Bronze dataset under ``domain=metadata`` through the normal ingest
path (landing bytes, Bronze Parquet, manifest), so dbt can join "where data comes
from" with "which lake tables and products it feeds". Only structure is exported:
free-text ``notes`` and change histories stay in Git. Batches are content-addressed,
so re-running on an unchanged registry writes nothing new.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.ingest import ingest_records  # noqa: E402

SOURCE = "solo-empire-registry"
DOMAIN = "metadata"
DATASETS = (
    "data_sources",
    "systems",
    "catalog_assets",
    "lineage_edges",
    "capability_domains",
    "system_capabilities",
)
DEFAULT_LAKE_URI = str(ROOT / "data" / "lake")
CANDIDATE_MARKER = "not confirmed in use"


def _load_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: YAML root must be a mapping")
    return value


def _rows(path: Path, key: str) -> list[dict[str, Any]]:
    rows = _load_yaml(path).get(key) or []
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"{path}: {key} must be a list of mappings")
    seen: set[str] = set()
    for row in rows:
        row_id = row.get("id")
        if not row_id:
            raise ValueError(f"{path}: every {key} entry needs an id")
        if row_id in seen:
            raise ValueError(f"{path}: duplicate id {row_id!r}")
        seen.add(row_id)
    return rows


def _data_source(flow: dict[str, Any]) -> dict[str, Any]:
    destination = flow.get("destination") or {}
    return {
        "id": flow["id"],
        "name": flow.get("name"),
        "channel": flow.get("channel"),
        "data": flow.get("data"),
        "cadence": flow.get("cadence"),
        "automation": flow.get("automation"),
        "destination_system": destination.get("system"),
        "destination_cells": list(destination.get("cells") or []),
        "destination_store": destination.get("store"),
        "repo": flow.get("repo"),
        "catalog_assets": list(flow.get("catalog_assets") or []),
        "probe_count": len(flow.get("probes") or []),
        "candidate": CANDIDATE_MARKER in str(flow.get("notes") or ""),
    }


def _system(system: dict[str, Any]) -> dict[str, Any]:
    repo = system.get("repo") or {}
    host = system.get("host") or {}
    return {
        "id": system["id"],
        "name": system.get("name"),
        "kind": system.get("kind"),
        "status": system.get("status"),
        "phase": None if system.get("phase") is None else str(system.get("phase")),
        "repo": repo.get("name") if isinstance(repo, dict) else repo,
        "host_provider": host.get("provider") if isinstance(host, dict) else None,
        "host_mode": host.get("mode") if isinstance(host, dict) else None,
    }


def _asset(asset: dict[str, Any]) -> dict[str, Any]:
    keys = ("id", "kind", "name", "owner", "status", "data_classification", "visibility", "source_path")
    return {key: asset.get(key) for key in keys}


def _edge(edge: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": edge["id"],
        "from_asset": edge.get("from"),
        "to_asset": edge.get("to"),
        "relationship": edge.get("relationship"),
        "evidence": edge.get("evidence"),
    }


def _domain(domain: dict[str, Any]) -> dict[str, Any]:
    return {key: domain.get(key) for key in ("id", "number", "name")}


def _system_capabilities(path: Path) -> list[dict[str, Any]]:
    """One row per (system, domain); rank 1 is the system's primary domain."""
    mapping = _load_yaml(path).get("systems") or {}
    if not isinstance(mapping, dict):
        raise ValueError(f"{path}: systems must map system id -> list of domain ids")
    rows: list[dict[str, Any]] = []
    for system_id, domains in mapping.items():
        if not isinstance(domains, list) or not domains:
            raise ValueError(f"{path}: systems.{system_id} needs a non-empty list of domains")
        for rank, domain_id in enumerate(domains, start=1):
            rows.append({
                "id": f"{system_id}:{domain_id}",
                "system_id": system_id,
                "domain_id": domain_id,
                "rank": rank,
                "is_primary": rank == 1,
            })
    return rows


def load_records(registries: Path, catalog: Path) -> dict[str, list[dict[str, Any]]]:
    """Return the export rows per dataset; raises ValueError on a malformed source."""
    return {
        "data_sources": [_data_source(row) for row in _rows(registries / "data-sources.yaml", "flows")],
        "systems": [_system(row) for row in _rows(registries / "online-systems.yaml", "systems")],
        "catalog_assets": [_asset(row) for row in _rows(catalog / "catalog.yaml", "assets")],
        "lineage_edges": [_edge(row) for row in _rows(catalog / "lineage.yaml", "edges")],
        "capability_domains": [_domain(row) for row in _rows(registries / "business-capabilities.yaml", "domains")],
        "system_capabilities": _system_capabilities(registries / "business-capabilities.yaml"),
    }


def export(
    registries: Path,
    catalog: Path,
    data_lake_uri: str,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    records = load_records(registries, catalog)
    if dry_run:
        return {
            "status": "dry-run",
            "datasets": {name: {"records": len(rows)} for name, rows in records.items()},
        }
    # Constant metadata: ingest refuses an existing batch whose metadata differs, and the
    # batch id is the content hash, so anything per-run here (a commit sha) would break
    # re-exports of unchanged files.
    metadata = {"provider": SOURCE}
    results: dict[str, Any] = {}
    for name, rows in records.items():
        result = ingest_records(
            rows,
            source=SOURCE,
            domain=DOMAIN,
            dataset=name,
            data_lake_uri=data_lake_uri,
            source_record_id_field="id",
            metadata=metadata,
            privacy_class="internal",
            retention_class="operational",
        )
        results[name] = {
            "records": len(rows),
            "manifest_key": result["manifest_key"],
            "new": not result["manifest_existed"],
        }
    return {"status": "exported", "datasets": results}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--registries", type=Path, required=True,
                        help="solo-empire repository-catalog/registries directory")
    parser.add_argument("--catalog", type=Path, required=True,
                        help="architecture-docs systems/ai-data/metadata-catalog directory")
    parser.add_argument("--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_LAKE_URI))
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = export(args.registries, args.catalog, args.data_lake_uri, dry_run=args.dry_run)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
