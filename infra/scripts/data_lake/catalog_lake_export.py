#!/usr/bin/env python3
"""Export the platform catalog (platform-empire SPEC-002) into the lake.

Rollout step 5: the owner's taxonomy (``graph.concepts``: domains, values) and metric definitions
(``graph.metric_definitions``) become two Bronze datasets under ``domain=metadata``, so Gold marts can
join measured values with their definitions. The catalog stays the source of truth in platform-empire;
this reads it through tasks-api (``GET /v1/catalog/concepts`` and ``/v1/catalog/metrics``) with the
``catalog-export`` automation token, which holds only ``catalog.concept:read`` and
``catalog.metric-definition:read``.

Only ``public`` and ``internal`` metric definitions are exported (SPEC-002 D7b). The API already hides
``confidential`` and ``restricted`` definitions from a token without the source cell's read permission;
the filter here keeps that rule even if the token is ever widened. Batches are content-addressed, so an
unchanged catalog writes nothing new.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.ingest import ingest_records  # noqa: E402

SOURCE = "empire-platform-catalog"
DOMAIN = "metadata"
DATASETS = ("catalog_concepts", "catalog_metric_definitions")
EXPORTABLE = frozenset({"public", "internal"})
USER_AGENT = "catalog-export/1"

CONCEPT_FIELDS = (
    "urn", "kind", "slug", "label", "description", "parent", "status", "replaced_by",
    "introduced_in", "retired_in", "was", "updated_at",
)
METRIC_FIELDS = (
    "urn", "key", "label", "description", "domain", "domain_urn", "unit", "scale", "value_range",
    "kind", "objectivity", "source", "aggregation", "cadence", "criteria", "target",
    "classification", "status", "replaced_by", "version", "updated_at",
)

# get(url, token) -> parsed JSON body; raises CatalogError on a transport or HTTP failure
Get = Callable[[str, str], Any]


class CatalogError(RuntimeError):
    pass


def http_get(url: str, token: str) -> Any:
    try:
        response = httpx.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": USER_AGENT},
            timeout=30,
        )
    except httpx.HTTPError as error:
        raise CatalogError(f"{type(error).__name__} for {url.rsplit('/', 1)[-1]}") from None
    if response.status_code != 200:
        raise CatalogError(f"HTTP {response.status_code} for {url.rsplit('/', 1)[-1]}")
    return response.json()


def _items(body: Any, what: str) -> list[dict[str, Any]]:
    items = body.get("items") if isinstance(body, dict) else None
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        raise CatalogError(f"{what}: expected an object with an items list")
    return items


def _pick(item: dict[str, Any], fields: tuple[str, ...], what: str) -> dict[str, Any]:
    if not item.get("urn"):
        raise CatalogError(f"{what}: an item has no urn")
    # read_permission is left out: it names an internal permission, not part of the definition
    return {"id": item["urn"], **{name: item.get(name) for name in fields}}


def load_records(base_url: str, token: str, get: Get = http_get) -> dict[str, list[dict[str, Any]]]:
    """Return the export rows per dataset; raises CatalogError on a failed or malformed read."""
    base = base_url.rstrip("/")
    concepts = _items(get(f"{base}/v1/catalog/concepts", token), "concepts")
    metrics = _items(get(f"{base}/v1/catalog/metrics", token), "metrics")
    return {
        "catalog_concepts": sorted(
            (_pick(item, CONCEPT_FIELDS, "concepts") for item in concepts), key=lambda row: row["id"]
        ),
        "catalog_metric_definitions": sorted(
            (
                _pick(item, METRIC_FIELDS, "metrics")
                for item in metrics
                if item.get("classification") in EXPORTABLE
            ),
            key=lambda row: row["id"],
        ),
    }


def export(
    env: Mapping[str, str],
    data_lake_uri: str,
    *,
    dry_run: bool = False,
    get: Get = http_get,
) -> dict[str, Any]:
    """Export when EMPIRE_TASKS_API and CATALOG_EXPORT_TOKEN are both set; otherwise report skipped."""
    base_url = env.get("EMPIRE_TASKS_API", "").strip()
    token = env.get("CATALOG_EXPORT_TOKEN", "").strip()
    if not base_url and not token:
        return {"status": "skipped"}
    if not base_url or not token:
        raise CatalogError("set both EMPIRE_TASKS_API and CATALOG_EXPORT_TOKEN")
    records = load_records(base_url, token, get)
    if dry_run:
        return {"status": "dry-run", "datasets": {name: {"records": len(rows)} for name, rows in records.items()}}
    results: dict[str, Any] = {}
    for name, rows in records.items():
        if not rows:  # an empty batch has nothing to land; the next non-empty export writes it
            results[name] = {"records": 0, "new": False}
            continue
        result = ingest_records(
            rows,
            source=SOURCE,
            domain=DOMAIN,
            dataset=name,
            data_lake_uri=data_lake_uri,
            source_record_id_field="id",
            metadata={"provider": SOURCE},
            privacy_class="internal",
            retention_class="operational",
        )
        results[name] = {"records": len(rows), "manifest_key": result["manifest_key"], "new": not result["manifest_existed"]}
    return {"status": "exported", "datasets": results}
