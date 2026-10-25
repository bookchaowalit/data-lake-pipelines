"""Shared lake-first adapter for Solo Empire data products.

Market data products (crypto, stock, fx, …) must not invent per-repo ingest
paths. They share this adapter:

    exact source bytes → landing → Bronze Parquet → manifest
    → DuckDB API projection
    → optional local CSV (CLI only)

PostgreSQL/ClickHouse stay multi-engine lab projections only.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence
from urllib.parse import unquote, urlparse

from data_lake.storage import ObjectNotFound, ObjectStore, StorageError


class LakeUnavailable(RuntimeError):
    """Raised when the Solo Empire data-lake runtime cannot be loaded."""


class LakeIngestError(RuntimeError):
    """Raised when lake ingest fails after the runtime loaded."""


@dataclass(frozen=True)
class LakeProductContract:
    """Per-product lake registration (not product HTTP envelope schema)."""

    source: str
    domain: str
    product_schema_version: str
    privacy_class: str = "public"
    retention_class: str = "operational"
    bronze_schema_version: str = "1"
    project_root: Optional[Path] = None
    data_lake_uri: str = ""
    lineage_filename: str = "lake_lineage.json"
    datasets: tuple[str, ...] = field(default_factory=tuple)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def find_lake_runtime_root(start: Optional[Path] = None) -> Optional[Path]:
    """Return the data-lake runtime root (this repository, or a checkout above ``start``)."""
    cur = (start or Path.cwd()).resolve()
    for parent in [cur, *cur.parents]:
        if (parent / "infra" / "scripts" / "data_lake" / "ingest.py").is_file():
            return parent
    own_root = Path(__file__).resolve().parents[3]
    if (own_root / "infra" / "scripts" / "data_lake" / "ingest.py").is_file():
        return own_root
    return None


def default_data_lake_uri(
    *,
    data_lake_uri: str = "",
    project_root: Optional[Path] = None,
) -> str:
    explicit = (
        data_lake_uri
        or os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI")
        or os.environ.get("DATA_LAKE_URI")
        or ""
    ).strip()
    if explicit:
        return explicit
    root = find_lake_runtime_root(project_root)
    if root is not None:
        return str((root / "data" / "lake").resolve())
    base = project_root or Path.cwd()
    return str((base / "data" / "lake").resolve())


def _ensure_data_lake_on_path(solo_root: Path) -> None:
    scripts = str(solo_root / "infra" / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)


def load_ingest_runtime(start: Optional[Path] = None):
    solo_root = find_lake_runtime_root(start)
    if solo_root is None:
        raise LakeUnavailable(
            "Cannot find the data-lake runtime (infra/scripts/data_lake). "
            "Run from a data-lake checkout."
        )
    _ensure_data_lake_on_path(solo_root)
    try:
        from data_lake.ingest import ingest_payload  # type: ignore
        from data_lake.parquet import ParquetUnavailable  # type: ignore
        from data_lake.storage import StorageError  # type: ignore
    except ImportError as exc:
        raise LakeUnavailable(
            "data_lake package is not importable. Ensure infra/scripts is on "
            "PYTHONPATH and dependencies from infra/requirements-data-lake.txt "
            "are installed (at least pyarrow)."
        ) from exc
    return ingest_payload, ParquetUnavailable, StorageError, solo_root


def load_query_runtime(start: Optional[Path] = None):
    solo_root = find_lake_runtime_root(start)
    if solo_root is None:
        raise LakeUnavailable(
            "Cannot find the data-lake runtime (infra/scripts/data_lake). "
            "Run from a data-lake checkout."
        )
    _ensure_data_lake_on_path(solo_root)
    try:
        from data_lake.query import DuckDBUnavailable, query_parquet  # type: ignore
    except ImportError as exc:
        raise LakeUnavailable(
            "data_lake.query is not importable. Ensure DuckDB is installed "
            "(infra/requirements-data-lake.txt)."
        ) from exc
    return query_parquet, DuckDBUnavailable, solo_root


def load_iceberg_query_runtime(start: Optional[Path] = None):
    solo_root = find_lake_runtime_root(start)
    if solo_root is None:
        raise LakeUnavailable(
            "Cannot find the data-lake runtime (infra/scripts/data_lake). "
            "Run from a data-lake checkout."
        )
    _ensure_data_lake_on_path(solo_root)
    try:
        from data_lake.iceberg import (  # type: ignore
            IcebergUnavailable,
            iceberg_table_location,
        )
        from data_lake.query import DuckDBUnavailable, query_iceberg  # type: ignore
        from data_lake.storage import StorageError  # type: ignore
    except ImportError as exc:
        raise LakeUnavailable(
            "Iceberg query runtime is not importable. Install the optional "
            "PyIceberg/DuckDB dependencies from infra/requirements-data-lake.txt."
        ) from exc
    return (
        iceberg_table_location,
        query_iceberg,
        IcebergUnavailable,
        DuckDBUnavailable,
        StorageError,
        solo_root,
    )


def resolve_lake_root(data_lake_uri: str) -> Path:
    uri = data_lake_uri.strip()
    if uri.startswith("s3://"):
        raise LakeUnavailable(
            "This operation requires a local lake path; use read_bronze_rows() "
            "for direct s3:// Bronze reads"
        )
    if uri.startswith("file://"):
        parsed = urlparse(uri)
        path = unquote(parsed.path)
        if parsed.netloc and parsed.netloc not in {"", "localhost"}:
            path = f"/{parsed.netloc}{path}"
        return Path(path or ".").expanduser().resolve()
    return Path(uri).expanduser().resolve()


def is_remote_lake_uri(data_lake_uri: str) -> bool:
    """Return whether a lake URI is an S3-compatible object-store location."""
    parsed = urlparse((data_lake_uri or "").strip())
    return parsed.scheme.lower() == "s3" and bool(parsed.netloc)


def remote_bronze_dataset_glob(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> str:
    """Build a bounded DuckDB glob for Bronze parts in an S3-compatible lake.

    The ingest contract writes one partition below ``ingest_date`` and
    ``source``. Restricting the glob to those known partition levels avoids
    scanning landing, manifests, or another dataset.
    """
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
    ).strip()
    if not is_remote_lake_uri(uri):
        raise LakeUnavailable("remote_bronze_dataset_glob requires an s3:// lake URI")
    return (
        f"{uri.rstrip('/')}/bronze/domain={contract.domain}/dataset={dataset}/"
        f"schema_version={contract.bronze_schema_version}/ingest_date=*/"
        "source=*/part-*.parquet"
    )


def bronze_dataset_dir(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> Path:
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
    )
    root = resolve_lake_root(uri)
    return (
        root
        / "bronze"
        / f"domain={contract.domain}"
        / f"dataset={dataset}"
        / f"schema_version={contract.bronze_schema_version}"
    )


def list_bronze_parquet_files(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
) -> list[Path]:
    base = bronze_dataset_dir(contract, dataset, data_lake_uri=data_lake_uri)
    if not base.exists():
        return []
    return sorted(path for path in base.rglob("*.parquet") if path.is_file())


# Each remote read starts one `aws` process (data_lake/storage.py), about a second apiece, and
# every ingest adds a manifest. Reading them in parallel and caching them keeps a remote read
# from growing one process per ingest. Manifests are write-once, so a cached body never goes stale.
MANIFEST_READ_WORKERS = 16
_MANIFEST_CACHE: dict[tuple[str, str], bytes] = {}
_MANIFEST_CACHE_LOCK = threading.Lock()


def _read_manifests(store: ObjectStore, lake_uri: str, keys: Sequence[str]) -> list[tuple[str, bytes | Exception]]:
    """Read manifest bodies in listing order; a failed read is returned as its exception."""
    remote = is_remote_lake_uri(lake_uri)

    def read(key: str) -> bytes | Exception:
        if remote:
            with _MANIFEST_CACHE_LOCK:
                cached = _MANIFEST_CACHE.get((lake_uri, key))
            if cached is not None:
                return cached
        try:
            body = store.get_bytes(key)
        except (ObjectNotFound, StorageError) as exc:
            return exc
        if remote:
            with _MANIFEST_CACHE_LOCK:
                _MANIFEST_CACHE[(lake_uri, key)] = body
        return body

    if not remote or len(keys) < 2:
        return [(key, read(key)) for key in keys]
    with ThreadPoolExecutor(max_workers=MANIFEST_READ_WORKERS) as pool:
        return list(zip(keys, pool.map(read, keys)))


def _committed_bronze_keys(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: str,
) -> set[str]:
    """Return Bronze parts referenced by a complete ingest manifest.

    A Parquet part can exist after a worker crashes before its manifest is
    written.  Treating the final manifest as the lake commit marker prevents
    readers from serving those partial batches.
    """
    store = ObjectStore(data_lake_uri)
    manifest_prefixes = (
        f"control/manifests/source={contract.source}",
        f"control/manifests/stream-parquet/source={contract.source}",
    )
    expected_prefix = (
        f"bronze/domain={contract.domain}/dataset={dataset}/"
        f"schema_version={contract.bronze_schema_version}/"
    )
    committed: set[str] = set()
    manifest_keys: list[str] = []
    for manifest_prefix in manifest_prefixes:
        try:
            manifest_keys.extend(store.list_keys(manifest_prefix))
        except StorageError as exc:
            raise LakeIngestError(f"Cannot list Bronze commit manifests for {dataset}") from exc
    json_keys = [key for key in manifest_keys if key.endswith(".json")]
    for manifest_key, body in _read_manifests(store, data_lake_uri, json_keys):
        try:
            if isinstance(body, Exception):
                raise body
            manifest = json.loads(body.decode("utf-8"))
        except ObjectNotFound:
            # A concurrent cleanup or eventually consistent listing is not a
            # commit; simply leave this batch out of the read set.
            continue
        except (StorageError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LakeIngestError(f"Invalid Bronze commit manifest for {dataset}") from exc
        if not isinstance(manifest, dict):
            raise LakeIngestError(f"Invalid Bronze commit manifest shape for {dataset}")
        for manifest_field, expected in {
            "source": contract.source,
            "domain": contract.domain,
            "schema_version": contract.bronze_schema_version,
        }.items():
            if manifest.get(manifest_field) != expected:
                raise LakeIngestError(
                    f"Bronze commit manifest identity does not match {dataset}: {manifest_field}"
                )
        # A source commonly owns more than one dataset (for example prices
        # and history).  Ignore its valid manifests for the other datasets;
        # only the requested dataset contributes committed parts.
        if manifest.get("dataset") != dataset:
            continue
        bronze = manifest.get("bronze")
        key = bronze.get("key") if isinstance(bronze, dict) else None
        if isinstance(key, str) and key.startswith(expected_prefix) and key.endswith(".parquet"):
            committed.add(key)
    return committed


def ingest_to_lake(
    contract: LakeProductContract,
    *,
    raw: bytes,
    records: list[dict[str, Any]],
    dataset: str,
    data_lake_uri: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    content_type: str = "application/json",
    input_format: str = "json",
    provider: str = "",
) -> dict[str, Any]:
    """Persist exact source bytes + Bronze rows. No local projection."""
    if not records:
        raise LakeIngestError("Refusing lake ingest with zero records")

    ingest_payload, ParquetUnavailable, StorageError, solo_root = load_ingest_runtime(
        contract.project_root,
    )
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
    )
    meta = {
        "adapter": contract.source,
        "product_schema_version": contract.product_schema_version,
    }
    if provider:
        meta["provider"] = provider
    if metadata:
        meta.update(metadata)

    try:
        result = ingest_payload(
            raw,
            records,
            source=contract.source,
            domain=contract.domain,
            dataset=dataset,
            data_lake_uri=uri,
            input_format=input_format,
            schema_version=contract.bronze_schema_version,
            source_record_id_field="id",
            event_time_field="event_time",
            content_type=content_type,
            metadata=meta,
            privacy_class=contract.privacy_class,
            retention_class=contract.retention_class,
        )
    except ParquetUnavailable as exc:
        raise LakeUnavailable(str(exc)) from exc
    except StorageError as exc:
        raise LakeIngestError(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise LakeIngestError(f"Lake ingest failed: {exc}") from exc
    return result


def write_lineage(
    contract: LakeProductContract,
    result: dict[str, Any],
    *,
    dataset: str,
    data_dir: Path,
) -> Path:
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / contract.lineage_filename
    previous: dict[str, Any] = {}
    if path.exists():
        try:
            previous = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            previous = {}

    datasets: dict[str, Any] = previous["datasets"] if isinstance(previous.get("datasets"), dict) else {}
    datasets[dataset] = {
        "run_id": result.get("run_id"),
        "record_count": result.get("record_count"),
        "raw_key": result.get("raw_key"),
        "bronze_key": result.get("bronze_key"),
        "manifest_key": result.get("manifest_key"),
        "data_lake": result.get("data_lake"),
        "ingested_at": utc_now_iso(),
    }
    body = {
        "source": contract.source,
        "domain": contract.domain,
        "schema_version": contract.bronze_schema_version,
        "product_schema_version": contract.product_schema_version,
        "updated_at": utc_now_iso(),
        "datasets": datasets,
    }
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def load_lineage(data_dir: Path, *, filename: str = "lake_lineage.json") -> Optional[dict[str, Any]]:
    path = Path(data_dir) / filename
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def read_bronze_rows(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
    require_committed: bool = True,
) -> list[dict[str, Any]]:
    uri = data_lake_uri or default_data_lake_uri(
        data_lake_uri=contract.data_lake_uri,
        project_root=contract.project_root,
    )
    remote = is_remote_lake_uri(uri)
    committed_keys = (
        _committed_bronze_keys(contract, dataset, data_lake_uri=uri)
        if require_committed
        else None
    )
    if remote:
        if committed_keys is not None:
            paths: str | list[str] = [f"{uri.rstrip('/')}/{key}" for key in sorted(committed_keys)]
            if not paths:
                return []
        else:
            paths = remote_bronze_dataset_glob(contract, dataset, data_lake_uri=uri)
    else:
        files = list_bronze_parquet_files(contract, dataset, data_lake_uri=uri)
        if committed_keys is not None:
            lake_root = resolve_lake_root(uri)
            files = [
                path for path in files
                if path.relative_to(lake_root).as_posix() in committed_keys
            ]
        if not files:
            return []
        paths = [str(path) for path in files]

    query_parquet, DuckDBUnavailable, _solo = load_query_runtime(
        contract.project_root,
    )
    statement = sql or (
        "SELECT event_id, source, source_record_id, domain, dataset, "
        "schema_version, received_at, event_time, payload_json, "
        "metadata_json, ingest_run_id, raw_object_key, raw_sha256, "
        "privacy_class, retention_class "
        "FROM lake_table "
        "ORDER BY event_time NULLS LAST, received_at NULLS LAST, event_id"
    )
    try:
        return query_parquet(paths, statement)
    except DuckDBUnavailable as exc:
        raise LakeUnavailable(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise LakeIngestError(f"Bronze DuckDB query failed for {dataset}: {exc}") from exc


def read_iceberg_rows(
    contract: LakeProductContract,
    dataset: str,
    *,
    data_lake_uri: Optional[str] = None,
    sql: str | None = None,
) -> list[dict[str, Any]]:
    """Read a registered Bronze table through the Iceberg catalog + DuckDB.

    ``data_lake_uri`` remains part of the product contract for parity with the
    Parquet path, but the REST catalog owns the actual table location. The
    catalog URI, warehouse, and token are read only from the runtime secret
    provider and are never written to a product response or manifest.
    """
    del data_lake_uri
    catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "").strip()
    warehouse_uri = os.environ.get("ICEBERG_WAREHOUSE_URI", "").strip()
    if not catalog_uri or not warehouse_uri:
        raise LakeUnavailable(
            "Iceberg read mode requires ICEBERG_CATALOG_URI and "
            "ICEBERG_WAREHOUSE_URI in the runtime secret provider"
        )

    (
        iceberg_table_location,
        query_iceberg,
        IcebergUnavailable,
        DuckDBUnavailable,
        StorageError,
        _solo,
    ) = load_iceberg_query_runtime(
        contract.project_root,
    )
    statement = sql or (
        "SELECT event_id, source, source_record_id, domain, dataset, "
        "schema_version, received_at, event_time, payload_json, "
        "metadata_json, ingest_run_id, raw_object_key "
        "FROM lake_table "
        "ORDER BY event_time NULLS LAST, received_at NULLS LAST, event_id"
    )
    try:
        resolved = iceberg_table_location(
            catalog_uri=catalog_uri,
            warehouse_uri=warehouse_uri,
            domain=contract.domain,
            dataset=dataset,
        )
        return query_iceberg(
            resolved["table_location"],
            statement,
            metadata_location=resolved.get("metadata_location"),
        )
    except (IcebergUnavailable, DuckDBUnavailable, StorageError) as exc:
        raise LakeUnavailable(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise LakeIngestError(
            f"Iceberg DuckDB query failed for {dataset}: {exc}"
        ) from exc


def parse_payload_json(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("payload_json")
    if raw is None or raw == "":
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(str(raw))
    except (json.JSONDecodeError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def select_latest_bronze_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    def temporal_rank(value: Any) -> tuple[int, float, str]:
        """Compare ISO timestamps by instant, including mixed UTC offsets."""
        raw = str(value or "").strip()
        if not raw:
            return (0, float("-inf"), "")
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return (1, parsed.astimezone(timezone.utc).timestamp(), "")
        except (TypeError, ValueError, OverflowError):
            # Invalid source timestamps sort after no timestamp only by their
            # deterministic raw value; quality gates can still quarantine them.
            return (0, float("-inf"), raw)

    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("source_record_id") or "").strip()
        if not key:
            payload = parse_payload_json(row)
            key = str(payload.get("id") or "").strip()
        if not key:
            key = str(row.get("event_id") or "")
        previous = best.get(key)
        if previous is None:
            best[key] = row
            continue
        prev_rank = (
            temporal_rank(previous.get("event_time")),
            temporal_rank(previous.get("received_at")),
            str(previous.get("event_id") or ""),
        )
        cur_rank = (
            temporal_rank(row.get("event_time")),
            temporal_rank(row.get("received_at")),
            str(row.get("event_id") or ""),
        )
        if cur_rank >= prev_rank:
            best[key] = row
    return [best[key] for key in sorted(best)]


def landing_object_bytes(data_lake_uri: str, raw_key: str) -> bytes:
    """Replay exact landing bytes for a raw_object_key (offline proof)."""
    ingest_payload, ParquetUnavailable, StorageError, solo_root = load_ingest_runtime()
    del ingest_payload, ParquetUnavailable  # import side-effect only
    _ensure_data_lake_on_path(solo_root)
    from data_lake.storage import ObjectStore  # type: ignore

    try:
        return ObjectStore(data_lake_uri).get_bytes(raw_key)
    except StorageError as exc:
        raise LakeIngestError(str(exc)) from exc
