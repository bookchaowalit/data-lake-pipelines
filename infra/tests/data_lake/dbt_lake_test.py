#!/usr/bin/env python3
"""
dbt-duckdb project smoke test.

infra/dbt/ reads the real local Bronze lake (data/lake), which is real
personal data that is intentionally never committed to git, so it never
exists in a clean checkout or CI. This test proves the dbt project itself
(models, manifest-aware input, schema tests, and config) is structurally
correct by pointing it at a small synthetic Bronze fixture instead.

Usage:
    python3 infra/tests/data_lake/dbt_lake_test.py

Exit code 0 = pass, 1 = failure.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = ROOT / "infra" / "dbt"


def resolve_dbt_bin() -> Path | None:
    on_path = shutil.which("dbt")
    if on_path:
        return Path(on_path)
    for venv_dbt in (
        ROOT / ".venv" / "bin" / "dbt",
        ROOT / ".venv" / "Scripts" / "dbt.exe",
        ROOT / "infra" / ".venv-datalake" / "bin" / "dbt",
    ):
        if venv_dbt.exists():
            return venv_dbt
    return None


DBT_BIN = resolve_dbt_bin()

sys.path.insert(0, str(ROOT / "infra" / "scripts"))
REGISTRY_FIXTURES = ROOT / "infra" / "tests" / "fixtures" / "registry_lake_export"
METADATA_MODELS = (
    "stg_meta_data_sources",
    "stg_meta_systems",
    "stg_meta_catalog_assets",
    "stg_meta_lineage_edges",
    "meta_lake_datasets",
    "meta_source_lineage",
    "meta_source_coverage",
    "stg_meta_capability_domains",
    "stg_meta_system_capabilities",
    "meta_system_landscape",
    "meta_capability_coverage",
    "stg_meta_value_streams",
    "stg_meta_value_stream_stages",
    "meta_value_stream_stage_systems",
    "meta_value_stream_coverage",
)


def export_registry_fixture(lake_root: Path) -> None:
    from data_lake.registry_lake_export import export

    export(
        REGISTRY_FIXTURES / "registries",
        REGISTRY_FIXTURES / "metadata-catalog",
        lake_root.as_uri(),
    )


def check_metadata_outputs(target_dir: Path) -> list[str]:
    """Problems with the metadata models' results and the generated dbt docs."""
    import duckdb

    problems: list[str] = []
    manifest = json.loads((target_dir / "manifest.json").read_text(encoding="utf-8"))
    built = {node["name"] for node in manifest["nodes"].values() if node["resource_type"] == "model"}
    problems += [
        f"model {name} missing from dbt docs manifest"
        for name in (*METADATA_MODELS, *SALES_MODELS)
        if name not in built
    ]
    if not (target_dir / "catalog.json").is_file():
        problems.append("dbt docs generate wrote no catalog.json")

    with duckdb.connect(str(DBT_PROJECT_DIR / "local.duckdb"), read_only=True) as con:
        rows = con.execute(
            "select source_id, automation, system_registered, catalog_assets, consumers "
            "from meta_source_coverage order by source_id"
        ).fetchall()
        expected = [
            ("bank-accounts", "not-captured", False, 0, 0),
            ("health-checkin", "automated", False, 0, 0),
            ("job-capture", "automated", True, 1, 1),
        ]
        if rows != expected:
            problems.append(f"meta_source_coverage: expected {expected}, got {rows}")
        datasets = con.execute(
            "select count(*) from meta_lake_datasets where domain = 'metadata'"
        ).fetchone()
        if datasets is None or datasets[0] != 8:
            problems.append(f"meta_lake_datasets: expected 8 metadata datasets, got {datasets}")
        landscape = con.execute(
            "select system_id, primary_domain, domains, automated_flows, catalog_assets, consumers "
            "from meta_system_landscape order by system_id"
        ).fetchall()
        expected_landscape = [
            ("book-job-data", "market-intelligence", ["market-intelligence", "data-knowledge"], 1, 2, 1),
            ("data-lake", "data-knowledge", ["data-knowledge"], 0, 0, 0),
        ]
        if landscape != expected_landscape:
            problems.append(f"meta_system_landscape: expected {expected_landscape}, got {landscape}")
        coverage = con.execute(
            "select domain_id, systems, running_systems, platform_coverage "
            "from meta_capability_coverage order by domain_number"
        ).fetchall()
        expected_coverage = [
            ("market-intelligence", 1, 1, "served"),
            ("offer-product", 0, 0, "missing"),
            ("data-knowledge", 2, 2, "served"),
        ]
        if coverage != expected_coverage:
            problems.append(f"meta_capability_coverage: expected {expected_coverage}, got {coverage}")
        streams = con.execute(
            "select value_stream_id, stages, served_stages, broken_stages, flow_status "
            "from meta_value_stream_coverage order by value_stream_number"
        ).fetchall()
        expected_streams = [
            ("lead-to-customer", 2, 1, ["Send proposal"], "partial"),
            ("record-to-report", 1, 1, [], "flowing"),
        ]
        if streams != expected_streams:
            problems.append(f"meta_value_stream_coverage: expected {expected_streams}, got {streams}")
    return problems


def build_synthetic_bronze(root: Path) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("pyarrow required for this test") from exc

    now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(5):
        received = now - timedelta(hours=i * 6)
        rows.append(
            {
                "event_id": f"fixture-event-{i}",
                "source": "fixture",
                "source_record_id": f"msg-{i}",
                "domain": "communications",
                "dataset": "emails",
                "schema_version": "1",
                "received_at": received.isoformat(),
                "event_time": received.isoformat(),
                "content_type": "application/json",
                "raw_object_key": f"fixture/msg-{i}.json",
                "raw_sha256": "0" * 64,
                "payload_json": json.dumps(
                    {
                        "id": f"msg-{i}",
                        "thread_id": f"thread-{i % 2}",
                        "from_address": "fixture@example.invalid",
                        "from_name": "Fixture Sender",
                        "subject": "Synthetic fixture - not real content",
                        "snippet": "synthetic",
                        "to_addresses": ["nobody@example.invalid"],
                        "is_read": i % 2 == 0,
                        "is_important": i == 0,
                        "has_attachment": i % 3 == 0,
                        "labels": ["INBOX"],
                        "source": "fixture",
                        "received_at": received.isoformat(),
                    }
                ),
                "metadata_json": "{}",
                "ingest_run_id": "fixture-run",
                "privacy_class": "private",
                "retention_class": "operational",
                "ingest_date": received.date().isoformat(),
            }
        )

    table = pa.Table.from_pylist(rows)
    part_dir = (
        root
        / "domain=communications"
        / "dataset=emails"
        / "schema_version=1"
        / f"ingest_date={now.date().isoformat()}"
        / "source=fixture"
    )
    part_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = part_dir / "part-fixture.parquet"
    pq.write_table(table, parquet_path)

    bronze_key = parquet_path.relative_to(root.parent).as_posix()
    manifest_path = (
        root.parent
        / "control"
        / "manifests"
        / "source=fixture"
        / "batch_id=fixture.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "manifest_version": "1",
                "source": "fixture",
                "batch_id": "fixture",
                "domain": "communications",
                "dataset": "emails",
                "schema_version": "1",
                "record_count": len(rows),
                "bronze": {
                    "key": bronze_key,
                    "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def build_jobs_silver_fixture(lake_root: Path) -> Path:
    """One small jobs Silver part, built by the real Bronze → Silver code."""
    from data_lake.ingest import ingest_payload
    from data_lake.silver import transform_bronze_to_silver

    ingest_payload(
        b"dbt-jobs-fixture",
        [
            {
                "id": f"job-{i}",
                "title": f"Engineer {i}",
                "company": "Fixture Co",
                "url": f"https://example.invalid/jobs/{i}",
                "event_time": "2026-09-30T12:00:00Z",
                "posted": "2026-09-29",
                "provider": "book-job-scraping",
                "source": "Fixture Board",
                "tags": "Python, SQL",
            }
            for i in range(3)
        ],
        source="book-job-data",
        domain="jobs",
        dataset="job_postings",
        data_lake_uri=str(lake_root),
        input_format="text",
        event_time_field="event_time",
        privacy_class="internal",
    )
    result = transform_bronze_to_silver(data_lake_uri=str(lake_root))
    return lake_root / result["silver_key"]


def build_news_silver_fixture(lake_root: Path) -> Path:
    """One small news Silver part, built by the real Bronze → Silver code (docs/specs/news-marts.md)."""
    from data_lake.ingest import ingest_payload
    from data_lake.news_marts import news_silver_contract
    from data_lake.silver import transform_bronze_to_silver

    ingest_payload(
        b"dbt-news-fixture",
        [
            {
                "id": f"Fixture News|{i}",
                "source": "Fixture News",
                "article_id": str(i),
                "headline": f"Fixture headline {i}",
                "canonical_url": f"https://example.invalid/news/{i}",
                "published_at": "2026-09-30T03:00:00Z",
                "captured_at": "2026-09-30T04:00:00Z",
            }
            for i in range(3)
        ],
        source="book-news-scraping",
        domain="news",
        dataset="articles",
        data_lake_uri=str(lake_root),
        input_format="text",
        event_time_field="captured_at",
        privacy_class="public",
    )
    result = transform_bronze_to_silver(news_silver_contract(), data_lake_uri=str(lake_root))
    return lake_root / result["silver_key"]


def build_synthetic_x_feed(lake_root: Path) -> None:
    """Twenty fake X sightings through the real lake writer, plus a re-export with one changed row."""
    sys.path.insert(0, str(ROOT / "infra" / "scripts" / "utils"))
    sys.path.insert(0, str(ROOT / "infra" / "scripts"))
    sys.path.insert(0, str(ROOT / "infra" / "tests" / "fixtures"))
    from data_lake.ingest import ingest_records
    from x_feed_rows import x_feed_rows

    rows = x_feed_rows("test-pull-dbt")
    for batch in (rows, [{**rows[0], "likes": 999}, *rows[1:]]):
        ingest_records(
            batch,
            source="x-feed",
            domain="social",
            dataset="x_feed",
            data_lake_uri=str(lake_root),
            source_record_id_field="sighting_key",
            event_time_field="collected_at",
            metadata={"adapter": "dbt_lake_test", "contract": "x_feed.v1"},
        )


SALES_MODELS = (
    "stg_commerce_events",
    "sales_rejected_events",
    "dim_customer",
    "fact_sales",
    "sales_daily_revenue",
)


def _envelope(event_id: str, kind: str, occurred_at: str, **data: object) -> dict:
    return {
        "id": event_id,
        "type": kind,
        "version": 1,
        "occurred_at": occurred_at,
        "producer": "platform-payment",
        "correlation_id": str(data.get("intent_id") or event_id),
        "causation_id": None,
        "data": data,
    }


def build_synthetic_commerce_events(lake_root: Path) -> None:
    """Fake payment events through the real stream sink + converter (docs/specs/sales-marts.md).

    Customer ws-a: 60k + 50k baht -> VIP, then a 30k refund -> standard again.
    The 50k sale is delivered twice. Customer ws-b: a USD sale that arrives 3 days late.
    Three bad events (zero amount, missing intent_id, refund of an unknown intent).
    """
    from data_lake.stream_sink import StreamMessage, convert_avro_file, sink_messages

    s1 = _envelope("payment.intent.succeeded:s1", "payment.intent.succeeded", "2026-11-01T03:00:00Z",
                   workspace_id="ws-a", intent_id="s1", amount=6_000_000, currency="THB", provider="stripe")
    # 18:30 UTC on 11-01 is 01:30 on 11-02 in Bangkok.
    s2 = _envelope("payment.intent.succeeded:s2", "payment.intent.succeeded", "2026-11-01T18:30:00Z",
                   workspace_id="ws-a", intent_id="s2", amount=5_000_000, currency="THB", provider="stripe")
    r1 = _envelope("payment.refund.succeeded:r1", "payment.refund.succeeded", "2026-11-04T02:00:00Z",
                   workspace_id="ws-a", intent_id="s2", refund_id="r1", amount=3_000_000, currency="THB")
    b1 = _envelope("payment.intent.succeeded:b1", "payment.intent.succeeded", "2026-11-03T05:00:00Z",
                   workspace_id="ws-b", intent_id="b1", amount=50_000, currency="USD", provider="stripe")
    bad_amount = _envelope("payment.intent.succeeded:z1", "payment.intent.succeeded", "2026-11-02T05:00:00Z",
                           workspace_id="ws-a", intent_id="z1", amount=0, currency="THB", provider="stripe")
    bad_intent = _envelope("payment.intent.succeeded:z2", "payment.intent.succeeded", "2026-11-02T06:00:00Z",
                           workspace_id="ws-a", amount=1_000, currency="THB", provider="stripe")
    orphan = _envelope("payment.refund.succeeded:r9", "payment.refund.succeeded", "2026-11-04T03:00:00Z",
                       workspace_id="ws-a", intent_id="nope", refund_id="r9", amount=100, currency="THB")
    # Review cases (Codex, 2026-11-07): two payments at the same instant; amounts that are not JSON
    # integers; a later duplicate success in another currency; refunds whose currency or workspace
    # differ from the retained payment.
    c1 = _envelope("payment.intent.succeeded:c1", "payment.intent.succeeded", "2026-11-05T03:00:00Z",
                   workspace_id="ws-c", intent_id="c1", amount=6_000_000, currency="THB", provider="stripe")
    c2 = _envelope("payment.intent.succeeded:c2", "payment.intent.succeeded", "2026-11-05T03:00:00Z",
                   workspace_id="ws-c", intent_id="c2", amount=5_000_000, currency="THB", provider="stripe")
    bad_fraction = _envelope("payment.intent.succeeded:z3", "payment.intent.succeeded", "2026-11-02T05:00:00Z",
                             workspace_id="ws-a", intent_id="z3", amount=12.5, currency="THB", provider="stripe")
    bad_string = _envelope("payment.intent.succeeded:z4", "payment.intent.succeeded", "2026-11-02T05:00:00Z",
                           workspace_id="ws-a", intent_id="z4", amount="1200", currency="THB", provider="stripe")
    s1_usd = _envelope("payment.intent.succeeded:s1-usd", "payment.intent.succeeded", "2026-11-01T05:00:00Z",
                       workspace_id="ws-a", intent_id="s1", amount=6_000_000, currency="USD", provider="stripe")
    usd_refund = _envelope("payment.refund.succeeded:r2", "payment.refund.succeeded", "2026-11-04T04:00:00Z",
                           workspace_id="ws-a", intent_id="s1", refund_id="r2", amount=100, currency="USD")
    other_ws_refund = _envelope("payment.refund.succeeded:r3", "payment.refund.succeeded", "2026-11-04T05:00:00Z",
                                workspace_id="ws-b", intent_id="s1", refund_id="r3", amount=100, currency="THB")
    other = _envelope("platform.noise:1", "commerce.cart.viewed", "2026-11-02T07:00:00Z", workspace_id="ws-a")

    # Through the production path: the lake sink lands each Kafka micro-batch as Avro, then the
    # converter writes Bronze Parquet + a stream-parquet conversion manifest (event-sink.yml).
    # received_at is the sink's clock, so each batch lands at its own fake time.
    batches = [
        ("2026-11-01T04:00:00Z", [s1]),
        ("2026-11-01T19:00:00Z", [s2]),
        ("2026-11-02T08:00:00Z", [bad_amount, bad_intent, other]),
        # Redelivery of s2 (at-least-once) in a later batch, together with its refund.
        ("2026-11-04T02:30:00Z", [s2, r1]),
        ("2026-11-04T03:30:00Z", [orphan]),
        # b1 happened on 11-03 but reaches the lake on 11-06.
        ("2026-11-06T09:00:00Z", [b1]),
        # Review cases. s1-usd arrives first so a non-deterministic pick could prefer it.
        ("2026-11-01T03:30:00Z", [s1_usd]),
        ("2026-11-05T03:10:00Z", [c1, c2, bad_fraction, bad_string]),
        ("2026-11-05T03:20:00Z", [usd_refund, other_ws_refund]),
    ]
    offset = 0
    for at, batch in batches:
        messages = []
        for envelope in batch:
            messages.append(StreamMessage(topic="commerce.events.v1", partition=0, offset=offset, value=envelope))
            offset += 1
        landed = sink_messages(
            messages,
            data_lake_uri=str(lake_root),
            source="aiven-kafka",
            domain="commerce",
            dataset="events",
            received_at=at,
        )
        convert_avro_file(data_lake_uri=str(lake_root), avro_key=landed["avro_key"])


def check_sales_outputs() -> list[str]:
    """Problems with the sales marts' results on the synthetic commerce fixture."""
    import duckdb

    problems: list[str] = []

    def expect(name: str, sql: str, expected: list) -> None:
        try:
            got = con.execute(sql).fetchall()
        except duckdb.Error as exc:
            problems.append(f"{name}: query failed: {exc}")
            return
        if got != expected:
            problems.append(f"{name}: expected {expected}, got {got}")

    with duckdb.connect(str(DBT_PROJECT_DIR / "local.duckdb"), read_only=True) as con:
        # Same zone as the dbt profile, so timestamps render the same on every machine.
        con.execute("set TimeZone = 'UTC'")
        expect(
            "fact_sales",
            "select intent_id, customer_id, cast(sale_date as varchar), currency, gross_minor, refunded_minor, "
            "net_minor, refund_count, over_refunded, arrived_late, customer_tier_at_sale "
            "from fact_sales order by intent_id",
            [
                ("b1", "ws-b", "2026-11-03", "USD", 50_000, 0, 50_000, 0, False, True, "standard"),
                ("c1", "ws-c", "2026-11-05", "THB", 6_000_000, 0, 6_000_000, 0, False, False, "vip"),
                ("c2", "ws-c", "2026-11-05", "THB", 5_000_000, 0, 5_000_000, 0, False, False, "vip"),
                ("s1", "ws-a", "2026-11-01", "THB", 6_000_000, 0, 6_000_000, 0, False, False, "standard"),
                ("s2", "ws-a", "2026-11-02", "THB", 5_000_000, 3_000_000, 2_000_000, 1, False, False, "vip"),
            ],
        )
        expect(
            "dim_customer",
            "select customer_id, version, tier, cast(valid_from as varchar), cast(valid_to as varchar), is_current "
            "from dim_customer order by customer_id, version",
            [
                ("ws-a", 1, "standard", "2026-11-01 03:00:00+00", "2026-11-01 18:30:00+00", False),
                ("ws-a", 2, "vip", "2026-11-01 18:30:00+00", "2026-11-04 02:00:00+00", False),
                ("ws-a", 3, "standard", "2026-11-04 02:00:00+00", None, True),
                ("ws-b", 1, "standard", "2026-11-03 05:00:00+00", None, True),
                ("ws-c", 1, "vip", "2026-11-05 03:00:00+00", None, True),
            ],
        )
        expect(
            "sales_daily_revenue",
            "select cast(business_date as varchar), currency, orders, customers, gross_minor, "
            "refunded_against_sales_minor, net_minor, refunds_booked_minor, late_orders "
            "from sales_daily_revenue order by business_date, currency",
            [
                ("2026-11-01", "THB", 1, 1, 6_000_000, 0, 6_000_000, 0, 0),
                ("2026-11-02", "THB", 1, 1, 5_000_000, 3_000_000, 2_000_000, 0, 0),
                ("2026-11-03", "USD", 1, 1, 50_000, 0, 50_000, 0, 1),
                ("2026-11-04", "THB", 0, 0, 0, 0, 0, 3_000_000, 0),
                ("2026-11-05", "THB", 2, 1, 11_000_000, 0, 11_000_000, 0, 0),
            ],
        )
        expect(
            "sales_rejected_events",
            "select event_key, reject_reason from sales_rejected_events order by event_key",
            [
                ("payment.intent.succeeded:z1", "invalid_amount"),
                ("payment.intent.succeeded:z2", "missing_id"),
                ("payment.intent.succeeded:z3", "invalid_amount"),
                ("payment.intent.succeeded:z4", "invalid_amount"),
                ("payment.refund.succeeded:r2", "currency_mismatch"),
                ("payment.refund.succeeded:r3", "workspace_mismatch"),
                ("payment.refund.succeeded:r9", "orphan_refund"),
            ],
        )
    return problems


def check_partial_commerce_lakes() -> list[str]:
    """The sales staging model builds on a lake with only ONE manifest family: stream-only (a fresh
    lake fed by the event sink) and batch-only (no commerce events yet). DuckDB fails on a glob that
    matches nothing, so the model must not read a missing family."""
    import duckdb

    problems: list[str] = []
    cases = (("stream-only", build_synthetic_commerce_events, True), ("batch-only", build_synthetic_x_feed, False))
    for label, build, expect_rows in cases:
        with tempfile.TemporaryDirectory() as tmp:
            build(Path(tmp))
            families = {"stream": any((Path(tmp) / "control/manifests/stream-parquet").glob("source=*/*.json")),
                        "batch": any((Path(tmp) / "control/manifests").glob("source=*/batch_id=*.json"))}
            if sum(families.values()) != 1:
                problems.append(f"{label} fixture should hold exactly one manifest family, got {families}")
                continue
            target_dir = Path(tmp) / "target"
            target_dir.mkdir(parents=True, exist_ok=True)
            db = Path(tmp) / "partial.duckdb"
            result = subprocess.run(
                [str(DBT_BIN), "build", "--select", "+sales_daily_revenue",
                 "--profiles-dir", str(DBT_PROJECT_DIR), "--project-dir", str(DBT_PROJECT_DIR),
                 "--target-path", str(target_dir),
                 "--vars", json.dumps({"bronze_root": str(Path(tmp) / "bronze"),
                                       "bronze_manifest_root": str(Path(tmp) / "control" / "manifests")})],
                cwd=str(DBT_PROJECT_DIR), capture_output=True, text=True,
                env={**os.environ, "DBT_DUCKDB_PATH": str(db)},
            )
            if result.returncode != 0:
                print(result.stdout[-2000:])
                problems.append(f"sales marts do not build on a {label} lake")
                continue
            path = db if db.exists() else DBT_PROJECT_DIR / "local.duckdb"
            with duckdb.connect(str(path), read_only=True) as con:
                n = con.execute("select count(*) from stg_commerce_events").fetchone()[0]
            if (n > 0) != expect_rows:
                problems.append(f"{label} lake: stg_commerce_events has {n} rows")
    return problems


def run() -> bool:
    if DBT_BIN is None:
        message = "dbt not on PATH (install infra/requirements-data-lake.txt)"
        if os.environ.get("SOLO_EMPIRE_CI") == "1":
            print(f"FAIL: {message}; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
            return False
        print(f"SKIP: {message}")
        return True

    with tempfile.TemporaryDirectory() as tmp:
        bronze_root = Path(tmp) / "bronze"
        build_synthetic_bronze(bronze_root)
        export_registry_fixture(Path(tmp))
        jobs_silver_part = build_jobs_silver_fixture(Path(tmp))
        news_silver_part = build_news_silver_fixture(Path(tmp))
        build_synthetic_x_feed(Path(tmp))
        build_synthetic_commerce_events(Path(tmp))
        target_dir = Path(tmp) / "target"
        # dbt-duckdb does not create the target directory itself before
        # opening the database file there.
        target_dir.mkdir(parents=True, exist_ok=True)

        result = subprocess.run(
            [
                str(DBT_BIN),
                "build",
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--target-path",
                str(target_dir),
                "--vars",
                json.dumps(
                    {
                        "bronze_root": str(bronze_root),
                        "bronze_manifest_root": str(
                            Path(tmp) / "control" / "manifests"
                        ),
                        "jobs_silver_part": str(jobs_silver_part),
                        "news_silver_part": str(news_silver_part),
                    }
                ),
            ],
            cwd=str(DBT_PROJECT_DIR),
            capture_output=True,
            text=True,
        )
        print(result.stdout[-4000:])
        if result.returncode != 0:
            print(result.stderr[-2000:])
            print("FAIL: dbt build against synthetic fixture failed")
            return False

        if "ERROR=0" not in result.stdout:
            print("FAIL: dbt reported one or more model/test errors")
            return False

        docs = subprocess.run(
            [
                str(DBT_BIN),
                "docs",
                "generate",
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--target-path",
                str(target_dir),
                "--vars",
                json.dumps(
                    {
                        "bronze_root": str(bronze_root),
                        "bronze_manifest_root": str(
                            Path(tmp) / "control" / "manifests"
                        ),
                        "jobs_silver_part": str(jobs_silver_part),
                        "news_silver_part": str(news_silver_part),
                    }
                ),
            ],
            cwd=str(DBT_PROJECT_DIR),
            capture_output=True,
            text=True,
        )
        if docs.returncode != 0:
            print(docs.stdout[-2000:])
            print("FAIL: dbt docs generate failed")
            return False

        problems = check_metadata_outputs(target_dir) + check_sales_outputs()
        for problem in problems:
            print(f"FAIL: {problem}")
        if problems:
            return False

        # An empty VIP threshold map turns VIP off: the sales marts still build, everyone is standard.
        no_vip = subprocess.run(
            [
                str(DBT_BIN),
                "build",
                "--select",
                "+sales_daily_revenue",
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--target-path",
                str(target_dir),
                "--vars",
                json.dumps(
                    {
                        "bronze_root": str(bronze_root),
                        "bronze_manifest_root": str(Path(tmp) / "control" / "manifests"),
                        "jobs_silver_part": str(jobs_silver_part),
                        "news_silver_part": str(news_silver_part),
                        "sales_vip_threshold_minor": {},
                    }
                ),
            ],
            cwd=str(DBT_PROJECT_DIR),
            capture_output=True,
            text=True,
        )
        if no_vip.returncode != 0:
            print(no_vip.stdout[-2000:])
            print("FAIL: sales marts do not build with an empty sales_vip_threshold_minor")
            return False
        import duckdb

        with duckdb.connect(str(DBT_PROJECT_DIR / "local.duckdb"), read_only=True) as con:
            tiers = con.execute("select distinct tier from dim_customer").fetchall()
        if tiers != [("standard",)]:
            print(f"FAIL: empty sales_vip_threshold_minor should leave only standard tiers, got {tiers}")
            return False

        partial = check_partial_commerce_lakes()
        for problem in partial:
            print(f"FAIL: {problem}")
        if partial:
            return False

        print("PASS: dbt build, quality tests, metadata models and dbt docs succeeded against synthetic fixtures")
        return True


if __name__ == "__main__":
    success = run()
    print("\nResult:", "PASS" if success else "FAIL")
    sys.exit(0 if success else 1)
