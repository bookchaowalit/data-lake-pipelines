#!/usr/bin/env python3
"""Report readiness of the optional hosted data-platform integrations.

This is a secret-safe configuration check. It does not log values, create
cloud resources, send data, or make network calls. Use it before enabling a
provider-specific live smoke test.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from dataclasses import dataclass


@dataclass(frozen=True)
class IntegrationSpec:
    identifier: str
    role: str
    required_any: tuple[tuple[str, ...], ...]
    note: str


INTEGRATIONS = (
    IntegrationSpec(
        "r2-storage",
        "primary_cloud_lake",
        (
            (
                "SOLO_EMPIRE_DATA_LAKE_URI",
                "DATA_LAKE_S3_ENDPOINT",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
            ),
        ),
        "R2 landing, Bronze, Silver, Gold, and manifests",
    ),
    IntegrationSpec(
        "r2-catalog",
        "primary_table_catalog",
        (
            (
                "ICEBERG_CATALOG_URI",
                "ICEBERG_WAREHOUSE_URI",
                "ICEBERG_CATALOG_TOKEN",
            ),
        ),
        "R2 Data Catalog REST metadata and Iceberg snapshots",
    ),
    IntegrationSpec(
        "bigquery",
        "query_comparison",
        (
            ("GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS"),
            ("GCP_PROJECT_ID", "GCP_WORKLOAD_IDENTITY_PROVIDER", "GCP_SERVICE_ACCOUNT"),
        ),
        "Hosted warehouse comparison over a small Gold snapshot",
    ),
    IntegrationSpec(
        "neon",
        "operational_control_plane",
        (("NEON_DATA_LAKE_CATALOG_URL",),),
        "Optional PostgreSQL control/catalog alternative; never raw lake storage",
    ),
    IntegrationSpec(
        "prefect-cloud",
        "orchestration",
        (("PREFECT_API_URL", "PREFECT_API_KEY"),),
        "Optional hosted scheduler; do not schedule the same job in two systems",
    ),
    IntegrationSpec(
        "grafana-cloud",
        "observability",
        (("GRAFANA_CLOUD_OTLP_ENDPOINT", "GRAFANA_CLOUD_API_KEY"),),
        "Optional low-volume metrics, logs, and traces",
    ),
    IntegrationSpec(
        "github-actions",
        "ci_and_fixture_scheduler",
        (("GITHUB_ACTIONS",),),
        "Public-repository CI and offline fixture/nightly checks",
    ),
    IntegrationSpec(
        "cloudflare-workers",
        "public_edge_surface",
        (("CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_API_TOKEN"),),
        "Optional public API/UI edge surface; not a Bronze writer",
    ),
    IntegrationSpec(
        "cloudflare-pages",
        "static_web_hosting",
        (("CLOUDFLARE_ACCOUNT_ID", "CLOUDFLARE_API_TOKEN", "CLOUDFLARE_PAGES_PROJECT"),),
        "Static portfolio/documentation web deployment",
    ),
    IntegrationSpec(
        "cloudflare-turnstile",
        "web_form_protection",
        (("TURNSTILE_SITE_KEY", "TURNSTILE_SECRET_KEY"),),
        "Free bot protection for public forms; secret key stays server-side",
    ),
    IntegrationSpec(
        "vercel-hobby",
        "frontend_comparison",
        (("VERCEL_TOKEN", "VERCEL_PROJECT_ID"),),
        "Optional frontend/deployment comparison; not the primary edge platform",
    ),
    IntegrationSpec(
        "supabase-app",
        "app_backend_comparison",
        (("SUPABASE_URL", "SUPABASE_ANON_KEY"),),
        "Optional Auth/Realtime/BaaS lab; not the lake source of truth",
    ),
    IntegrationSpec(
        "cloud-run",
        "container_backend_comparison",
        (
            ("CLOUD_RUN_SERVICE", "GOOGLE_CLOUD_PROJECT", "GOOGLE_APPLICATION_CREDENTIALS"),
            ("CLOUD_RUN_SERVICE", "GCP_PROJECT_ID", "GOOGLE_APPLICATION_CREDENTIALS"),
            (
                "CLOUD_RUN_SERVICE",
                "GCP_PROJECT_ID",
                "GCP_WORKLOAD_IDENTITY_PROVIDER",
                "GCP_SERVICE_ACCOUNT",
            ),
        ),
        "Optional container backend; billing account and budget guard required",
    ),
    IntegrationSpec(
        "upstash-redis",
        "cache_rate_limit_queue_lab",
        (("UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN"),),
        "Optional cache, rate-limit, or small queue; never durable lake storage",
    ),
    IntegrationSpec(
        "resend",
        "transactional_email",
        (("RESEND_API_KEY",),),
        "Optional low-volume transactional email provider",
    ),
    IntegrationSpec(
        "posthog",
        "product_analytics",
        (("POSTHOG_HOST", "POSTHOG_API_KEY"),),
        "Optional product analytics and feature-flag lab; scrub personal data",
    ),
    IntegrationSpec(
        "sentry",
        "application_error_tracking",
        (("SENTRY_DSN",),),
        "Optional application error tracking; the server may use SENTRY_AUTH_TOKEN for read-only issue summaries or source maps",
    ),
    IntegrationSpec(
        "infisical",
        "secret_management",
        (
            (
                "INFISICAL_SITE_URL",
                "INFISICAL_PROJECT_ID",
                "INFISICAL_ENVIRONMENT",
                "INFISICAL_UNIVERSAL_AUTH_CLIENT_ID",
                "INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET",
            ),
            ("INFISICAL_CLI_ENABLED", "INFISICAL_PROJECT_ID", "INFISICAL_ENVIRONMENT"),
        ),
        "Free secret source for local, CI, and hosted deployments",
    ),
)


def _configured(names: tuple[str, ...]) -> bool:
    return all(bool(os.environ.get(name, "").strip()) for name in names)


def readiness_report() -> dict[str, Any]:
    rows: list[dict[str, object]] = []
    for spec in INTEGRATIONS:
        matched = next((group for group in spec.required_any if _configured(group)), None)
        present_names = sorted(
            {
                name
                for group in spec.required_any
                for name in group
                if os.environ.get(name, "").strip()
            }
        )
        expected_names = sorted({name for group in spec.required_any for name in group})
        rows.append(
            {
                "id": spec.identifier,
                "role": spec.role,
                "status": "configured" if matched else "not_configured",
                "present_names": present_names,
                "missing_names": [name for name in expected_names if name not in present_names],
                "note": spec.note,
            }
        )
    configured = sum(1 for row in rows if row["status"] == "configured")
    return {
        "status": "ok",
        "configured_count": configured,
        "integration_count": len(rows),
        "integrations": rows,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument(
        "--require",
        default="",
        help="Comma-separated integration IDs that must be configured",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report = readiness_report()
    required = {value.strip() for value in args.require.split(",") if value.strip()}
    rows = {str(row["id"]): row for row in report["integrations"]}
    missing_required = sorted(
        identifier
        for identifier in required
        if identifier not in rows or rows[identifier]["status"] != "configured"
    )
    report["required"] = sorted(required)
    report["missing_required"] = missing_required
    report["status"] = "error" if missing_required else "ok"

    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print(
            f"Hosted integrations: {report['configured_count']}/"
            f"{report['integration_count']} configured"
        )
        for row in report["integrations"]:
            print(f"  {row['id']}: {row['status']} ({row['role']})")
        if missing_required:
            print(f"Required integrations missing: {', '.join(missing_required)}", file=sys.stderr)
    return 0 if report["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
