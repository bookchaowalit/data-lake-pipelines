---
title: Cloud Data Lake MVP
aliases: [Hosted Lake, R2 Lake, Cloud Lakehouse]
tags: [architecture, cloud, data-lake, r2, iceberg, duckdb, portfolio]
created: 2026-08-07
updated: 2026-08-07
description: Safe, low-cost hosted mirror of the local Parquet/Iceberg data-lake contract.
related: [data-lake-architecture, hosted-data-integrations, secret-management, data-product-portfolio]
status: implementation-ready
---

# Cloud Data Lake MVP

## Purpose

The cloud lane is a hosted execution target for the same lake-first contract;
it is not a second source of truth and it is not a permanent dual-write
replication system.

```text
source product contract
        │
        ├── local: file:// or MinIO → SQLite Iceberg catalog → DuckDB
        │
        └── hosted: R2 → R2 Data Catalog REST → DuckDB / comparison engines
```

Use the local target for development and fixtures. Promote a known-good batch
to the hosted target for portfolio evidence, cloud integration tests, and a
publicly demonstrable lakehouse. Keep personal, private, and restricted data
out of the portfolio bucket.

## Selected hosted components

| Layer | Selection | Boundary |
|---|---|---|
| Object Storage | Cloudflare R2 | Exact landing bytes and Parquet datasets |
| Table catalog | R2 Data Catalog | Hosted Iceberg REST catalog; public beta |
| Query | DuckDB first | Read-only query/projection; comparison engines are labs |
| Schedule | GitHub Actions or Prefect Cloud | Choose one scheduler per job |
| Observability | Grafana Cloud | Small-volume metrics/logs/traces only |
| Operational state | Neon Postgres, if needed | Control-plane metadata, never raw lake storage |

R2's current free allowance is 10 GB-month storage, 1 million Class A
operations, and 10 million Class B operations per month, with no direct R2
egress charge. Standard R2 storage and operation limits still apply. See the
[R2 pricing documentation](https://developers.cloudflare.com/r2/pricing/).

R2 Data Catalog provides the Iceberg REST catalog endpoint and a provider-issued
warehouse name. It currently has public-beta status and its catalog/compaction
usage has a published included allowance; verify the current account terms
before treating it as a zero-cost production dependency. See
[R2 Data Catalog](https://developers.cloudflare.com/r2/data-catalog/) and
[catalog management](https://developers.cloudflare.com/r2/data-catalog/manage-catalogs/).

## Runtime configuration

Do not commit any value below. Store credentials in the existing secret
resolver, CI secret store, or a local ignored environment file.

### Two Cloudflare connection surfaces

Use Wrangler as the **control plane** for Cloudflare account access, bucket
management, and R2 Data Catalog lifecycle. Use the R2 **S3-compatible data
plane** for the pipeline's object reads/writes. Wrangler login by itself does
not provide the `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` pair required by
`storage.py` and the AWS CLI adapter.

```bash
# Interactive, operator-owned authentication; opens a browser.
npx wrangler@latest login
npx wrangler@latest whoami --json

# Read-only account/bucket check after login.
npx wrangler@latest r2 bucket list
```

The repository currently has no root-level Wrangler installation and no
Cloudflare session. `npx wrangler@latest` is therefore the one-off CLI path;
pin Wrangler inside the owning Worker package when a deployable Worker is
added. Do not run `bucket create`, `catalog enable`, or `deploy` in CI unless an
operator has explicitly approved that external mutation.

```bash
# R2 S3-compatible object storage
export SOLO_EMPIRE_DATA_LAKE_URI=s3://solo-empire-portfolio
export DATA_LAKE_S3_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
export AWS_REGION=auto
export AWS_ACCESS_KEY_ID=<r2-s3-access-key-id>
export AWS_SECRET_ACCESS_KEY=<r2-s3-secret-access-key>

# R2 Data Catalog values returned by the Cloudflare dashboard/Wrangler
export ICEBERG_CATALOG_URI=https://<catalog-uri>
export ICEBERG_WAREHOUSE_URI=<provider-issued-warehouse-name>
export ICEBERG_CATALOG_TOKEN=<scoped-r2-data-catalog-token>

# Explicit safety gates for remote writes
export DATA_LAKE_CLOUD_WRITE_ENABLED=true
export DATA_LAKE_CLOUD_MAX_OBJECT_BYTES=67108864
```

The S3 access key/secret and the Iceberg catalog token are separate credentials
with separate scopes. A read-only query process should use a read-only catalog
and storage token. The ingest process needs write access only to the portfolio
bucket/prefix. Never reuse the MinIO root credential or a broad account token.

## Setup by the operator

The repository does not create cloud resources or call an external account
automatically. After creating the R2 bucket, enable its catalog and copy the
returned catalog URI and warehouse name:

```bash
npx wrangler@latest r2 bucket create solo-empire-portfolio
npx wrangler@latest r2 bucket catalog enable solo-empire-portfolio
```

### Browser path for credentials

Wrangler manages the account, bucket, and catalog lifecycle, but the R2 API
token used by an S3/Iceberg client is created in the Cloudflare Dashboard:

1. Open **R2 Object Storage → Overview → Account Details → Manage API Tokens**.
2. Select **Create API token** and choose **Admin Read & Write** for a pipeline
   that writes Bronze objects and commits Iceberg catalog metadata.
3. Create a separate **Admin Read only** token for DuckDB/API readers when the
   consumer must not write. R2 Data Catalog read-only tokens are supported for
   query-only workloads.
4. Copy the token value once. Store it as `ICEBERG_CATALOG_TOKEN` in Infisical,
   the CI secret store, or an ignored local environment file.
   In the Solo Empire Infisical project, use environment `dev` and folder
   `/cloudflare` for the R2 runtime values.
5. Copy the R2 S3 Access Key ID and Secret Access Key for
   `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, and set the endpoint to
   `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`.
6. Copy the catalog URI and warehouse returned by the catalog page or Wrangler
   into `ICEBERG_CATALOG_URI` and `ICEBERG_WAREHOUSE_URI`.

The token value, S3 secret, catalog URI, and warehouse are different runtime
fields. They may be issued from one scoped R2 token or separated by workload;
never put any secret value in this document. See the official [R2 token
authentication guide](https://developers.cloudflare.com/r2/api/tokens/) and
[catalog authentication guide](https://developers.cloudflare.com/r2/data-catalog/manage-catalogs/#authenticate-your-iceberg-engine).

## Preflight and ingest

Run the preflight before any remote write. It makes no network call and fails
if a cloud write was not explicitly enabled:

```bash
python3 infra/scripts/data_lake/cloud_preflight.py \
  --data-lake-uri "$SOLO_EMPIRE_DATA_LAKE_URI" \
  --catalog-uri "$ICEBERG_CATALOG_URI" \
  --require-cloud-write \
  --json
```

After credentials are available, run the separate read-only bucket-control
check. It contacts only supported S3 control operations. For R2 it checks the
lifecycle rule, records provider-managed encryption, and reports bucket
versioning as not applicable; public URLs and custom domains still require a
Cloudflare control-plane review. It never uploads or deletes an object:

```bash
python3 infra/scripts/data_lake/cloud_preflight.py \
  --data-lake-uri "$SOLO_EMPIRE_DATA_LAKE_URI" \
  --check-storage-controls \
  --json
```

Treat `storage_controls.status=attention` or a non-empty `manual_controls` as a
promotion blocker. A successful control check still does not replace the
bounded ingest/replay/restore drill or the R2 off-host-copy evidence.

Then run one fixture or a deliberately selected public batch. The existing
ingest command writes landing bytes, Bronze Parquet, registers the Iceberg
table, and writes the manifest in that order:

```bash
python3 infra/scripts/data_lake/ingest.py \
  --input path/to/public-fixture.json \
  --source cloud-lake-demo \
  --domain operations \
  --dataset cloud_lake_demo \
  --format json \
  --data-lake-uri "$SOLO_EMPIRE_DATA_LAKE_URI" \
  --iceberg-catalog-uri "$ICEBERG_CATALOG_URI" \
  --iceberg-warehouse-uri "$ICEBERG_WAREHOUSE_URI" \
  --privacy-class public \
  --json
```

The fixture path is a placeholder; replace it with a real public fixture and
use the source product's supported options. Always perform the preflight first.
A future product-specific cloud command should
preserve the same `--no-project-csv` and fail-closed behavior as the local
pilot.

### Hosted Iceberg registration and query lab

After the Bronze object and manifest exist, register the existing Parquet part
through R2 Data Catalog and query the catalog-owned table with DuckDB. This is
an operator-gated metadata write; it does not create a second raw batch:

```bash
npx --yes @infisical/cli@0.43.120 run \
  --projectId=<solo-empire-project-id> \
  --env=dev --path=/cloudflare -- \
  bash -lc 'export DATA_LAKE_CLOUD_WRITE_ENABLED=true; \
    PYTHONPATH=infra/scripts .venv/bin/python \
    infra/scripts/data_lake/iceberg_catalog_e2e.py --json'
```

The command verifies the manifest-referenced Bronze file, creates or reuses
`bronze.portfolio_cloud_lake_demo`, queries it through DuckDB, and confirms a
retry keeps the same snapshot. `DATA_LAKE_CLOUD_WRITE_ENABLED` remains `false`
in Infisical after the command. The R2 Data Catalog is still an optional beta
metadata lane; Bronze Parquet remains the durable data contract.

The product read pilot is also live for `book-crypto-data`: the bounded prefix
`portfolio-demo/book-crypto-iceberg-001` is registered as
`bronze.market_crypto_prices`. Two Bronze rows matched two Iceberg rows exactly,
including raw-object lineage, and a registration retry kept the same snapshot.
The API reported `lake_first_iceberg_rest_duckdb` with no CSV dependency. This
is evidence for the feature-flagged comparison path; it does not change the
default Parquet read mode or make the beta catalog the raw source of truth.

The same shared-store pilot is live for `book-stock-data` on
`portfolio-demo/book-stock-iceberg-001`: both
`bronze.market_stock_prices` and `bronze.market_stock_history` passed Iceberg
versus direct Parquet parity with raw-object lineage and idempotent snapshot
retries. Its API also reported `lake_first_iceberg_rest_duckdb` without a CSV
dependency; the default remains direct Bronze Parquet.

`book-fx-data` also passed the live shared-store pilot on
`portfolio-demo/book-fx-iceberg-001`: both
`bronze.market_exchange_rates` and `bronze.market_exchange_history` matched
direct Bronze Parquet, retained raw-object lineage, and kept the same snapshot
on registration retry. Its API reported
`lake_first_iceberg_rest_duckdb` without a CSV dependency; the default remains
direct Bronze Parquet.

`book-defi-data` also passed the live shared-store pilot on
`portfolio-demo/book-defi-iceberg-001`: both
`bronze.market_defi_yields` and `bronze.market_defi_yields_history` matched
direct Bronze Parquet semantically, retained raw-object lineage, and kept the
same snapshot on registration retry. Its API on port 8104 reported
`lake_first_iceberg_rest_duckdb` without a CSV dependency; the default remains
direct Bronze Parquet. The hosted check used a deterministic DeFiLlama-shaped
fixture and did not scrape the live upstream.

`book-flight-data` also passed the live shared-store pilot on
`portfolio-demo/book-flight-iceberg-001`: both
`bronze.travel_flight_prices` and `bronze.travel_flight_prices_history` matched
direct Bronze Parquet semantically across two provider files, retained
per-provider raw-object lineage including exact HTML bytes, and kept the same
snapshot on every registration retry. Its API on port 8105 reported
`lake_first_iceberg_rest_duckdb` without a CSV dependency; the default remains
direct Bronze Parquet. The hosted check used deterministic JSON/HTML fixtures
and did not contact a flight provider.

`book-seo-data` also passed the live shared-store pilot on
`portfolio-demo/book-seo-iceberg-001`: both
`bronze.seo_seo_rankings` and `bronze.seo_seo_rankings_history` matched direct
Bronze Parquet semantically across two query-provider files, retained
per-provider/query raw-object lineage including exact HTML bytes, and kept the
same snapshot on every registration retry. Its API on port 8106 reported
`lake_first_iceberg_rest_duckdb` without a CSV dependency; the default remains
direct Bronze Parquet. The hosted check used deterministic page fixtures and
did not contact Google, Firecrawl, or a paid SERP provider.

`book-ai-tools-data` (retired 2026-10-23 in data-product-contracts-v3; record kept as history) also passed the live shared-store pilot on
`portfolio-demo/book-ai-tools-iceberg-001`: both
`bronze.ai-tools_ai_tools` and `bronze.ai-tools_ai_tools_history` matched direct
Bronze Parquet semantically across two directory-provider files, retained
per-provider/URL raw-object lineage including exact HTML bytes and canonical
URL identity, and kept the same snapshot on every registration retry. Its API
on port 8107 reported `lake_first_iceberg_rest_duckdb` without a CSV dependency;
the default remains direct Bronze Parquet. The hosted check used deterministic
directory fixtures and did not contact Firecrawl, Brave, or paid search.

## Cost and safety guard

The storage adapter enforces these rules for non-local `s3://` endpoints:

- remote writes require `DATA_LAKE_CLOUD_WRITE_ENABLED=true`;
- each uploaded object is limited by `DATA_LAKE_CLOUD_MAX_OBJECT_BYTES`;
- local MinIO endpoints are not treated as paid cloud targets;
- the preflight reports configuration without printing credential values;
- CI runs the contract test offline and has no cloud credentials.

The default per-object limit is 64 MiB. Raise it only for a deliberate native
file workload and record the reason in the run manifest or job documentation.
Use dataset retention and R2 lifecycle rules to prevent fixture data from
accumulating indefinitely.

## Verification boundary

The committed workflow
`.github/workflows/data-lake-cloud-contract.yml` validates URI handling,
REST catalog configuration, remote-write opt-in, and object-size limits without
contacting R2. It is not evidence that a real R2 bucket is reachable.

Live verification remains a separate operator action:

1. run a small public fixture or public source batch;
2. verify provider-supported bucket controls with the read-only preflight and
   close any R2 `manual_controls` with Cloudflare control-plane evidence;
3. verify landing bytes and Bronze checksum;
4. query the registered table with DuckDB through the Iceberg REST catalog;
5. replay the manifest and confirm the same `run_id`/`raw_sha256`;
6. verify the R2 off-host copy that replaces bucket versioning;
7. remove the test prefix only through the provider/catalog workflow.

Do not run this live check with private personal data or broad cloud
credentials.
