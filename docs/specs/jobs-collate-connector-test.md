# Collate connector test for jobs (ADR-0016 P4, first step)

Status: probe built 2026-11-19; DuckDB dry run done; the endpoint run waits for P2 (MotherDuck
token, first load) and the `reader` share.
Decision: architecture-docs `ADR-0016-managed-analytics-stack-on-free-tiers.md` 1.1, Rollout P4.

## Goal

ADR-0016 1.1 makes P4 start with one question: does Collate Free's PostgreSQL connector discover
`empire_gold.jobs` through MotherDuck's Postgres endpoint as `reader`? Collate lists no MotherDuck
or DuckDB connector, and the endpoint runs DuckDB SQL without PostgreSQL's `pg_*` functions. If the
answer is no, P4 closes as not adopted and the YAML catalog stays. Only if it is yes does P4 go on
to Collate ingestion and the renamed dbt artifact.

The probe answers it before anyone signs up for Collate.

## What the probe runs

`infra/scripts/data_lake/collate_probe.py`:

1. **SQLAlchemy dialect** (endpoint only). The connector connects through SQLAlchemy's PostgreSQL
   dialect, whose version check must pass, and takes table names, primary keys, indexes and
   unique constraints from it. The server version the dialect reads is the one the connector uses
   to build its column query. Columns are not taken from the dialect: OpenMetadata replaces its
   `get_columns`.
2. **Catalog queries.** The connector's own SQL, rendered for that server version: databases,
   schemas, tables, comments, the three test-connection queries, then per approved mart the column
   query, foreign keys and partitions.

The SQL is **not stored in this repository**. OpenMetadata's ingestion code, including
`postgres/queries.py`, is under the Collate Community License 1.0 (copyright Collate), not
Apache-2.0, and copying it here would mean redistributing it under that licence. The probe
downloads `queries.py` at a pinned commit (`dd013e2…`, tag `2.0.8-release`) at run time, checks
its sha256, and reads the SQL strings with `ast`; nothing in the file is executed. Bump the pin
and the hash when Collate's ingestion version moves.

Required steps are those metadata ingestion of `empire_gold.jobs` needs (dialect connect, table
names, primary keys; databases, schemas, tables, comments, the test queries, columns). The rest
only fill optional fields. `viable` is true when every required step works and the connector's
column query returns each approved mart with exactly its approved columns.

The endpoint connection uses `sslmode=verify-full` with the system CA store, so the `reader` token
goes only to a server that proves it is the host. A dropped or refused connection is
`inconclusive` (exit 2) and fails the workflow run; only answers from the endpoint count as a
verdict. Output is the verdict and step names; errors are exception types only.

A pass is necessary, not sufficient: Collate may run a newer connector than the pinned commit, so
`viable` still needs Collate's own test connection and one ingestion run. A fail settles P4.

## DuckDB dry run (2026-11-19, DuckDB 2.0.0)

The endpoint is DuckDB SQL, so a local `empire_gold.duckdb` shows which queries DuckDB itself can
answer (`collate_probe.py --duckdb <file> [--assume-server-version N]`; the test
`COLLATE_PROBE_NETWORK=1 ... collate_probe_test` runs it too):

| Step | Result |
|---|---|
| databases, schemas, tables, schema/table comments, test queries, table owners, foreign keys | ok; `jobs` and all four marts listed |
| **columns** (required) | **fails**: the query includes the identity sub-select for servers 10 and later, which calls `pg_get_serial_sequence`; DuckDB has no such function. With the pre-10 form it works and returns the approved columns |
| server_version, view definitions, policies, procedures, functions, partitions (optional) | fail (`SHOW server_version_num`, `pg_get_viewdef` signature, `pg_policy`, `regtype`, `pg_partitioned_table`) |

So the connector works only if the endpoint reports a PostgreSQL version below 10, and SQLAlchemy's
dialect also needs a `version()` string it can parse (DuckDB returns `v2.0.0`). Both depend on how
the endpoint presents itself, which only the endpoint run shows. Expect a fail; the run settles it.

## Owner steps

1. Finish P2 and the `reader` share (`jobs-motherduck-load.md`, `jobs-preset-dashboard.md` step 3).
2. Add two repository secrets at
   https://github.com/bookchaowalit/data-lake-pipelines/settings/secrets/actions:
   - `MOTHERDUCK_READER_TOKEN` (from Infisical `/motherduck`)
   - `MOTHERDUCK_PG_HOST`: the Postgres endpoint host for the account's region,
     `pg.<region>-aws.motherduck.com` (MotherDuck UI → Settings, or the Postgres endpoint docs)
3. Run **Collate connector probe** at
   https://github.com/bookchaowalit/data-lake-pipelines/actions/workflows/collate-probe.yml.
4. Read the summary:
   - `viable: false`: record the result in ADR-0016 (P4 closed as not adopted, with the run link),
     and do not sign up for Collate. The YAML catalog stays the catalog.
   - `viable: true`: sign up for Collate Free (owner item O4), run its test connection with the
     same host and the `reader` token, then one ingestion of `empire_gold.jobs`. Only then build
     the dbt artifact step (filtered to the approved marts, relations renamed to
     `empire_gold.jobs.<mart>`).

The probe connects as user `postgres` with the token as password and database `empire_gold`, as
the endpoint docs describe; if the shared database attaches under another name for `reader`, the
probe reports `databases`/`schemas` but no tables, which is a fail to fix in the share, not in the
probe.
