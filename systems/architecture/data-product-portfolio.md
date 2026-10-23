---
title: Data Product Portfolio Architecture
aliases: [Data Products, Scraping and API Portfolio]
tags: [architecture, data-products, scraping, api, cli, portfolio]
created: 2026-08-04
updated: 2026-10-23
description: Boundary and contract rules for data-producing repositories consumed by Solo Empire applications.
related: [repository-boundaries, ARCHITECTURE, REGISTRY]
---

# Data Product Portfolio Architecture

## Intent

Solo Empire applications and the `bookchaowalit` portfolio should consume
data through explicit products and contracts. The primary inputs are:

```text
public scraping + external APIs + controlled CLI/automation
        -> domain data product
        -> normalized contract/API/CLI
        -> application, dashboard, or portfolio demo
```

Application repositories must not import scraper implementation files or read
another repository's runtime data directory directly. They consume a stable
HTTP/API, CLI contract, export schema, or versioned package instead.

## Boundary rule

The primary boundary is the **domain/product**, not the transport mechanism.
Scraping, API ingestion, and a CLI can live in one domain repository when they
share the same users, data model, release cadence, and scaling profile. Split
them into separate repositories only when they have independent deployment,
credentials, ownership, or portfolio value.

This keeps the portfolio understandable without turning every script into a
micro-repository:

| Layer | Canonical owner | Boundary |
|---|---|---|
| Source adapters | Domain data product | Scrapers, API clients, RSS/feeds, rate limits, retries |
| Normalization and quality | Domain data product | Schema, deduplication, fixtures, freshness checks |
| Delivery interface | Domain data product | API, CLI, export, webhook, or MCP adapter |
| Application experience | Product/application repo | UI, workflow, auth, user experience |
| Cross-domain orchestration | `infra/` | Solo Empire database, control plane, shared CLI, MCP, sync, and operations |

`infra/` remains the shared operating system. A domain-specific adapter should
move into its domain product; a genuinely cross-domain scheduler, registry,
secret policy, or runtime helper stays in `infra/`.

## Scraper/source-product boundary

Scraper repositories and source data products are producers only. They may
pull from approved upstream websites, feeds, and public APIs, then write
`landing/`, Bronze Parquet, manifests, and quarantine objects through the
shared lake adapter. They may expose their own read-only GET API, CLI, or MCP
contract backed by the lake.

They must not call or mutate another application's backend, write to another
repository's database or runtime `data/` directory, or push into CRM, tasks,
revenue, dashboards, RAG/AI indexes, notifications, or external task systems.
Consumers pull from the source product's contract; the source product never
pushes into the consumer. This keeps the direction explicit:

```text
upstream pull → scraper/source product → lake write → read-only contract
                                                  ↑
                                apps and infra pull from here
```

### Shared acquisition capability versus domain ownership

The platform is intentionally broad across businesses, but scraping is not
duplicated inside every application. Each source should use the strongest
available access path in this order: official API, CLI/export, RSS/feed, then
bounded scraping when no supported interface exists. Shared HTTP/RSS/browser
engines provide rate limits, retries, normalization, deduplication, and safety
controls; domain adapters own source-specific parsing and output contracts.

The current multi-business coverage inventory lives in the nested
`book-job-scraping/config/source_coverage.yaml` registry. It records active
and blocked sources across jobs, property, discovery, news, marketplace,
restaurants, finance, marketing, AI, travel, and opportunities. That registry
is a coverage/activation queue, not permission for every downstream app to
scrape or for one repository to become the durable owner of every domain.

The root-level
[`scraping-data-coverage.yaml`](../ai-data/scraping-data-coverage.yaml) registry
owns the complementary requirement-to-delivery view: consumer decision,
owner, permission, freshness SLO, expected capture, lake dataset, contract,
and next action. `task scraping:coverage:report` joins both
registries with local cron and freshness evidence. A scheduler job counts as
mapped inventory only; end-to-end coverage additionally requires fresh lake
evidence and a versioned consumer contract.

```text
shared acquisition engines
  → domain source adapters (API / CLI / RSS / scraper)
  → domain contract + lake/API
  → applications, dashboards, CRM, and AI consumers
```

The read-only API belongs to the source product boundary but is not a backend
integration. Cross-domain opportunity synthesis has been retired. Technology
signal discovery now uses the `book-discovery-data` `discovery.v1` contract;
the 14 opportunity-domain pilots still use the local portfolio inventory and
each owning domain contract. Track B job consumers use the separate
`book-job-data` contract (`job.v1` on `:8109`).

## Current portfolio map

Funding, benefits, learning, participation and commercial discovery now have
14 separate local contract scaffolds. Their domain taxonomy, existing-product
reuse, ownership rules and implementation gaps are in the
[Discovery Data Product Portfolio](discovery-portfolio.md) and its
[machine-readable manifest](discovery-portfolio-v1.json). All fourteen now have
manually invoked official-source pilots and a local CLI consumer.
The new domain v2 contracts do not alter the frozen contracts below.

| Capability | Current repository | Target status |
|---|---|---|
| Jobs capture | `book-job-scraping` | Collection-only file handoff |
| Jobs data contract | [`bookchaowalit/career-job-market-data`](https://github.com/bookchaowalit/career-job-market-data) | Public `job.v1` / :8109; Lake-first Track B consumer boundary |
| Ecommerce data | `book-ecommerce-scraping` | Already independent |
| News data | `book-news-scraping` | `news.v1` Bronze/API on :8108; additive to the frozen v3 map |
| Technology discovery data | `book-discovery-data` | `discovery.v1` Bronze/API on :8110; additive technology signals from approved captures |
| Property data | `book-property-scraping` | Already independent |
| Food and beverage data | `book-restaurant-scraping` | Already independent; portfolio name may later become food-beverage |
| Directory/business data | `book-directory-scraping` | Already independent |
| Crypto data | `book-crypto-data` (local staging) | Validate contract, then publish the crypto data product |
| Stocks data | `book-stock-data` (local staging) | Validate contract, then publish the stock data product |
| FX data | `book-fx-data` (local staging) | Validate contract, then publish the currency data product |
| DeFi data | `book-defi-data` (local staging) | Validate contract, then publish the DeFi data product |
| Flight data | `book-flight-data` (local staging) | Validate provider policy, then publish the flight data product |
| SEO data | `book-seo-data` (local staging) | Validate provider policy, then publish the SEO intelligence product |
| AI tools data | `book-ai-tools-data` (`bookchaowalit/tech-ai-tools-data`) | **Retired 2026-10-23** in [`data-product-contracts-v3.yaml`](data-product-contracts-v3.yaml) (owner approved); no live producer or consumer, repository to be archived |

The existing `bookchaowalit-*` frontend, mobile, backend, and product
repositories remain application or portfolio consumers unless their code is
itself a reusable data product. They should consume contracts rather than
become storage owners for scraper output.

## Migration order

1. Keep the coverage registry as the migration inventory, while moving each
   non-job source adapter to the sibling domain repository that owns its
   contract; do not make downstream applications scrape directly.
2. Define one normalized schema and fixture set per domain before moving code.
3. Extract the smallest independently runnable source product from
   `book-opportunity-scraping` first: crypto, stocks, FX, DeFi, flights, SEO,
   and AI tools were separate candidates (AI tools was retired on 2026-10-23).
4. Keep `opportunities/` as the human-facing inventory and research area;
   executable discovery belongs to the owning domain repositories and the
   shared portfolio CLI.
5. Point applications and `infra` consumers at APIs, CLIs, exports, or
   versioned packages. Remove direct reads of legacy paths only after a
   consumer migration check passes.
6. Register every resulting repository and deployment source path before
   deleting or deprecating the old implementation.

## Current implementation handoff

The free-only local API + CLI slice is implemented in the six frozen source
repositories (the seventh, AI tools, was retired in v3), with the additive `book-news-scraping` `news.v1`,
`book-discovery-data` `discovery.v1`, and separate Track B `book-job-data`
boundaries, and tracked in
[`operations/runbooks/CODING-AGENT-DATA-PRODUCT-API-FREE-PLAN.md`](../../operations/runbooks/CODING-AGENT-DATA-PRODUCT-API-FREE-PLAN.md).
Each repo exposes a loopback read-only HTTP API that serves normalized local
data only, with `POST /v1/refresh` disabled by default and paid providers
blocked unless explicitly opted in.

**The six domain source-product v1 contracts are frozen in generation v3** (v3
retires `ai_tools.v1` / `:8107`; v2 is superseded). Port map, envelope keys, schema_version values,
GET-only consumer surface, and free-only defaults are the single source of
truth in:

- [`data-product-contracts-v3.yaml`](data-product-contracts-v3.yaml)
- [`data-product-versioning-policy.md`](data-product-versioning-policy.md)

Breaking changes require a **new** `schema_version` (for example `crypto.v2`),
not an in-place mutation of v1. Local/CI guard:

```bash
task contracts:check
```

First consumers migrated to the versioned envelopes:

- `bookchaowalit/book-products/insights-dashboard` — typed client + static UI
  with fixture fallback (`?fixtures=1`)
- `bookchaowalit-website/.../bookchaowalit-portfolio-frontend` — Live Systems
  page reads ports 8101–8110 (or sanitized fixtures); legacy
  `scraper-dashboard.json` is deprecated and unused at runtime
- `book-news-scraping` — approved RSS captures replay into Bronze and expose
  the read-only `news.v1` API on `:8108`; consumers must preserve publisher
  attribution and never read the exported CSVs directly.
- `book-discovery-data` — approved GitHub Trending, Hacker News, DEV.to, and
  Product Hunt captures replay into Bronze and expose the read-only
  `discovery.v1` API on `:8110`; consumers must preserve public-source
  attribution and never read exported CSVs directly.
- Cross-domain opportunity discovery still uses `infra/scripts/discovery/portfolio.py`
  and the local domain CLIs; the retired opportunity synthesis has no shared
  service.
- **Track B job consumers** (`import_job_scraper`, dashboard leads, and
  book-sales/jobsdb) use `infra/scripts/utils/job_client.py`: API first via
  `GET :8109` with Bronze DuckDB fallback. They must not open scraper CSVs and
  must never use the retired opportunity synthesis boundary.

The Track B contract is maintained separately in
[`job-data-contract-v1.yaml`](job-data-contract-v1.yaml). The producer path
is `book-job-scraping capture → book-job-data landing/Bronze`; there is no
scraper-to-backend push connection. The public `book-job-data` repository is
the published source boundary, installs the public shared
[`data-lake-core`](https://github.com/bookchaowalit/data-lake-core)
runtime, and its pinned commit is checked by parent CI.
The former nested source remains temporarily as a compatibility mirror for the
existing Render Docker context; it is not a second contract or consumer path.
The public repo now contains the deploy-ready Dockerfile and Render Blueprint.
Do not delete the mirror until the existing Render service has been explicitly
repointed and the hosted verifier passes.

Next safe steps: migrate any remaining job-only scripts after their consumer
role is registered, then repoint the existing Render service to the public
repository and run the hosted verifier. The current parent Render lane remains
bounded and unchanged until that cutover is evidenced.

## Portfolio quality gate

A new data repository is ready to be shown publicly only when it has:

- a one-sentence domain problem and named users;
- a README with setup, demo, source policy, and architecture;
- a normalized schema plus sanitized fixtures;
- tests for parsing, normalization, retries, and failure paths;
- a local CLI and one machine-consumable delivery interface;
- independent runtime data, logs, environment, and deployment configuration;
- rate-limit, privacy, robots, terms-of-service, retention, and secret rules;
- registry, app inventory, and consumer references updated.

Until that gate is met, code remains in the legacy repository as a migration
candidate rather than being copied into an unowned or duplicate repository.

The seven local staging repositories above contain the first extracted source
files, independent `data/` boundaries, package metadata, and offline smoke
tests. They have reserved origin URLs but no commit or published remote yet.
The legacy source remains available during validation; consumers must not be
switched until the first baseline is intentionally published and its contract
is recorded in the registry.
