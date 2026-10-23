---
title: Data Product Versioning Policy
aliases: [Data Product Contracts, Schema Freeze]
tags: [architecture, data-products, contracts, versioning]
created: 2026-08-04
updated: 2026-10-23
description: Frozen v3 port map for local free-only data-product APIs and consumers.
related: [data-product-portfolio, data-product-contracts-v3, data-product-contracts-v2, data-product-contracts-v1, CODING-AGENT-DATA-PRODUCT-API-FREE-PLAN]
---

# Data Product Versioning Policy

## Intent

Keep domain data products and application consumers compatible without silent
breakage. Generations **v1** and **v2** remain archival freezes. Generation
**v3** is the active freeze: it retires the `ai_tools` product
(`ai_tools.v1` on `:8107`, producer `book-ai-tools-data` =
[`bookchaowalit/tech-ai-tools-data`](https://github.com/bookchaowalit/tech-ai-tools-data))
after the owner approved its retirement on 2026-10-23. The other six rows are
copied from v2 unchanged; `flights` is on hold, not retired.

Machine-readable freeze:

- [`data-product-contracts-v3.yaml`](data-product-contracts-v3.yaml) (active)
- [`data-product-contracts-v2.yaml`](data-product-contracts-v2.yaml) (superseded 2026-10-23)
- [`data-product-contracts-v1.yaml`](data-product-contracts-v1.yaml) (superseded 2026-09-08)

Guard:

```bash
task contracts:check
# also runs as part of: task docs:check
```

## Frozen v3 port and schema map

| ID | Port | schema_version | Repository | Role |
|---|---|---|---|---|
| crypto | 8101 | `crypto.v1` | `book-crypto-data` | domain source |
| stocks | 8102 | `stock.v1` | `book-stock-data` | domain source |
| fx | 8103 | `fx.v1` | `book-fx-data` | domain source |
| defi | 8104 | `defi.v1` | `book-defi-data` | domain source |
| flights | 8105 | `flight.v1` | `book-flight-data` | domain source |
| seo | 8106 | `seo.v1` | `book-seo-data` | domain source |

### Retired in v3

| ID | Port | schema_version | Repository | Retired | Note |
|---|---|---|---|---|---|
| ai_tools | 8107 | `ai_tools.v1` | `book-ai-tools-data` (`bookchaowalit/tech-ai-tools-data`) | 2026-10-23 | Owner-approved retirement; repository to be archived. Port 8107 stays reserved and is not reassigned in v3. |

Consumers must drop the retired row in the same change set: catalogs, typed
clients, fixtures, launchers, ingest paths and freeze tests. A consumer that
still lists `ai_tools` or `:8107` does not match the v3 freeze.

Default bind: **`127.0.0.1` only**.

## Envelope contract (all products)

Every data response uses these **required keys**:

```text
schema_version
source
retrieved_at
data_status
items
next_cursor
```

Optional diagnostic fields (`error`, `detail`) must never carry secrets and
must not be required by consumers.

## HTTP contract

| Method | Path | Rule |
|---|---|---|
| GET | `/healthz` | No upstream scrape |
| GET | `/v1/metadata` | Envelope + local metadata |
| GET | `/v1/records` | Envelope of records |
| GET | `/v1/records/{id}` | Single record or 404 |
| GET | `/v1/history` | History envelope |
| POST | `/v1/refresh` | **403 by default** |

Consumers may call **GET only**. They must not scrape upstream sources or
perform external writes.

## Free-only defaults (frozen)

```text
FREE_ONLY=true
ALLOW_PAID_PROVIDERS=false
ALLOW_EXTERNAL_WRITES=false
ALLOW_REFRESH=false
```

## What is a breaking change?

Any of the following **must not** ship as the same `*.v1` schema:

- Removing or renaming a required envelope key
- Changing `schema_version` string meaning without a new version id
- Reassigning a reserved port (8101–8107, including the retired 8107) to a different product
- Changing default bind to non-loopback
- Making `POST /v1/refresh` succeed by default
- Requiring paid credentials for the free default path
- Enabling external writes (Telegram, Todoist, email, etc.) by default
- Removing GET endpoints listed above
- Changing `source` identity away from the repository name without version bump

### Additive (non-breaking) changes allowed under the same `*.v1`

- New optional fields **inside** `items[]` objects
- New optional top-level keys **beyond** the required six (consumers ignore unknowns)
- New optional query parameters with defaults preserving prior behavior
- Documentation and fixture updates that still validate against the freeze

## Required process for breaking changes

1. Propose a **new** schema id for a domain payload change, such as `crypto.v2`.
2. Keep serving the previous version until consumers migrate (dual-publish if needed).
3. Update the active contract map only by adding a **new freeze file** (for
   example `data-product-contracts-v3.yaml`), never by editing frozen rows in
   place.
4. Update producer config, fixtures, consumer catalogs, and compatibility tests
   in the same change set.
5. Run:

```bash
task contracts:check
task docs:check
# plus each affected repo's unit tests
```

## Compatibility matrix (v3 consumers)

| Consumer | Expects ports | Expects schemas |
|---|---|---|
| `insights-dashboard` (`market-insights-web`) | 8101–8106 | six domain `*.v1` |
| `bookchaowalit-portfolio-frontend` (`portfolio-site-web`) | 8101–8106 | six domain `*.v1` |

Both consumers also support the additive `news.v1` (`:8108`) and
`discovery.v1` (`:8110`) contracts; those ports are intentionally outside the
frozen six-product v3 map. A v3 consumer therefore shows eight products
(ports 8101–8106, 8108, 8110).

The six-product v3 rows are frozen. The additive news domain is tracked
separately by [`news-data-contract-v1.yaml`](news-data-contract-v1.yaml):
`book-news-scraping` serves `news.v1` on `:8108` and does not reassign any
reserved v3 port.
The additive technology-discovery domain is tracked separately by
[`discovery-data-contract-v1.yaml`](discovery-data-contract-v1.yaml):
`book-discovery-data` serves `discovery.v1` on `:8110` and does not reassign
any reserved v3 port. Both additive contracts are consumed alongside, but do
not mutate, the six frozen v3 rows.

## Enforcement

| Layer | Mechanism |
|---|---|
| Solo Empire control plane | `data_product_contract_guard.py` via `task contracts:check` / `task docs:check` |
| Producers (6 APIs) | `tests/test_contract_freeze.py` in each repo |
| Insights / portfolio | contract freeze unit tests against catalog |

## Safety

- Free-plan / local-first only for the default path
- No paid API requirement in v1 free defaults
- No production public bind in the freeze
- No commit/push/deploy is implied by this policy document
