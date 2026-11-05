"""Column contracts for the X-intelligence lakehouse: Arrow schemas (raw) and Postgres DDL (curated).

Every column name below was read from the live "Chief Findings Warehouse" Sheet on 2026-11-05
(header rows only, read-only scope). A Sheet column that is not listed here is kept, not dropped:
it lands in the ``_extra`` JSON column of the raw row and is reported as a schema mismatch.

Kinds:
    str    text; numbers the Sheet returns for a text column are kept as their text
    int    int64; a non-integer value is a coercion error
    float  float64
    bool   TRUE/FALSE (Sheet text or Sheet boolean)
    ts     "YYYY-MM-DD HH:MM[:SS]" wall time in SHEET_TZ, stored as UTC timestamp
    date   "YYYY-MM-DD"

X ids (post_id, user_id, ...) are always ``str``: they exceed 2**53 and the Sheet already stores
them as text.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa

Kind = Literal["str", "int", "float", "bool", "ts", "date"]

SHEET_TZ = "Asia/Bangkok"  # the bots write ICT wall time with no offset (columns say *_ict)

ARROW: dict[str, pa.DataType] = {
    "str": pa.string(),
    "int": pa.int64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
    "ts": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}
POSTGRES: dict[str, str] = {
    "str": "text",
    "int": "bigint",
    "float": "double precision",
    "bool": "boolean",
    "ts": "timestamptz",
    "date": "date",
}

# Lineage columns on every raw row.
META_FIELDS: list[pa.Field] = [
    pa.field("_row_key", pa.string(), nullable=False),  # sha256 of the natural key's raw text
    pa.field("_batch_id", pa.string(), nullable=False),
    pa.field("_source", pa.string(), nullable=False),  # sheet-backfill | append
    pa.field("_sheet_row", pa.int32()),  # 1-based Sheet row at backfill time; null for appends
    pa.field("_ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("_coerce_errors", pa.string()),  # JSON {column: {raw, reason}}; the column is null
    pa.field("_extra", pa.string()),  # JSON of Sheet columns this contract does not know
]


@dataclass(frozen=True)
class Dataset:
    name: str
    tab: str
    columns: tuple[tuple[str, Kind], ...]
    key: tuple[str, ...]
    # Column whose ICT date is the row's ingest date (Hive partition). None: looked up (see backfill).
    ingest_ts: str | None
    raw: bool = True
    required: tuple[str, ...] | None = None  # key columns that must not be blank (default: all)
    notes: str = ""
    kinds: dict[str, Kind] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "kinds", dict(self.columns))

    @property
    def required_columns(self) -> tuple[str, ...]:
        return self.key if self.required is None else self.required

    @property
    def names(self) -> list[str]:
        return [c for c, _ in self.columns]

    def arrow_schema(self) -> pa.Schema:
        data = [pa.field(c, ARROW[k], nullable=True) for c, k in self.columns]
        return pa.schema(
            data + META_FIELDS,
            metadata={"dataset": self.name, "contract": f"{self.name}.v1", "sheet_tab": self.tab},
        )


def _cols(spec: str) -> tuple[tuple[str, Kind], ...]:
    """'a:str b:int' -> (('a','str'), ('b','int')). Order matches the Sheet header."""
    out = []
    for part in spec.split():
        name, kind = part.split(":")
        out.append((name, kind))
    return tuple(out)  # type: ignore[return-value]


X_FEED = Dataset(
    "x_feed", "X Feed",
    _cols("collected_at:ts requesting_chief:str watch_type:str watch_value:str post_id:str post_url:str "
          "author_handle:str posted_at:ts text:str likes:int reposts:int replies:int views:int tag:str "
          "query_id:str score:float top:bool"),
    key=("post_id", "requesting_chief", "query_id", "watch_type", "watch_value", "collected_at"),
    ingest_ts="collected_at",
)
X_POST_CONTEXT = Dataset(
    "x_post_context", "X Post Context",
    _cols("post_id:str ref_type:str ref_post_id:str ref_post_url:str ref_author_handle:str "
          "ref_author_type:str ref_text:str ref_likes:int ref_posted_at:ts media_count:int "
          "media_types:str media_urls:str media_alt_text:str video_duration_s:str video_views:str"),
    key=("post_id", "ref_type", "ref_post_id"),
    required=("post_id",),  # 85% of rows are media-only posts: no ref_type / ref_post_id
    ingest_ts=None,  # no timestamp column: backfill uses the post's X Feed collected_at
    notes="video_duration_s / video_views hold '8.0; 8.0' lists for multi-video posts, so they stay text",
)
X_TRENDS_TH = Dataset(
    "x_trends_th", "X Trends TH",
    _cols("snapshot_date:date snapshot_at:ts rank:int trend_name:str trend_type:str post_count:int "
          "search_url:str picked_for_posts:bool note:str"),
    key=("snapshot_at", "rank", "trend_name"),
    ingest_ts="snapshot_at",
)
X_TRENDS_INTL = Dataset(
    "x_trends_intl", "X Trends Intl",
    _cols("snapshot_date:date snapshot_at:ts woeid:int country:str rank:int trend_name:str trend_type:str "
          "post_count:int search_url:str note:str"),
    key=("snapshot_at", "woeid", "rank", "trend_name"),
    ingest_ts="snapshot_at",
)
X_TOPIC_COUNTS = Dataset(
    "x_topic_counts", "X Topic Counts",
    _cols("snapshot_date:date snapshot_at:ts query_id:str topic:str buckets:int total_24h:int "
          "max_hour_24h:int max_hour_start_ict:ts median_hour_prior6d:float ratio:float spike:bool "
          "drilldown_query_id:str note:str"),
    key=("snapshot_at", "query_id"),
    ingest_ts="snapshot_at",
)
X_ACCOUNT_SNAPSHOTS = Dataset(
    "x_account_snapshots", "X Account Snapshots",
    _cols("snapshot_date:date snapshot_at:ts list:str watchlist_row:int handle:str user_id:str "
          "followers_count:int following_count:int tweet_count:int listed_count:int like_count:int "
          "media_count:int verified:bool protected:bool account_status:str followers_delta_vs_prev:int "
          "note:str"),
    key=("snapshot_at", "list", "handle"),
    ingest_ts="snapshot_at",
)
ALPHA_FEED = Dataset(
    "alpha_feed", "Alpha Feed",
    _cols("pulled_at_ict:ts source:str sub_source:str date:date title:str description:str metric_1:str "
          "metric_2:str extra:str url:str time_bound_date:date tags:str requesting_chief:str notes:str"),
    key=("pulled_at_ict", "source", "sub_source", "url", "title"),
    ingest_ts="pulled_at_ict",
    notes="metric_1/metric_2 mix numbers with words ('Resolved', 'fed_due:2026-11-01'), so they stay text",
)
# Raw history of the two curated feeds: not in the original list, added so the curated tables can
# always be rebuilt from R2 (Postgres keeps only the latest version of a claim).
X_SIGNALS = Dataset(
    "x_signals", "X Signals",
    _cols("post_id:str collected_at:ts query_id:str category:str subcategory:str signal_type:str "
          "author_class:str expanded_link:str novelty:str opportunity_score:int verification_status:str "
          "suggested_action:str life_cat:str domain:str topic:str tags:str tax_version:str cred_tier:str "
          "manip_flags:str content_label:str route:str alert_tier:str triage_version:str"),
    key=("post_id", "collected_at"),
    ingest_ts="collected_at",
)
CLAIMS = Dataset(
    "claims", "Claims",
    _cols("claim_id:str claim:str first_seen_link:str spread_count:int verification_status:str "
          "verification_source:str first_post_id:str first_seen:ts topic:str tags:str supporting_links:str "
          "contradicting_links:str last_checked:ts priority:float queue_state:str checked_by:str next_check:ts"),
    key=("claim_id", "last_checked"),  # one raw row per observed version of a claim
    ingest_ts="last_checked",
)
# Config tabs: curated only (small, mutable, no history needed).
X_WATCHLIST = Dataset(
    "x_watchlist", "X Watchlist",
    _cols("requesting_chief:str type:str value:str purpose:str added_date:date last_used:date tag:str"),
    key=("requesting_chief", "type", "value"),
    ingest_ts=None, raw=False,
)
X_QUERY_PLAN = Dataset(
    "x_query_plan", "X Query Plan",
    _cols("query_id:str query:str requesting_chiefs:str tags:str watchlist_rows:str priority:str "
          "last_since_id:str last_run_at:ts runs:int calls_total:int posts_total:int new_posts_last:int "
          "avg_yield:float status:str calls_last_run:int yield_last6:str query_type:str lang_filter:str "
          "result_cap:str author_caps:str run_slot:str endpoint:str plan_ref:str max_pages:str"),
    key=("query_id",),
    ingest_ts=None, raw=False,
    notes="result_cap/max_pages hold 'n/a', '25/seed', '8 calls', so they stay text",
)
TAXONOMY_MAP = Dataset(
    "taxonomy_map", "Taxonomy Map",
    _cols("section:str v3_topic_or_type:str book_domain_or_type:str note:str"),
    key=("section", "v3_topic_or_type"),
    ingest_ts=None, raw=False,
)

DATASETS: dict[str, Dataset] = {
    d.name: d
    for d in (X_FEED, X_POST_CONTEXT, X_TRENDS_TH, X_TRENDS_INTL, X_TOPIC_COUNTS, X_ACCOUNT_SNAPSHOTS,
              ALPHA_FEED, X_SIGNALS, CLAIMS, X_WATCHLIST, X_QUERY_PLAN, TAXONOMY_MAP)
}
RAW_DATASETS = [d for d in DATASETS.values() if d.raw]

# Tabs this package must never write. serve_sheet.py writes only to SERVE_TABS.
PROTECTED_TABS = frozenset({"AI Summaries", "AI Summaries Latest"})
PROTECTED_PREFIX = "Chief "
SERVE_TABS = {"x_signals": "Serve · X Signals 7d", "claims": "Serve · Open Claims"}


def is_protected(tab: str) -> bool:
    return tab in PROTECTED_TABS or tab.startswith(PROTECTED_PREFIX)


# ---------------------------------------------------------------------------- curated (Postgres)

PG_SCHEMA = "x_intel"


@dataclass(frozen=True)
class Curated:
    table: str
    source: Dataset
    pk: tuple[str, ...]
    # Extra columns joined from another dataset: (column, kind, source column in that dataset).
    joined: tuple[tuple[str, Kind, str], ...] = ()
    indexes: tuple[tuple[str, ...], ...] = ()
    # Column that orders versions of one key: an older version never replaces a newer one.
    version: str | None = None

    @property
    def data_columns(self) -> list[tuple[str, Kind]]:
        return list(self.source.columns) + [(c, k) for c, k, _ in self.joined]


CURATED: dict[str, Curated] = {
    c.table: c
    for c in (
        Curated(
            "x_signals", X_SIGNALS, pk=("post_id",),
            # Post facts come from the X Feed row with the same post_id (every Signals post_id is in
            # X Feed as of 2026-11-05). A later batch without the feed row keeps the stored values.
            joined=(("author_handle", "str", "author_handle"), ("post_url", "str", "post_url"),
                    ("posted_at", "ts", "posted_at"), ("post_text", "str", "text"),
                    ("likes", "int", "likes"), ("reposts", "int", "reposts"), ("replies", "int", "replies"),
                    ("views", "int", "views"), ("feed_score", "float", "score"), ("feed_tag", "str", "tag")),
            indexes=(("collected_at",), ("created_at",), ("domain", "collected_at"), ("cred_tier",),
                     ("alert_tier", "collected_at"), ("route",), ("author_handle",)),
            version="collected_at",
        ),
        Curated("claims", CLAIMS, pk=("claim_id",),
                indexes=(("first_post_id",), ("first_seen",), ("created_at",), ("queue_state", "next_check"),
                         ("topic",)), version="last_checked"),
        Curated("x_watchlist", X_WATCHLIST, pk=("requesting_chief", "type", "value"),
                indexes=(("type", "value"), ("tag",))),
        Curated("x_query_plan", X_QUERY_PLAN, pk=("query_id",), indexes=(("status",), ("last_run_at",))),
        Curated("taxonomy_map", TAXONOMY_MAP, pk=("section", "v3_topic_or_type")),
    )
}


def postgres_ddl() -> str:
    """Idempotent DDL for the curated layer (safe to run on every deploy)."""
    # Check first: "create schema if not exists" still needs CREATE on the database, which the
    # least-privilege role that owns only this schema does not have.
    out = [f"do $$ begin if not exists (select 1 from pg_namespace where nspname = '{PG_SCHEMA}') "
           f"then create schema {PG_SCHEMA}; end if; end $$;"]
    for c in CURATED.values():
        cols = [f"    {name} {POSTGRES[kind]}{' not null' if name in c.pk else ''}"
                for name, kind in c.data_columns]
        cols += ["    created_at timestamptz not null default now()",
                 "    updated_at timestamptz not null default now()",
                 "    _batch_id text"]
        out.append(f"create table if not exists {PG_SCHEMA}.{c.table} (\n"
                   + ",\n".join(cols)
                   + f",\n    primary key ({', '.join(c.pk)})\n);")
        for idx in c.indexes:
            name = f"{c.table}_{'_'.join(idx)}_idx"
            out.append(f"create index if not exists {name} on {PG_SCHEMA}.{c.table} ({', '.join(idx)});")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    print(postgres_ddl())
