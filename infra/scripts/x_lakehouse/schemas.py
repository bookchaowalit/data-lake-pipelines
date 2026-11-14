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

import os
from dataclasses import dataclass, field
from typing import Literal

import pyarrow as pa

Kind = Literal["str", "int", "float", "bool", "ts", "date"]
Privacy = Literal["public", "private"]

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
    # "private": the owner's own X account data. Raw only, under <prefix>/private/; inbox in schema
    # x_intel_private, bot door x_intel_private.ingest(); never served to the Sheet (private.py).
    privacy: Privacy = "public"
    # False: the dataset has no Sheet tab (bots write it through ingest() only). The Sheet catch-up,
    # the Sheet validation and the inbox-vs-Sheet compare skip it.
    sheet: bool = True
    # Text columns whose phone numbers become "[phone]" at ingest (SQL) and when typed (Python).
    mask_phone: tuple[str, ...] = ()
    # Key columns where a blank cell is the value "" (never NULL): x_bookmarks.folder_id.
    empty_ok: tuple[str, ...] = ()
    kinds: dict[str, Kind] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "kinds", dict(self.columns))

    @property
    def required_columns(self) -> tuple[str, ...]:
        return self.key if self.required is None else self.required

    @property
    def private(self) -> bool:
        return self.privacy == "private"

    @property
    def names(self) -> list[str]:
        return [c for c, _ in self.columns]

    def arrow_schema(self) -> pa.Schema:
        data = [pa.field(c, ARROW[k], nullable=True) for c, k in self.columns]
        return pa.schema(
            data + META_FIELDS,
            metadata={"dataset": self.name, "contract": f"{self.name}.v1", "sheet_tab": self.tab,
                      "privacy": self.privacy},
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
# Weekly outputs of the Chief of Bot Monday report. Not in the 2026-11-05 scope; added 2026-11-08 so
# no X-pipeline tab is left without a lake home at cutover (headers read from the live Sheet that day).
X_ANALYSES = Dataset(
    "x_analyses", "X Analyses",
    _cols("report_week:str analysis:str rank:int item:str post_id:str link:str topic:str book_domain:str "
          "geo:str value:str value_kind:str source_tier:str metric:str note:str"),
    key=("report_week", "analysis", "rank", "item"),
    ingest_ts=None,  # ISO week text only: dated by the run (backfill) and checked against every partition
    notes="value holds counts, ratios and words depending on value_kind, so it stays text",
)
X_TRIAGE_AUDIT = Dataset(
    "x_triage_audit", "X Triage Audit",
    _cols("audit_week:str logged_at:ts n:int label_agree:float route_agree:float label_confusions:str "
          "route_confusions:str drift_vs_prev:str rule_version:str note:str"),
    key=("audit_week", "logged_at"),
    ingest_ts="logged_at",
)
TAXONOMY_MAP = Dataset(
    "taxonomy_map", "Taxonomy Map",
    _cols("section:str v3_topic_or_type:str book_domain_or_type:str note:str"),
    key=("section", "v3_topic_or_type"),
    ingest_ts=None, raw=False,
)

# ---------------------------------------------------------------- bot-only datasets (2026-11-10)
# Spec: docs/specs/x-lake-private-datasets.md (Chief Data schema_spec m1110-src). No Sheet tab: the bots
# send them through ingest() only. "numeric" in the spec maps to float (double precision) and "int" to
# int (bigint), the kinds this package already uses. Derived values (deltas, velocity, eng_rate, rank)
# are the writer's; the lake stores what it is sent.

# Private: the owner's own X account data (inbox in schema x_intel_private, raw under private/).
X_OWN_ACCOUNT_DAILY = Dataset(
    "x_own_account_daily", "",
    _cols("snapshot_date:date snapshot_at:ts handle:str user_id:str followers:int following:int posts:int "
          "listed:int followers_delta:int posts_24h:int views_24h:int likes_24h:int replies_24h:int "
          "reposts_24h:int mentions_24h:int source:str"),
    key=("snapshot_date", "handle"),
    required=("snapshot_date", "handle", "user_id"),
    ingest_ts="snapshot_at",  # blank for the CSV backfill: those rows are dated by the run, checked everywhere
    privacy="private", sheet=False,
    notes="posts = tweet_count; source is backfill_csv or run",
)
X_OWN_MENTIONS = Dataset(
    "x_own_mentions", "",
    _cols("post_id:str own_handle:str kind:str collected_at:ts posted_at:ts author_handle:str author_id:str "
          "text:str in_reply_to_post_id:str conversation_id:str likes:int reposts:int replies:int quotes:int "
          "views:int url:str filter_flags:str"),
    key=("post_id", "own_handle"),
    ingest_ts="collected_at",
    privacy="private", sheet=False, mask_phone=("text",),
    notes="kind: mention / reply / quote; flagged rows are kept with their filter_flags",
)
# A bookmark is observed again on every pull: the raw key carries last_seen_at and removed_at, so each
# new observation (seen again, or removed) is a new raw version; a reader takes the newest
# last_seen_at per (post_id, own_handle, folder_id). own_handle: the owner pulls the bookmarks of three
# accounts, and two of them may bookmark the same post.
X_BOOKMARKS = Dataset(
    "x_bookmarks", "",
    _cols("post_id:str own_handle:str folder_id:str folder_name:str first_seen_at:ts last_seen_at:ts removed_at:ts "
          "author_handle:str author_id:str posted_at:ts text:str lang:str domain:str topic:str tags:str "
          "time_bound_date:date calendar_candidate:bool likes:int reposts:int replies:int views:int url:str "
          "filter_flags:str"),
    key=("post_id", "own_handle", "folder_id", "last_seen_at", "removed_at"),
    required=("post_id", "own_handle", "last_seen_at"),
    ingest_ts="last_seen_at",
    privacy="private", sheet=False, mask_phone=("text",), empty_ok=("folder_id",),
    notes="folder_id is '' outside a folder (never NULL); removed_at stays null until the bookmark is gone",
)
X_HOME_TIMELINE = Dataset(
    "x_home_timeline", "",
    _cols("post_id:str collected_at:ts posted_at:ts author_handle:str author_id:str author_followers:int "
          "text:str lang:str likes:int reposts:int replies:int quotes:int views:int age_min:float "
          "velocity:float eng_rate:float rank:int domain:str topic:str filter_flags:str url:str"),
    key=("post_id", "collected_at"),
    ingest_ts="collected_at",
    privacy="private", sheet=False, mask_phone=("text",),
    notes="fetched posts and their ranking only: no reply text is stored",
)
# Public (not account data): the public inbox and the normal raw path, never served to the Sheet here.
X_SPACES = Dataset(
    "x_spaces", "",
    _cols("space_id:str collected_at:ts state:str title:str scheduled_start:ts started_at:ts lang:str "
          "creator_handle:str creator_id:str participant_count:int query:str domain:str topic:str "
          "calendar_candidate:bool filter_flags:str url:str"),
    key=("space_id", "collected_at"),
    ingest_ts="collected_at",
    sheet=False, mask_phone=("title",),
)
X_COMMUNITIES = Dataset(
    "x_communities", "",
    _cols("community_id:str snapshot_date:date name:str description:str member_count:int created_at:ts "
          "access:str query:str domain:str topic:str member_delta:int filter_flags:str url:str"),
    key=("community_id", "snapshot_date"),
    ingest_ts=None,  # created_at is the community's own creation time: dated by the run, checked everywhere
    sheet=False, mask_phone=("name", "description"),
    notes="rows flagged adult / spam / gambling are stored with their flags; filtering is the writer's",
)
# Kinds proposed from the spec's one-line column list (no closer model in the repo). The raw key carries
# verified_at, so each verification is a raw version; a reader takes the newest per list_id.
X_LIST_SOURCES = Dataset(
    "x_list_sources", "",
    _cols("list_id:str name:str owner_handle:str member_count:int follower_count:int mapped_qid:str "
          "members_hash:str verified_at:ts status:str"),
    key=("list_id", "verified_at"),
    required=("list_id",),
    ingest_ts="verified_at",
    sheet=False,
    notes="ready before X Lists are live: no writer yet",
)
# ------------------------------------------------- more bot-only datasets (owner 2026-11-14, raw only)
# No writer spec: kinds follow the X API v2 objects the bots read. Proposed contract v1; a column the
# writer sends that is not here is refused by ingest() ("unknown columns"), never dropped silently.
# Who quoted, reposted or liked a top post (Chief Data caps it at ~10 calls a day). One row per pull:
# raw is partitioned by collected_at and only checks keys inside a partition, so collected_at must be
# in the key (an engagement seen again on another day is a new version, not a duplicate key). A
# reader takes the earliest collected_at per (post_id, engagement, user_id).
X_POST_ENGAGERS = Dataset(
    "x_post_engagers", "",
    _cols("post_id:str engagement:str user_id:str user_handle:str user_name:str user_followers:int "
          "user_verified:bool collected_at:ts engaged_at:ts quote_post_id:str quote_text:str "
          "quote_likes:int query_id:str filter_flags:str"),
    key=("post_id", "engagement", "user_id", "collected_at"),
    ingest_ts="collected_at",
    sheet=False, mask_phone=("quote_text",),
    notes="engagement: quote / repost / like; engaged_at only for quotes (X gives no time for likes or reposts)",
)
# X News stories (search_news), one row per story per pull: a story's headline and post list change.
X_NEWS = Dataset(
    "x_news", "",
    _cols("story_id:str collected_at:ts headline:str summary:str category:str keywords:str lang:str "
          "post_ids:str post_count:int story_updated_at:ts query:str url:str"),
    key=("story_id", "collected_at"),
    ingest_ts="collected_at",
    sheet=False, mask_phone=("headline", "summary"),
    notes="keywords and post_ids are '; '-joined lists (ingest() refuses nested values)",
)
# Private: posts the owner's accounts liked, and their bookmark folders. A like is pulled again every
# run with fresh counts, so collected_at is in the key (same reason as x_post_engagers).
X_OWN_LIKES = Dataset(
    "x_own_likes", "",
    _cols("post_id:str own_handle:str collected_at:ts posted_at:ts author_handle:str author_id:str text:str "
          "lang:str likes:int reposts:int replies:int quotes:int views:int url:str filter_flags:str"),
    key=("post_id", "own_handle", "collected_at"),
    ingest_ts="collected_at",
    privacy="private", sheet=False, mask_phone=("text",),
    notes="X gives no liked-at time: the earliest collected_at per (post_id, own_handle) is the first pull that saw it",
)
# A folder list per account per pull (the folder's posts are x_bookmarks rows with its folder_id).
X_BOOKMARK_FOLDERS = Dataset(
    "x_bookmark_folders", "",
    _cols("folder_id:str own_handle:str collected_at:ts name:str post_count:int"),
    key=("folder_id", "own_handle", "collected_at"),
    ingest_ts="collected_at",
    privacy="private", sheet=False, mask_phone=("name",),
)
PRIVATE_NAMES = ("x_own_account_daily", "x_own_mentions", "x_bookmarks", "x_home_timeline", "x_own_likes",
                 "x_bookmark_folders")
NEW_PUBLIC_NAMES = ("x_spaces", "x_communities", "x_list_sources", "x_post_engagers", "x_news")
NEW_NAMES = PRIVATE_NAMES + NEW_PUBLIC_NAMES

# Rollback switch (name only; the owner sets it): X_LAKE_NEW_DATASETS=off drops the bot-only datasets
# from every allowlist, so ingest() refuses them as unknown again. Stored rows and files stay.
NEW_DATASETS_ON = os.environ.get("X_LAKE_NEW_DATASETS", "on").strip().lower() != "off"


SHEET_DATASETS: dict[str, Dataset] = {
    d.name: d
    for d in (X_FEED, X_POST_CONTEXT, X_TRENDS_TH, X_TRENDS_INTL, X_TOPIC_COUNTS, X_ACCOUNT_SNAPSHOTS,
              ALPHA_FEED, X_SIGNALS, CLAIMS, X_WATCHLIST, X_QUERY_PLAN, TAXONOMY_MAP, X_ANALYSES,
              X_TRIAGE_AUDIT)
}
NEW_DATASETS: dict[str, Dataset] = {
    d.name: d for d in (X_OWN_ACCOUNT_DAILY, X_OWN_MENTIONS, X_BOOKMARKS, X_HOME_TIMELINE, X_SPACES,
                        X_COMMUNITIES, X_LIST_SOURCES, X_POST_ENGAGERS, X_NEWS, X_OWN_LIKES, X_BOOKMARK_FOLDERS)
}
# Every dataset the drain / append accept (the allowlist; there is no separate ALLOWED list).
DATASETS: dict[str, Dataset] = {**SHEET_DATASETS, **(NEW_DATASETS if NEW_DATASETS_ON else {})}
RAW_DATASETS = [d for d in DATASETS.values() if d.raw]
PUBLIC_DATASETS = {n: d for n, d in DATASETS.items() if not d.private}
PRIVATE_DATASETS = {n: d for n, d in DATASETS.items() if d.private}

# Tabs this package must never write. serve_sheet.py writes only to SERVE_TABS.
PROTECTED_TABS = frozenset({"AI Summaries", "AI Summaries Latest"})
PROTECTED_PREFIX = "Chief "
SERVE_TABS = {"x_signals": "Serve · X Signals 7d", "claims": "Serve · Open Claims",
              "x_query_plan": "Serve · X Query Plan",
              "x_feed": "Serve · X Feed 7d"}  # from raw x_feed (feed.py): no curated table behind it
# Never served, whatever a tab list says: the four private datasets and the three new public ones.
SERVE_DENY = frozenset(NEW_NAMES)


def is_protected(tab: str) -> bool:
    return tab in PROTECTED_TABS or tab.startswith(PROTECTED_PREFIX)


# ---------------------------------------------------------------------------- curated (Postgres)

PG_SCHEMA = "x_intel"
# Holds the private inbox only (private.py); the private datasets are raw-only, no curated table.
PRIVATE_SCHEMA = "x_intel_private"


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
    # (column, value): a stored row whose column equals value (case-insensitive) is only replaced by a
    # row carrying the same value. Claims: a claim Book checked is never overwritten by a bot.
    protect: tuple[str, str] | None = None

    @property
    def data_columns(self) -> list[tuple[str, Kind]]:
        return list(self.source.columns) + [(c, k) for c, k, _ in self.joined]

    @property
    def qualified(self) -> str:
        return f"{PG_SCHEMA}.{self.table}"


# Raw-only (owner 2026-11-12): only these tables exist in Postgres. Every other dataset - x_feed,
# x_post_context, trends, topic counts, account snapshots, Alpha Feed and the seven bot-only datasets -
# lives in raw on R2 only; tables over them are built later by whoever needs them.
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
                         ("topic",)), version="last_checked", protect=("checked_by", "book")),
        Curated("x_watchlist", X_WATCHLIST, pk=("requesting_chief", "type", "value"),
                indexes=(("type", "value"), ("tag",))),
        Curated("x_query_plan", X_QUERY_PLAN, pk=("query_id",), indexes=(("status",), ("last_run_at",))),
        Curated("taxonomy_map", TAXONOMY_MAP, pk=("section", "v3_topic_or_type")),
        Curated("x_analyses", X_ANALYSES, pk=("report_week", "analysis", "rank", "item"),
                indexes=(("report_week",), ("post_id",))),
        Curated("x_triage_audit", X_TRIAGE_AUDIT, pk=("audit_week", "logged_at")),
    )
}


def table_ddl(c: Curated) -> list[str]:
    cols = [f"    {name} {POSTGRES[kind]}{' not null' if name in c.pk else ''}"
            for name, kind in c.data_columns]
    cols += ["    created_at timestamptz not null default now()",
             "    updated_at timestamptz not null default now()",
             "    _batch_id text"]
    out = [f"create table if not exists {c.qualified} (\n"
           + ",\n".join(cols)
           + f",\n    primary key ({', '.join(c.pk)})\n);"]
    for idx in c.indexes:
        name = f"{c.table}_{'_'.join(idx)}_idx"
        out.append(f"create index if not exists {name} on {c.qualified} ({', '.join(idx)});")
    return out


# One row per drain run (inbox / both), written by scheduled.py at the end of the run: dataset names
# and counts only. Not a dataset: ingest() refuses the name like any unknown dataset.
DRAIN_LOG_DDL = f"""create table if not exists {PG_SCHEMA}.drain_log (
    batch_id text primary key,
    started_at timestamptz not null,
    source text not null,
    rows_by_dataset jsonb not null default '{{}}'::jsonb,
    raw_written jsonb not null default '{{}}'::jsonb,
    curated jsonb not null default '{{}}'::jsonb,
    coerce_errors jsonb not null default '{{}}'::jsonb,
    missing_from_raw jsonb not null default '{{}}'::jsonb,
    ok bool not null
);
create index if not exists drain_log_started_at_idx on {PG_SCHEMA}.drain_log (started_at);"""


def postgres_ddl() -> str:
    """Idempotent DDL for the curated layer (safe to run on every deploy). Schema x_intel only; the
    private inbox has its own DDL (private.private_ddl)."""
    # Check first: "create schema if not exists" still needs CREATE on the database, which the
    # least-privilege role that owns only this schema does not have.
    out = [f"do $$ begin if not exists (select 1 from pg_namespace where nspname = '{PG_SCHEMA}') "
           f"then create schema {PG_SCHEMA}; end if; end $$;"]
    for c in CURATED.values():
        out += table_ddl(c)
    out.append(DRAIN_LOG_DDL)
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    print(postgres_ddl())
