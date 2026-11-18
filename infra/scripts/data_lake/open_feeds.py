#!/usr/bin/env python3
"""Open feeds: every free RSS/Atom feed, keyless JSON API and robots-allowed page in one registry, into Bronze.

Spec: docs/specs/open-feeds.md. Registry: repository-catalog/registries/open-feeds.yaml (one row per
feed; adding a feed is adding a row). One hourly run fetches every feed that is due, keeps only the
items it has not seen before, and writes two Bronze datasets under ``domain=open-data``,
``source=open-feeds``:

- ``feed_items``: one row per new item (title, link, publish time, kept fields), every feed together.
- ``feed_runs``: one row per feed fetched (status, HTTP code, counts, duration) for feed health.

Per-feed state (hashes of the items currently in each feed, ETag/Last-Modified, last fetch) is one
gzip JSON file outside the lake (the lake is write-once): the scheduled run keeps it in the GitHub
Actions cache. A lost state file only means the next run re-sends the items still in the feeds;
Bronze keeps them as extra rows and Silver keeps one row per ``id``. A run costs a handful of R2
writes however many feeds the registry holds. Only counts, feed ids and object keys are printed
(public log).

Subcommands: ``run`` (default), ``check`` (validate the registry offline), ``probe`` (fetch feeds and
report status + item counts without touching the lake), ``import-opml`` (print registry rows for the
feeds of an OPML file).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import threading
import time
import urllib.parse
import urllib.robotparser
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

SOURCE = "open-feeds"
DOMAIN = "open-data"
DATASET_ITEMS = "feed_items"
DATASET_RUNS = "feed_runs"
REGISTRY = ROOT / "repository-catalog" / "registries" / "open-feeds.yaml"
DEFAULT_LAKE_URI = str(ROOT / "data" / "lake")
DEFAULT_STATE = ROOT / "data" / "runtime" / "open-feeds" / "state.json.gz"
USER_AGENT = "data-lake-pipelines-open-feeds/1.0 (+https://github.com/bookchaowalit/data-lake-pipelines)"

KINDS = ("rss", "json", "html")
KEEP = ("headline", "full")
EVERY_HOURS = (1, 2, 3, 6, 12, 24, 168)
MAX_BYTES = 8 * 1024 * 1024
TIMEOUT_SECONDS = 25
HOST_PAUSE_SECONDS = 1.0
# Hosts whose terms ask for a slower pace than the default pause (arXiv: one request every 3 s).
HOST_MIN_PAUSE = {"rss.arxiv.org": 3.0, "export.arxiv.org": 3.0}
MAX_WORKERS = 12
MAX_ITEMS_PER_FEED = (
    300  # default; a row may raise it with `max_items` (up to MAX_ITEMS_CAP)
)
MAX_ITEMS_CAP = 5000
FEED_DEADLINE_SECONDS = 60.0  # wall clock for one response (slow-drip bodies included)
PARSER_VERSION = (
    "4"  # bump when parsing or normalisation changes: validators reset, items re-sent
)
RUN_BUDGET_SECONDS = (
    15 * 60
)  # fetching stops here; the rest stay due (the workflow cap is 25 min)
BATCH_BYTES = (
    24 * 1024 * 1024
)  # one Bronze batch stays well under the lake's 64 MiB object cap
TITLE_CAP = 500
SUMMARY_CAP = 2000
LINK_CAP = 2048
DUE_SLACK = timedelta(minutes=10)
FAIL_RATIO = 0.5
RETRY_PAUSE_SECONDS = 3.0
TRANSIENT = (
    "ConnectError",
    "ConnectTimeout",
    "ReadTimeout",
    "ReadError",
    "RemoteProtocolError",
    "HTTP 429",
    "HTTP 500",
    "HTTP 502",
    "HTTP 503",
    "HTTP 504",
)
FIELDS = {
    "id",
    "name",
    "kind",
    "url",
    "category",
    "subcategory",
    "every_hours",
    "keep",
    "lang",
    "licence",
    "enabled",
    "note",
    "max_items",
    *KINDS,
}
# Sources that source-coverage.yaml marks skip / blocked-legal (or that need an owner decision): an
# enabled row on one of these hosts is refused, so an OPML import cannot slip one in.
BLOCKED_HOSTS = (
    "reddit.com",
    "producthunt.com",
    "coingecko.com",
    "linkedin.com",
    "jobthai.com",
    "fastwork.co",
    "stackexchange.com",
    "stackoverflow.com",
    "open-meteo.com",
)
ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,63}$")
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


class FeedError(RuntimeError):
    """A feed answered with an error, an unsafe redirect or a body we refuse to parse."""

    def __init__(
        self, message: str, status: int | None = None, retry_after: float | None = None
    ):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


class RegistryError(ValueError):
    """The registry has a row the collector must not run."""


# --------------------------------------------------------------------------------------------- registry


@dataclass(frozen=True)
class Feed:
    id: str
    name: str
    kind: str
    url: str
    category: str
    subcategory: str | None = None
    every_hours: int = 1
    keep: str = "headline"
    lang: str | None = None
    licence: str | None = None
    enabled: bool = True
    max_items: int = MAX_ITEMS_PER_FEED
    spec: dict[str, Any] = field(default_factory=dict)

    @property
    def host(self) -> str:
        return urllib.parse.urlsplit(self.url).hostname or ""


def blocked_host(host: str) -> bool:
    host = host.strip().rstrip(".").lower()
    try:
        host = host.encode("idna").decode(
            "ascii"
        )  # the A-label the connection resolves
    except UnicodeError:
        return True  # a name that cannot be encoded is never fetched
    return any(host == b or host.endswith("." + b) for b in BLOCKED_HOSTS)


def _https(url: Any) -> bool:
    if not isinstance(url, str) or len(url) > LINK_CAP:
        return False
    parts = urllib.parse.urlsplit(url)
    return parts.scheme == "https" and bool(parts.hostname)


def parse_registry(doc: Any) -> list[Feed]:
    if not isinstance(doc, dict) or not isinstance(doc.get("feeds"), list):
        raise RegistryError("registry needs a top-level `feeds` list")
    feeds: list[Feed] = []
    seen: set[str] = set()
    for index, row in enumerate(doc["feeds"]):
        where = f"feeds[{index}]"
        if not isinstance(row, dict):
            raise RegistryError(f"{where} is not a mapping")
        unknown = set(row) - FIELDS
        if unknown:
            raise RegistryError(f"{where}: unknown field(s) {sorted(unknown)}")
        fid = row.get("id")
        if not isinstance(fid, str) or not ID_RE.match(fid):
            raise RegistryError(f"{where}: id must match {ID_RE.pattern}")
        if fid in seen:
            raise RegistryError(f"duplicate id {fid}")
        seen.add(fid)
        kind = row.get("kind")
        if kind not in KINDS:
            raise RegistryError(f"{fid}: kind must be one of {KINDS}")
        if not _https(row.get("url")):
            raise RegistryError(f"{fid}: url must be https")
        keep = row.get("keep", "headline")
        if keep not in KEEP:
            raise RegistryError(f"{fid}: keep must be one of {KEEP}")
        every = row.get("every_hours", 1)
        if every not in EVERY_HOURS:
            raise RegistryError(f"{fid}: every_hours must be one of {EVERY_HOURS}")
        category = row.get("category")
        if not isinstance(category, str) or not category:
            raise RegistryError(f"{fid}: category is required")
        spec = row.get(kind) or {}
        if not isinstance(spec, dict):
            raise RegistryError(f"{fid}: `{kind}` must be a mapping")
        if kind == "json" and not spec.get("snapshot") and "items" not in spec:
            raise RegistryError(
                f"{fid}: json feeds need `items` (dot path) or `snapshot: true`"
            )
        if kind == "html" and not isinstance(spec.get("item"), str):
            raise RegistryError(f"{fid}: html feeds need an `item` CSS selector")
        max_items = row.get("max_items", MAX_ITEMS_PER_FEED)
        if (
            not isinstance(max_items, int)
            or isinstance(max_items, bool)
            or not 1 <= max_items <= MAX_ITEMS_CAP
        ):
            raise RegistryError(f"{fid}: max_items must be 1..{MAX_ITEMS_CAP}")
        enabled = row.get("enabled", True) is not False
        host = urllib.parse.urlsplit(row["url"]).hostname or ""
        if enabled and blocked_host(host):
            raise RegistryError(
                f"{fid}: {host} is skip/blocked-legal in source-coverage.yaml"
            )
        feeds.append(
            Feed(
                id=fid,
                name=str(row.get("name") or fid),
                kind=kind,
                url=row["url"],
                category=category,
                subcategory=row.get("subcategory"),
                every_hours=every,
                keep=keep,
                lang=row.get("lang"),
                licence=row.get("licence"),
                enabled=enabled,
                max_items=max_items,
                spec=spec,
            )
        )
    return feeds


def load_registry(path: Path = REGISTRY) -> list[Feed]:
    import yaml  # PyYAML is in uv.lock (dbt); imported here so `--help` works without it

    with path.open(encoding="utf-8") as handle:
        return parse_registry(yaml.safe_load(handle))


# --------------------------------------------------------------------------------------------- fetching


@dataclass
class Response:
    status: int
    body: bytes
    etag: str | None = None
    last_modified: str | None = None
    final_url: str = ""


# fetch(url, headers) -> Response. It never follows a redirect: a 3xx comes back as a Response
# (status 3xx, final_url = the Location), so _follow checks every hop before it is requested.
Fetch = Callable[[str, dict[str, str]], Response]
REDIRECTS = (301, 302, 303, 307, 308)
MAX_HOPS = 5


def _guarded_backend() -> Any:
    """An httpcore network backend that resolves the host itself, refuses the connection unless
    every address is public (global), and connects to a checked address. TLS keeps the original
    hostname (httpcore sends the origin host as SNI and verifies the certificate against it), so
    a DNS rebinding answer between a check and the connect is impossible: there is one lookup."""
    import ipaddress
    import socket

    from httpcore._backends.sync import SyncBackend

    class GuardedBackend(SyncBackend):
        def connect_tcp(  # type: ignore[override]
            self,
            host: str,
            port: int,
            timeout: float | None = None,
            local_address: str | None = None,
            socket_options: Any = None,
        ) -> Any:
            try:
                infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            except OSError:
                raise FeedError("ConnectError: DNS lookup failed") from None
            addresses = [str(info[4][0]) for info in infos]
            try:
                public = bool(addresses) and all(
                    ipaddress.ip_address(a).is_global for a in addresses
                )
            except ValueError:
                public = False
            if not public:
                raise FeedError("host is not on the public internet")
            last: Exception | None = None
            for address in dict.fromkeys(addresses):
                try:
                    return super().connect_tcp(
                        address,
                        port,
                        timeout=timeout,
                        local_address=local_address,
                        socket_options=socket_options,
                    )
                except Exception as exc:  # noqa: BLE001 — try the next checked address
                    last = exc
            assert last is not None
            raise last

    return GuardedBackend()


def _peer_is_public(resp: Any) -> bool:
    import ipaddress

    stream = resp.extensions.get("network_stream")
    addr = stream.get_extra_info("server_addr") if stream is not None else None
    if not addr:
        return False  # unknown peer: refuse rather than guess
    try:
        return ipaddress.ip_address(str(addr[0])).is_global
    except ValueError:
        return False


def http_fetch(
    timeout: float = TIMEOUT_SECONDS,
    max_bytes: int = MAX_BYTES,
    deadline: float = FEED_DEADLINE_SECONDS,
) -> Fetch:
    import httpx

    transport = httpx.HTTPTransport(trust_env=False)
    pool = getattr(transport, "_pool", None)
    if pool is None or not hasattr(pool, "_network_backend"):
        # Fail closed: without the guarded backend there is no address check at all.
        raise RuntimeError("httpx transport without a replaceable network backend")
    pool._network_backend = _guarded_backend()
    client = httpx.Client(
        transport=transport,
        follow_redirects=False,
        timeout=timeout,
        headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
        trust_env=False,  # no proxy from the environment can route around the address checks
    )

    def fetch(url: str, headers: dict[str, str]) -> Response:
        if not _https(url):
            raise FeedError("url must be https")
        host = urllib.parse.urlsplit(url).hostname or ""
        if blocked_host(host):
            raise FeedError("host is skip/blocked-legal")
        started = time.monotonic()
        try:
            with client.stream("GET", url, headers=headers) as resp:
                # Second line of defence behind the guarded backend: the peer actually connected
                # to must be public before any byte of the body is read.
                if not _peer_is_public(resp):
                    raise FeedError("connected address is not on the public internet")
                if resp.status_code == 304:
                    return Response(304, b"", final_url=url)
                if resp.status_code in REDIRECTS:
                    # The 3xx body is never read.
                    try:
                        location = urllib.parse.urljoin(
                            url, resp.headers.get("location", "")
                        )
                    except ValueError:
                        raise FeedError(
                            "bad redirect location", status=resp.status_code
                        ) from None
                    return Response(resp.status_code, b"", final_url=location)
                if resp.status_code >= 400:
                    header = resp.headers.get("retry-after")
                    retry_after: float | None = None
                    if header:
                        try:
                            retry_after = float(header)
                        except ValueError:
                            from email.utils import parsedate_to_datetime

                            try:
                                when = parsedate_to_datetime(header)
                                if when.tzinfo is None:
                                    when = when.replace(tzinfo=timezone.utc)
                                retry_after = max(
                                    0.0,
                                    (when - datetime.now(timezone.utc)).total_seconds(),
                                )
                            except (TypeError, ValueError):
                                retry_after = None
                    raise FeedError(
                        f"HTTP {resp.status_code}",
                        status=resp.status_code,
                        retry_after=retry_after,
                    )
                chunks: list[bytes] = []
                size = 0
                for chunk in resp.iter_bytes():
                    size += len(chunk)
                    if size > max_bytes:
                        raise FeedError("body over size cap", status=resp.status_code)
                    if time.monotonic() - started > deadline:
                        # Checked per chunk; a stalled read still ends at the I/O timeout.
                        raise FeedError(
                            "ReadTimeout: feed deadline", status=resp.status_code
                        )
                    chunks.append(chunk)
                return Response(
                    resp.status_code,
                    b"".join(chunks),
                    etag=resp.headers.get("etag"),
                    last_modified=resp.headers.get("last-modified"),
                    final_url=url,
                )
        except FeedError:
            raise
        except (httpx.HTTPError, httpx.InvalidURL, ValueError) as exc:
            raise FeedError(type(exc).__name__) from None

    return fetch  # one shared client; httpx.Client is thread-safe


def _follow(
    fetch: Fetch,
    url: str,
    conditional: tuple[str | None, dict[str, str]] = (None, {}),
    robots: "Robots | None" = None,
) -> Response:
    """Follow redirects one hop at a time. Every hop must stay https and off blocked hosts, and
    with `robots` (html scrapes) be allowed by robots.txt. Conditional headers are sent only to the
    URL that issued them. The returned final_url is the URL that answered."""
    validator_url, validators = conditional
    target = url
    for _ in range(MAX_HOPS + 1):
        if not _https(target):
            raise FeedError("redirect left https")
        if blocked_host(urllib.parse.urlsplit(target).hostname or ""):
            raise FeedError("redirect to a skip/blocked-legal host")
        if robots is not None and not robots.allowed(target):
            raise FeedError("robots.txt disallows the redirect target")
        resp = fetch(target, validators if target == validator_url else {})
        if resp.status not in REDIRECTS:
            resp.final_url = target
            return resp
        target = resp.final_url
    raise FeedError("too many redirects")


class Robots:
    """robots.txt per host, fetched once per run. A host whose robots.txt cannot be read is refused."""

    def __init__(self, fetch: Fetch):
        self._fetch = fetch
        self._cache: dict[str, urllib.robotparser.RobotFileParser | None] = {}
        self._lock = threading.Lock()

    def allowed(self, url: str) -> bool:
        parts = urllib.parse.urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        with self._lock:
            if origin not in self._cache:
                rules = urllib.robotparser.RobotFileParser()
                result: urllib.robotparser.RobotFileParser | None = rules
                try:
                    resp = _follow(self._fetch, origin + "/robots.txt")
                    rules.parse(resp.body.decode("utf-8", "replace").splitlines())
                except FeedError as exc:
                    # 404 means no rules (allowed); anything else (403, timeout) means we do not know.
                    if exc.status == 404 or str(exc) == "HTTP 404":
                        rules.parse([])
                    else:
                        result = None
                except Exception:  # noqa: BLE001 — a broken robots.txt answer refuses this host only
                    result = None
                self._cache[origin] = result
            cached = self._cache[origin]
        return cached is not None and cached.can_fetch(USER_AGENT, url)


# --------------------------------------------------------------------------------------------- parsing


def _clean(text: Any, cap: int) -> str:
    if text is None:
        return ""
    value = WS_RE.sub(" ", TAG_RE.sub(" ", str(text))).strip()
    return value[:cap]


def _link(value: Any, base: str) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    absolute = urllib.parse.urljoin(base, value.strip())
    parts = urllib.parse.urlsplit(absolute)
    if (
        parts.scheme not in ("http", "https")
        or not parts.hostname
        or len(absolute) > LINK_CAP
    ):
        return None
    return absolute


def _iso(value: Any) -> str | None:
    """RFC 822, ISO 8601, epoch seconds/ms or a time.struct_time -> UTC ISO 8601 (seconds)."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, time.struct_time):
            dt = datetime(*value[:6], tzinfo=timezone.utc)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            seconds = value / 1000 if value > 1e11 else value
            dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        elif isinstance(value, str):
            text = value.strip()
            if re.fullmatch(r"\d{10,13}", text):
                return _iso(int(text))
            try:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                from email.utils import parsedate_to_datetime

                dt = parsedate_to_datetime(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            return None
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    except (ValueError, TypeError, OverflowError, OSError, IndexError):
        return None


def _refuse_entities(body: bytes) -> None:
    # No DTD-declared entities (defusedxml is not in the lock): blocks entity-expansion bombs and
    # external entities before any XML parser sees the body. Feeds never need their own entities.
    if b"<!ENTITY" in body.upper():
        raise FeedError("XML entity declaration refused")


def parse_rss(body: bytes, feed: Feed, base: str | None = None) -> list[dict[str, Any]]:
    """RSS 0.9x/1.0/2.0 and Atom through feedparser; the author is never kept (personal data)."""
    import io

    import feedparser

    _refuse_entities(body)
    # A stream, never bytes/str: feedparser treats a str as a URL or a file path.
    parsed = feedparser.parse(
        io.BytesIO(body), sanitize_html=False, resolve_relative_uris=False
    )
    if not parsed.get("version"):
        # An HTML error or maintenance page answered 200: not a feed, so `seen` must not be reset.
        raise FeedError("not a feed")
    import xml.sax

    if not parsed.entries and isinstance(
        parsed.get("bozo_exception"), xml.sax.SAXException
    ):
        # Truncated or broken XML with no entries is a failure, not an empty feed (an encoding
        # warning on a feed that still has entries is fine).
        raise FeedError("malformed feed")
    base = base or feed.url
    items = []
    for entry in parsed.entries:
        link = _link(entry.get("link"), base)
        title = _clean(entry.get("title"), TITLE_CAP)
        guid = entry.get("id") or link or title or None
        published = _iso(entry.get("published_parsed") or entry.get("updated_parsed"))
        extra: dict[str, Any] = {}
        tags = [
            t.get("term")
            for t in entry.get("tags") or []
            if isinstance(t, dict) and t.get("term")
        ]
        if tags:
            extra["tags"] = [_clean(t, 80) for t in tags[:10]]
        if feed.keep == "full":
            summary = _clean(entry.get("summary"), SUMMARY_CAP)
            if summary:
                extra["summary"] = summary
        items.append(
            {
                "guid": str(guid) if guid else None,
                "title": title,
                "link": link,
                "published_at": published,
                "extra": extra,
            }
        )
    return items


PATH_TOKEN = re.compile(r'\["([^"]+)"\]|([^.\[\]]+)')


def _path(path: str) -> list[str]:
    """`a.b.0.c`, with `["key.with.dots"]` for a key that contains dots or slashes."""
    return [quoted or plain for quoted, plain in PATH_TOKEN.findall(path)]


def _dig(value: Any, path: str | None) -> Any:
    if path in (None, "", "."):
        return value
    for part in _path(str(path)):
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            return None
    return value


def _template(template: str, row: Any) -> str:
    return re.sub(
        r"\{([A-Za-z0-9_.]+)\}",
        lambda m: urllib.parse.quote(str(_dig(row, m.group(1)) or ""), safe=":/"),
        template,
    )


def parse_json(
    body: bytes, feed: Feed, base: str | None = None
) -> list[dict[str, Any]]:
    try:
        doc = json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise FeedError("not JSON") from None
    spec = feed.spec
    if spec.get("snapshot"):
        # The whole document is one record; it is new only when its content changes.
        payload = _dig(doc, spec.get("path"))
        if payload is None:
            raise FeedError("snapshot path is empty")
        text = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        if len(text) > MAX_BYTES:
            raise FeedError("snapshot over size cap")
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        return [
            {
                "guid": digest,
                "title": feed.name,
                "link": None,
                "published_at": _iso(_dig(doc, spec.get("time"))),
                "extra": {"payload": payload},
            }
        ]
    rows = _dig(doc, spec.get("items"))
    if isinstance(rows, dict) and spec.get("items_are_values"):
        rows = [
            dict(v, _key=k) if isinstance(v, dict) else {"_key": k, "value": v}
            for k, v in rows.items()
        ]
    if not isinstance(rows, list):
        raise FeedError("items path is not a list")
    base = base or feed.url
    items: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            items.append(
                {"guid": None}
            )  # counted as skipped, never merged into another item
            continue
        ids = spec.get("id") or []
        ids = [ids] if isinstance(ids, str) else ids
        parts = [_dig(row, p) for p in ids]
        if ids and any(part is None or part == "" for part in parts):
            items.append(
                {"guid": None}
            )  # a missing id field must not collapse rows into "None|None"
            continue
        guid = "|".join(str(part) for part in parts) if ids else None
        title = (
            _clean(_dig(row, spec.get("title")), TITLE_CAP) if spec.get("title") else ""
        )
        link_value = (
            _template(spec["link_template"], row)
            if spec.get("link_template")
            else _dig(row, spec.get("link"))
        )
        link = _link(link_value, base)
        if not guid:
            guid = link or json.dumps(row, sort_keys=True, ensure_ascii=False)
        changes = spec.get("changes")
        if changes:
            # A revised value (World Bank back-fills, a KEV due date) is a new version of the item:
            # the content of these fields is part of the guid, so the revision lands as a new row.
            version = json.dumps(
                [_dig(row, c) for c in changes], sort_keys=True, ensure_ascii=False
            )
            guid = f"{guid}#{hashlib.sha256(version.encode('utf-8', 'surrogatepass')).hexdigest()[:12]}"
        keep = spec.get("keep")
        if keep == "*" or (keep is None and feed.keep == "full"):
            extra = {"row": row}
        else:
            extra = {p: _dig(row, p) for p in keep or [] if _dig(row, p) is not None}
        items.append(
            {
                "guid": guid,
                "title": title,
                "link": link,
                "published_at": _iso(_dig(row, spec.get("time"))),
                "extra": extra,
            }
        )
    return items


def parse_html(
    body: bytes, feed: Feed, base: str | None = None
) -> list[dict[str, Any]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(body, "html.parser")
    spec = feed.spec
    base = base or feed.url
    items = []
    for node in soup.select(spec["item"]):
        title_node = node.select_one(spec["title"]) if spec.get("title") else node
        link_node = (
            node.select_one(spec["link"])
            if spec.get("link")
            else (node if node.name == "a" else node.find("a"))
        )
        title = _clean(title_node.get_text(" ") if title_node else "", TITLE_CAP)
        href = link_node.get("href") if link_node else None
        link = _link(href if isinstance(href, str) else None, base)
        if not (title or link):
            continue
        extra = {}
        for name, selector in (spec.get("fields") or {}).items():
            found = node.select_one(selector)
            if found:
                extra[name] = _clean(found.get_text(" "), 300)
        items.append(
            {
                "guid": link or title,
                "title": title,
                "link": link,
                "published_at": None,
                "extra": extra,
            }
        )
    if not items:
        # A trending page always has entries: none means the layout changed, not an empty day.
        raise FeedError("html selector matched nothing")
    return items


PARSERS = {"rss": parse_rss, "json": parse_json, "html": parse_html}


# --------------------------------------------------------------------------------------------- run


def _hash(guid: str) -> str:
    return hashlib.sha256(guid.encode("utf-8")).hexdigest()[:16]


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def due(feed: Feed, state: dict[str, Any], now: datetime) -> bool:
    last = (state.get(feed.id) or {}).get("last_fetch")
    if not last:
        return True
    try:
        then = datetime.fromisoformat(last)
    except ValueError:
        return True
    return now - then >= timedelta(hours=feed.every_hours) - DUE_SLACK


@dataclass
class FeedResult:
    feed: Feed
    status: str  # ok | not-modified | error | robots-denied
    http_status: int | None
    items: list[dict[str, Any]]
    error: str | None
    duration_ms: int
    bytes: int
    etag: str | None = None
    last_modified: str | None = None
    skipped: int = 0  # rows without a usable id
    truncated: int = 0  # items past the feed's max_items
    final_url: str | None = None  # the URL that answered (validators belong to it)


def _get(
    fetch: Fetch,
    url: str,
    conditional: tuple[str | None, dict[str, str]],
    retry_pause: float,
    robots: "Robots | None" = None,
    stop_at: float | None = None,
) -> Response:
    """One retry for a transient failure (network blip, 429/5xx); everything else fails at once.
    No retry once the run budget would be passed."""
    try:
        return _follow(fetch, url, conditional, robots)
    except FeedError as exc:
        if not str(exc).startswith(TRANSIENT):
            raise
        if stop_at is not None and time.monotonic() + retry_pause >= stop_at:
            raise
        if exc.retry_after is not None and exc.retry_after > retry_pause:
            raise  # the host asked for a longer pause than ours: try again next run
    time.sleep(retry_pause)
    return _follow(fetch, url, conditional, robots)


def _encodable(items: list[dict[str, Any]]) -> None:
    """Lone surrogates from json.loads would crash hashing / JSONL later, outside this feed's try."""
    try:
        json.dumps(items, ensure_ascii=False).encode("utf-8")
    except UnicodeEncodeError:
        raise FeedError("text is not valid UTF-8") from None


def rule_fingerprint(feed: Feed) -> str:
    """How a feed is read (URL, kind, parser spec, keep, max_items). Validators issued under another
    rule are not sent, so a parser or registry change re-reads the body instead of getting a 304."""
    rule = json.dumps(
        [PARSER_VERSION, feed.url, feed.kind, feed.spec, feed.keep, feed.max_items],
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(rule.encode("utf-8")).hexdigest()[:12]


def fetch_feed(
    feed: Feed,
    fetch: Fetch,
    feed_state: dict[str, Any],
    robots: Robots | None,
    retry_pause: float = RETRY_PAUSE_SECONDS,
    stop_at: float | None = None,
) -> FeedResult:
    started = time.monotonic()

    def done(status: str, **kw: Any) -> FeedResult:
        return FeedResult(
            feed=feed,
            status=status,
            duration_ms=int((time.monotonic() - started) * 1000),
            **kw,
        )

    try:
        denied = (
            feed.kind == "html" and robots is not None and not robots.allowed(feed.url)
        )
    except Exception:  # noqa: BLE001 — never let one host's robots.txt stop the run
        denied = True
    if denied:
        return done(
            "robots-denied",
            http_status=None,
            items=[],
            error="robots.txt disallows",
            bytes=0,
        )
    headers = {}
    same_rule = feed_state.get("rule") in (None, rule_fingerprint(feed))
    # Validators belong to the URL that issued them (after redirects); they go to that URL only.
    validator_url = feed_state.get("validator_url") or feed_state.get("url") or feed.url
    if same_rule and feed_state.get("etag"):
        headers["If-None-Match"] = feed_state["etag"]
    if same_rule and feed_state.get("last_modified"):
        headers["If-Modified-Since"] = feed_state["last_modified"]
    answered: int | None = None
    try:
        resp = _get(
            fetch,
            feed.url,
            (validator_url, headers),
            retry_pause,
            robots if feed.kind == "html" else None,
            stop_at,
        )
        answered = resp.status
        if resp.status == 304:
            return done(
                "not-modified",
                http_status=304,
                items=[],
                error=None,
                bytes=0,
                etag=feed_state.get("etag"),
                last_modified=feed_state.get("last_modified"),
                final_url=resp.final_url,
            )
        parsed = PARSERS[feed.kind](resp.body, feed, resp.final_url or feed.url)
        usable = [i for i in parsed if i.get("guid")]
        if len(usable) > feed.max_items and all(i.get("published_at") for i in usable):
            usable.sort(
                key=lambda i: str(i["published_at"]), reverse=True
            )  # keep the newest
        items = usable[: feed.max_items]
        _encodable(items)
    except FeedError as exc:
        return done(
            "error",
            http_status=exc.status or answered,
            items=[],
            error=str(exc)[:120],
            bytes=0,
        )
    except Exception as exc:  # noqa: BLE001 — one broken feed must never stop the run
        return done(
            "error", http_status=answered, items=[], error=type(exc).__name__, bytes=0
        )
    return done(
        "ok",
        http_status=resp.status,
        items=items,
        error=None,
        bytes=len(resp.body),
        etag=resp.etag,
        last_modified=resp.last_modified,
        skipped=len(parsed) - len(usable),
        truncated=len(usable) - len(items),
        final_url=resp.final_url,
    )


def _overdue_first(feeds: list[Feed], state: dict[str, Any]) -> list[Feed]:
    """Never-fetched feeds first, then the longest wait: a feed deferred by the run budget starts
    early next time instead of queueing behind the same hourly feeds again."""
    return sorted(
        feeds, key=lambda f: str((state.get(f.id) or {}).get("last_fetch") or "")
    )


def fetch_all(
    feeds: list[Feed],
    fetch: Fetch,
    state: dict[str, Any],
    *,
    workers: int = MAX_WORKERS,
    pause: float = HOST_PAUSE_SECONDS,
    budget: float = RUN_BUDGET_SECONDS,
    grace: float = FEED_DEADLINE_SECONDS + TIMEOUT_SECONDS,
) -> list[FeedResult]:
    """Hosts in parallel, feeds of one host one after another with a pause (politeness).

    No feed starts after `budget` seconds, and the call returns at `budget + grace` whatever is
    still in flight: workers are daemon threads, so a stalled connection is abandoned (it ends with
    the process) instead of holding the run past the workflow timeout. Feeds not finished by then are
    left out of the results; their state is untouched and they stay due, first in line next run."""
    import queue

    robots = Robots(fetch)
    stop_at = time.monotonic() + budget
    by_host: dict[str, list[Feed]] = {}
    for feed in _overdue_first(feeds, state):
        by_host.setdefault(feed.host, []).append(feed)
    groups: queue.Queue[list[Feed]] = queue.Queue()
    for group in by_host.values():
        groups.put(group)
    results: list[FeedResult] = []
    started: dict[str, Feed] = {}
    lock = threading.Lock()

    def worker() -> None:
        while True:
            try:
                group = groups.get_nowait()
            except queue.Empty:
                return
            for index, feed in enumerate(group):
                if index:
                    time.sleep(max(pause, HOST_MIN_PAUSE.get(feed.host, 0.0)))
                if time.monotonic() >= stop_at:
                    break
                with lock:
                    started[feed.id] = feed
                res = fetch_feed(
                    feed,
                    fetch,
                    state.get(feed.id) or {},
                    robots,
                    retry_pause=pause * 3,
                    stop_at=stop_at,
                )
                with lock:
                    results.append(res)

    threads = [
        threading.Thread(target=worker, daemon=True, name=f"open-feeds-{n}")
        for n in range(max(1, min(workers, len(by_host))))
    ]
    for thread in threads:
        thread.start()
    hard_stop = stop_at + grace
    for thread in threads:
        thread.join(max(0.0, hard_stop - time.monotonic()))
    with lock:
        done = list(results)
        finished = {r.feed.id for r in done}
        # A feed still in flight at the hard stop is recorded as an error: its last_fetch moves on,
        # so it does not jump the queue again next run and starve the others.
        done += [
            FeedResult(
                feed=f,
                status="error",
                http_status=None,
                items=[],
                error="abandoned at the run deadline",
                duration_ms=int((budget + grace) * 1000),
                bytes=0,
            )
            for fid, f in started.items()
            if fid not in finished
        ]
    order = {feed.id: i for i, feed in enumerate(feeds)}
    return sorted(done, key=lambda r: order[r.feed.id])


def build_rows(
    results: Iterable[FeedResult], state: dict[str, Any], fetched_at: str
) -> tuple[list[dict], list[dict], dict]:
    """New item rows, run rows and the next state. Pure: no I/O."""
    items: list[dict[str, Any]] = []
    runs: list[dict[str, Any]] = []
    next_state = {k: dict(v) for k, v in state.items()}
    for res in results:
        feed = res.feed
        prev = next_state.get(feed.id) or {}
        seen = list(prev.get("seen") or [])
        # After a parser or registry rule change the current items are re-sent with the new
        # normalisation (a replay); report/Silver keep the newest row per id.
        replay = res.status == "ok" and prev.get("rule") not in (
            None,
            rule_fingerprint(feed),
        )
        seen_set = set() if replay else set(seen)
        fresh: list[str] = []
        current: list[str] = []
        per_day = bool(feed.spec.get("per_day"))
        for item in res.items:
            # per_day: one observation per UTC day (a repo trending again tomorrow, HN points of
            # today), instead of one row for the item's whole life.
            h = _hash(f"{item['guid']}@{fetched_at[:10]}" if per_day else item["guid"])
            current.append(h)
            if h in seen_set or h in fresh:
                continue
            fresh.append(h)
            items.append(
                {
                    "id": f"{feed.id}:{h}",
                    "feed_id": feed.id,
                    "category": feed.category,
                    "subcategory": feed.subcategory,
                    "kind": feed.kind,
                    "lang": feed.lang,
                    "title": item.get("title") or "",
                    "link": item.get("link"),
                    "published_at": item.get("published_at"),
                    "fetched_at": fetched_at,
                    "data": json.dumps(
                        item.get("extra") or {},
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
            )
        entry = dict(prev)
        entry["last_fetch"] = fetched_at
        # Keep the hashes of what the feed shows now (an item that drops out and comes back later is
        # rare and lands again under the same id). A 304 or a failed fetch keeps the old list.
        entry["seen"] = list(dict.fromkeys(current)) if res.status == "ok" else seen
        if res.status in ("ok", "not-modified"):
            entry["last_ok"] = fetched_at
            entry["errors"] = 0
            entry["etag"] = res.etag
            entry["last_modified"] = res.last_modified
            entry["validator_url"] = res.final_url or feed.url
            entry["rule"] = rule_fingerprint(feed)
            entry.pop("url", None)
        else:
            entry["errors"] = int(prev.get("errors") or 0) + 1
        next_state[feed.id] = entry
        runs.append(
            {
                "id": f"{feed.id}@{fetched_at}",
                "feed_id": feed.id,
                "category": feed.category,
                "fetched_at": fetched_at,
                "status": res.status,
                "http_status": res.http_status,
                "error": res.error,
                "items_seen": len(res.items),
                "items_new": len(fresh),
                "items_skipped": res.skipped,
                "items_truncated": res.truncated,
                "duration_ms": res.duration_ms,
                "bytes": res.bytes,
                "consecutive_errors": entry["errors"],
            }
        )
    return items, runs, next_state


def lake_fingerprint(data_lake_uri: str) -> str:
    """Names the lake a state belongs to without storing the URI or the endpoint (no bucket or
    account names in the cache): the same s3:// URI on MinIO and on R2 are different lakes."""
    endpoint = ""
    if data_lake_uri.startswith("s3://"):
        endpoint = (
            os.environ.get("DATA_LAKE_S3_ENDPOINT")
            or os.environ.get("AWS_ENDPOINT_URL")
            or ""
        )
    uri = data_lake_uri.strip().rstrip("/")
    endpoint = endpoint.strip().rstrip("/")
    key = (
        f"{uri}|{endpoint}" if endpoint else uri
    )  # no endpoint: the original (URI-only) value
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


LEGACY_FINGERPRINT_UNTIL = datetime(2026, 11, 20, tzinfo=timezone.utc)


def _legacy_fingerprint(data_lake_uri: str) -> str | None:
    """URI-only fingerprint of states written before 2026-11-18 17:00 UTC (only the live R2 run
    wrote them). Accepted until LEGACY_FINGERPRINT_UNTIL so the live state carries over once; after
    that an endpoint-less match is never trusted for an S3 lake."""
    if _now() >= LEGACY_FINGERPRINT_UNTIL:
        return None
    return hashlib.sha256(
        data_lake_uri.strip().rstrip("/").encode("utf-8")
    ).hexdigest()[:16]


def read_state(
    path: Path, lake: str | None = None, legacy: str | None = None
) -> dict[str, Any]:
    """The state file, or {} when it is missing, unreadable or belongs to another lake (the run
    then re-sends current items)."""
    import gzip

    try:
        doc = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, EOFError, ValueError):
        return {}
    if not isinstance(doc, dict) or (
        lake is not None and doc.get("lake") not in {lake, legacy or lake}
    ):
        return {}
    feeds = doc.get("feeds")
    return feeds if isinstance(feeds, dict) else {}


def write_state(
    path: Path, feeds: dict[str, Any], updated: str, lake: str | None = None
) -> None:
    import gzip

    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(
        {"version": 1, "updated": updated, "lake": lake, "feeds": feeds},
        sort_keys=True,
        separators=(",", ":"),
    )
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(gzip.compress(body.encode("utf-8"), mtime=0))
    os.replace(tmp, path)


def batches(
    rows: list[dict[str, Any]], limit: int = BATCH_BYTES
) -> list[list[dict[str, Any]]]:
    """Split rows so each Bronze batch's JSONL stays under `limit` bytes (a cache miss can bring
    tens of thousands of items at once)."""
    out: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    size = 0
    for row in rows:
        n = len(json.dumps(row, ensure_ascii=False, sort_keys=True).encode("utf-8")) + 1
        if current and size + n > limit:
            out.append(current)
            current, size = [], 0
        current.append(row)
        size += n
    if current:
        out.append(current)
    return out


def capture(
    feeds: list[Feed],
    fetch: Fetch,
    data_lake_uri: str,
    *,
    only: set[str] | None = None,
    state_path: Path = DEFAULT_STATE,
    now: datetime | None = None,
    dry_run: bool = False,
    force: bool = False,
    workers: int = MAX_WORKERS,
    pause: float = HOST_PAUSE_SECONDS,
    budget: float = RUN_BUDGET_SECONDS,
) -> dict[str, Any]:
    from data_lake.ingest import ingest_records

    now = now or _now()
    fetched_at = now.isoformat(timespec="seconds")
    lake = lake_fingerprint(data_lake_uri)
    state = read_state(state_path, lake, _legacy_fingerprint(data_lake_uri))
    active = [f for f in feeds if f.enabled and (only is None or f.id in only)]
    todo = [f for f in active if force or due(f, state, now)]
    results = fetch_all(todo, fetch, state, workers=workers, pause=pause, budget=budget)
    items, runs, next_state = build_rows(results, state, fetched_at)
    counts: defaultdict[str, int] = defaultdict(int)
    for r in results:
        counts[r.status] += 1
    enabled = [f for f in feeds if f.enabled]
    # Health from the state after this run, so a re-run with nothing due cannot hide a broken hour.
    failing = sum(
        1 for f in enabled if int((next_state.get(f.id) or {}).get("errors") or 0) > 0
    )
    report: dict[str, Any] = {
        "status": "dry-run" if dry_run else "captured",
        "fetched_at": fetched_at,
        "state_loaded": bool(state),
        "feeds_registered": len(feeds),
        "feeds_due": len(todo),
        "feeds_deferred": len(todo) - len(results),
        "ok": counts["ok"],
        "not_modified": counts["not-modified"],
        "errors": counts["error"],
        "robots_denied": counts["robots-denied"],
        "items_new": len(items),
        "items_skipped": sum(r.skipped for r in results),
        "items_truncated": sum(r.truncated for r in results),
        "failed_feeds": sorted(r.feed.id for r in results if r.status == "error"),
        "feeds_failing": failing,
        "feeds_enabled": len(enabled),
    }
    if dry_run:
        return report
    manifests: dict[str, list[str]] = {}
    common = dict(
        source=SOURCE,
        domain=DOMAIN,
        data_lake_uri=data_lake_uri,
        source_record_id_field="id",
        event_time_field="fetched_at",
        privacy_class="public",
        retention_class="operational",
    )
    for dataset, rows, meta in (
        (DATASET_ITEMS, items, {"provider": SOURCE, "registry": "open-feeds.yaml"}),
        (DATASET_RUNS, runs, {"provider": SOURCE}),
    ):
        for batch in batches(rows):
            res = ingest_records(batch, dataset=dataset, metadata=meta, **common)
            manifests.setdefault(dataset, []).append(res["manifest_key"])
    # State last: if this write fails the next run re-sends the same items, and Silver keeps one row
    # per `id` (source_record_id), so the worst case is a duplicate Bronze row, never a lost item.
    # Rows of feeds removed from the registry are dropped here (the full registry decides, never
    # an --only selection).
    known = {f.id for f in feeds}
    write_state(
        state_path,
        {k: v for k, v in next_state.items() if k in known},
        fetched_at,
        lake,
    )
    report["manifests"] = manifests
    return report


# --------------------------------------------------------------------------------------------- probe / import


def probe(
    feeds: list[Feed], fetch: Fetch, workers: int = MAX_WORKERS
) -> list[dict[str, Any]]:
    results = fetch_all(feeds, fetch, {}, workers=workers, pause=0.3)
    return [
        {
            "id": r.feed.id,
            "status": r.status,
            "items": len(r.items),
            "error": r.error,
            "newest": max(
                (i["published_at"] for i in r.items if i.get("published_at")),
                default=None,
            ),
        }
        for r in results
    ]


def slug(text: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return value[:60] or "feed"


def opml_rows(
    body: bytes, category: str, lang: str | None = None
) -> list[dict[str, Any]]:
    _refuse_entities(body)
    root = ET.fromstring(body)  # noqa: S314 — entities refused above; OPML is a CC0 list we chose
    rows = []
    taken: set[str] = set()
    for node in root.iter("outline"):
        url = node.get("xmlUrl")
        if not url:
            continue
        url = url.strip()
        if url.startswith("http://"):
            url = "https://" + url[len("http://") :]
        if not _https(url) or blocked_host(urllib.parse.urlsplit(url).hostname or ""):
            continue
        name = node.get("title") or node.get("text") or url
        fid = slug(name)
        if (
            len(fid) < 2 or fid == "feed"
        ):  # e.g. a Thai-only title: name it after the URL instead
            parts = urllib.parse.urlsplit(url)
            fid = slug(f"{parts.hostname or ''} {parts.path}")
        base_id, n = fid[:60], 2
        while fid in taken:
            fid = f"{base_id}-{n}"
            n += 1
        taken.add(fid)
        row: dict[str, Any] = {
            "id": fid,
            "name": name.strip(),
            "kind": "rss",
            "url": url,
            "category": category,
        }
        if lang:
            row["lang"] = lang
        rows.append(row)
    return rows


# --------------------------------------------------------------------------------------------- report


def committed_parts(
    data_lake_uri: str,
    dataset: str,
    days: int | None = None,
    manifests: dict[str, bytes] | None = None,
) -> list[str]:
    """Parquet parts of `dataset` that a commit manifest names (ADR-0001: the manifest is the commit
    marker, so a part left by a run that stopped before its manifest is never read). Pass the
    `manifests` of one listing to reuse it for several datasets."""
    from data_lake.storage import ObjectStore

    store = ObjectStore(data_lake_uri)
    if manifests is None:
        manifests = store.get_prefix(f"control/manifests/source={SOURCE}/")
    cutoff = None
    if days is not None:
        cutoff = (_now() - timedelta(days=days + 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    parts = []
    for key, body in sorted(manifests.items()):
        if "/batch_id=" not in key or not key.endswith(".json"):
            continue
        try:
            manifest = json.loads(body)
        except ValueError:
            continue
        if manifest.get("domain") != DOMAIN or manifest.get("dataset") != dataset:
            continue
        if cutoff and str(manifest.get("received_at") or "") < cutoff:
            continue
        bronze_key = (manifest.get("bronze") or {}).get("key")
        if bronze_key:
            uri = store.object_uri(bronze_key)
            parts.append(
                urllib.parse.unquote(uri[len("file://") :])
                if uri.startswith("file://")
                else uri
            )
    return parts


ITEMS_VIEW = """
WITH raw AS (
  SELECT source_record_id AS id, payload_json AS p, received_at FROM lake_table
  QUALIFY row_number() OVER (PARTITION BY source_record_id ORDER BY received_at DESC) = 1
)
SELECT id,
       json_extract_string(p, '$.feed_id') AS feed_id,
       json_extract_string(p, '$.category') AS category,
       json_extract_string(p, '$.title') AS title,
       json_extract_string(p, '$.link') AS link,
       TRY_CAST(json_extract_string(p, '$.published_at') AS TIMESTAMPTZ) AS published_at,
       TRY_CAST(json_extract_string(p, '$.fetched_at') AS TIMESTAMPTZ) AS fetched_at
FROM raw
"""

RUNS_FAILING = """
WITH r AS (SELECT json_extract_string(payload_json, '$.feed_id') AS feed_id,
                  json_extract_string(payload_json, '$.status') AS status,
                  json_extract_string(payload_json, '$.error') AS error,
                  TRY_CAST(json_extract_string(payload_json, '$.http_status') AS INTEGER) AS http_status,
                  TRY_CAST(json_extract_string(payload_json, '$.consecutive_errors') AS INTEGER) AS streak,
                  json_extract_string(payload_json, '$.fetched_at') AS fetched_at FROM lake_table
           QUALIFY row_number() OVER (PARTITION BY feed_id ORDER BY fetched_at DESC) = 1)
SELECT feed_id, error, http_status, streak FROM r WHERE status IN ('error', 'robots-denied')
ORDER BY streak DESC, feed_id
"""


def report(
    data_lake_uri: str, *, days: int = 7, search: str | None = None, limit: int = 30
) -> dict[str, Any]:
    """Counts by category and feed over the last `days` days (one row per item id), the feeds failing
    on their latest run, and a title search. Reads committed parts only."""
    from data_lake.query import query_parquet

    since = f"now() - INTERVAL {int(days)} DAY"
    view = f"WITH items AS ({ITEMS_VIEW}) "
    from data_lake.storage import ObjectStore

    out: dict[str, Any] = {"days": days}
    listing = ObjectStore(data_lake_uri).get_prefix(
        f"control/manifests/source={SOURCE}/"
    )
    items = committed_parts(data_lake_uri, DATASET_ITEMS, days, listing)
    if items:
        out["totals"] = query_parquet(
            items,
            view
            + f"""
            SELECT count(*) AS items, count(DISTINCT feed_id) AS feeds, min(fetched_at)::VARCHAR AS first_fetch,
                   max(fetched_at)::VARCHAR AS last_fetch FROM items WHERE fetched_at >= {since}""",
        )[0]
        out["by_category"] = query_parquet(
            items,
            view
            + f"""
            SELECT category, count(*) AS items, count(DISTINCT feed_id) AS feeds FROM items
            WHERE fetched_at >= {since} GROUP BY 1 ORDER BY 2 DESC""",
        )
        out["top_feeds"] = query_parquet(
            items,
            view
            + f"""
            SELECT feed_id, category, count(*) AS items FROM items WHERE fetched_at >= {since}
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT {int(limit)}""",
        )
    else:
        out["totals"] = {
            "items": 0,
            "feeds": 0,
            "first_fetch": None,
            "last_fetch": None,
        }
        out["by_category"], out["top_feeds"] = [], []
    if search:
        out["search"] = (
            query_parquet(
                items,
                view
                + f"""
                SELECT coalesce(published_at, fetched_at)::VARCHAR AS at, feed_id, title, link FROM items
                WHERE fetched_at >= {since} AND title ILIKE ? ORDER BY coalesce(published_at, fetched_at) DESC
                LIMIT {int(limit)}""",
                [f"%{search}%"],
            )
            if items
            else []
        )
    runs = committed_parts(data_lake_uri, DATASET_RUNS, days, listing)
    out["failing_feeds"] = query_parquet(runs, RUNS_FAILING) if runs else []
    return out


# --------------------------------------------------------------------------------------------- CLI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    sub = parser.add_subparsers(dest="cmd")
    run = sub.add_parser("run", help="fetch due feeds into Bronze (default)")
    run.add_argument(
        "--data-lake-uri",
        default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_LAKE_URI),
    )
    run.add_argument(
        "--require-cloud",
        action="store_true",
        help="fail unless the lake URI is s3:// (scheduled runs must never fall back to a local path)",
    )
    run.add_argument(
        "--state-file",
        type=Path,
        default=Path(os.environ.get("OPEN_FEEDS_STATE", DEFAULT_STATE)),
    )
    run.add_argument("--dry-run", action="store_true")
    run.add_argument(
        "--force", action="store_true", help="fetch every enabled feed, due or not"
    )
    run.add_argument("--only", help="comma-separated feed ids")
    run.add_argument("--workers", type=int, default=MAX_WORKERS)
    sub.add_parser("check", help="validate the registry offline")
    rp = sub.add_parser(
        "report",
        help="counts by category/feed, failing feeds and a title search (DuckDB on Bronze)",
    )
    rp.add_argument(
        "--data-lake-uri",
        default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_LAKE_URI),
    )
    rp.add_argument("--days", type=int, default=7)
    rp.add_argument("--search")
    rp.add_argument("--limit", type=int, default=30)
    pr = sub.add_parser(
        "probe", help="fetch feeds and print status + item counts (no lake)"
    )
    pr.add_argument("--only", help="comma-separated feed ids")
    pr.add_argument("--workers", type=int, default=MAX_WORKERS)
    op = sub.add_parser(
        "import-opml", help="print registry rows (JSON) for the feeds of an OPML file"
    )
    op.add_argument("file", type=Path)
    op.add_argument("--category", required=True)
    op.add_argument("--lang")
    return parser


def _only(feeds: list[Feed], only: str | None) -> set[str] | None:
    """The --only ids; an id that is not in the registry is an error (a typo must not run nothing)."""
    if not only:
        return None
    wanted = {x.strip() for x in only.split(",") if x.strip()}
    unknown = wanted - {f.id for f in feeds}
    if unknown:
        raise RegistryError(f"unknown feed id(s): {sorted(unknown)}")
    return wanted


def _select(feeds: list[Feed], only: str | None) -> list[Feed]:
    wanted = _only(feeds, only)
    return feeds if wanted is None else [f for f in feeds if f.id in wanted]


def main(
    argv: list[str] | None = None,
    fetch: Fetch | None = None,
    pause: float = HOST_PAUSE_SECONDS,
) -> int:
    parser = build_parser()
    argv = list(sys.argv[1:] if argv is None else argv)
    # `run` is the default subcommand: insert it after the global --registry option when absent.
    head = 2 if argv[:1] == ["--registry"] else 0
    if not (
        set(argv[head : head + 1])
        & {"run", "check", "probe", "report", "import-opml", "-h", "--help"}
    ):
        argv.insert(head, "run")
    args = parser.parse_args(argv)
    cmd = args.cmd or "run"
    try:
        if cmd == "import-opml":
            print(
                json.dumps(
                    opml_rows(args.file.read_bytes(), args.category, args.lang),
                    ensure_ascii=False,
                    indent=1,
                )
            )
            return 0
        if cmd == "report":
            print(
                json.dumps(
                    report(
                        args.data_lake_uri,
                        days=args.days,
                        search=args.search,
                        limit=args.limit,
                    ),
                    ensure_ascii=False,
                    indent=1,
                    default=str,
                )
            )
            return 0
        feeds = load_registry(args.registry)
        if cmd == "check":
            kinds: defaultdict[str, int] = defaultdict(int)
            for f in feeds:
                kinds[f.kind] += 1
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "feeds": len(feeds),
                        "enabled": sum(f.enabled for f in feeds),
                        "kinds": dict(kinds),
                        "categories": len({f.category for f in feeds}),
                    },
                    sort_keys=True,
                )
            )
            return 0
        if cmd == "probe":
            rows = probe(
                _select(feeds, args.only), fetch or http_fetch(), workers=args.workers
            )
            print(json.dumps(rows, ensure_ascii=False, indent=1))
            return 0
        uri = (args.data_lake_uri or "").strip()
        if not uri or (args.require_cloud and not uri.startswith("s3://")):
            print(json.dumps({"status": "error", "error": "LakeUriNotConfigured"}))
            return 2
        result = capture(
            feeds,
            fetch or http_fetch(),
            uri,
            only=_only(feeds, args.only),
            state_path=args.state_file,
            dry_run=args.dry_run,
            force=args.force or bool(args.only),
            workers=args.workers,
            pause=pause,
        )
    except RegistryError as exc:
        print(
            json.dumps(
                {"status": "error", "error": "RegistryError", "detail": str(exc)}
            )
        )
        return 2
    except Exception as exc:  # noqa: BLE001 — public log: the exception type only
        print(json.dumps({"status": "error", "error": type(exc).__name__}))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    attempted = result["feeds_due"] - result["feeds_deferred"]
    enabled = result["feeds_enabled"]
    if result["feeds_due"] and not attempted:
        return 3  # feeds were due but none finished (stalled or out of budget): not a healthy run
    if (attempted and result["errors"] / attempted > FAIL_RATIO) or (
        enabled and result["feeds_failing"] / enabled > FAIL_RATIO
    ):
        # Most feeds failing (this run, or on their latest fetch when nothing was due) = the runner
        # or the network is broken, not one publisher.
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
