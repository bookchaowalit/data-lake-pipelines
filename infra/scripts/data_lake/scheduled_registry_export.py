#!/usr/bin/env python3
"""Hosted run of registry_lake_export (Railway cron `registry-export` in `data-schedules`).

Fetches only the five source files: the three registries from GitHub (`main` of the private
solo-empire repository) with a read-only token, and the two metadata-catalog files from the hosted
knowledge store, space architecture-docs (the source of the architecture documents since the
architecture-docs SPEC-001 phase 3 cutover; the Git repository is archived). It writes them to a
temporary directory and exports them to SOLO_EMPIRE_DATA_LAKE_URI. Prints dataset counts only,
reports a failed run to Sentry (opt-in) and posts the `registry-export` heartbeat to the
uptime Worker. After the export it renders the system landscape page (system_landscape.py) and,
when it differs from the store's copy, writes it there as a new version (``if_version``: a page
someone else changed since the read is refused and the run fails). Exit code 0 = exported (or
nothing new), 1 = failed.

Environment: GITHUB_TOKEN (contents:read on solo-empire), SOLO_EMPIRE_DATA_LAKE_URI,
EMPIRE_KNOWLEDGE_API + SOLO_WEB_PUBLIC_ORIGIN + KNOWLEDGE_STORE_KEY + KNOWLEDGE_STORE_SUBJECT
(knowledge_store.py; the store-sync key in Infisical /knowledge-sync),
REGISTRY_REF (default main), HEARTBEAT_TOKEN (optional), plus the R2 variables that
data_lake/storage.py needs for s3:// URIs. With EMPIRE_TASKS_API and CATALOG_EXPORT_TOKEN (the platform-empire
`catalog-export` automation, renewed by token-rotate) it also exports the SPEC-002 catalog (catalog_lake_export.py);
with neither set that step reports "skipped", with only one set the run fails.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import urllib.parse
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake import catalog_lake_export  # noqa: E402
from data_lake.knowledge_store import KnowledgeStore, StoreError, configured  # noqa: E402
from data_lake.observability import run_with_reporting  # noqa: E402
from data_lake.registry_lake_export import export, load_records  # noqa: E402
from data_lake.system_landscape import build, render  # noqa: E402

OWNER = "bookchaowalit"
# name -> (source, directory, files); names match the export() arguments. The source is a GitHub
# repository, or STORE for store space architecture-docs.
STORE = "store:architecture-docs"
STORE_SPACE = "architecture-docs"
SOURCES = {
    "registries": ("solo-empire", "repository-catalog/registries", ("data-sources.yaml", "online-systems.yaml", "business-capabilities.yaml")),
    "catalog": (STORE, "systems/ai-data/metadata-catalog", ("catalog.yaml", "lineage.yaml")),
}
LANDSCAPE_PATH = "operations/platform/SYSTEM-LANDSCAPE-GENERATED.md"
HEARTBEAT_URL = "https://monitor.bookchaowalit.com/heartbeat/registry-export"
# Cloudflare answers 403 to default client User-Agents on workers.dev.
USER_AGENT = "registry-export/1"

Fetch = Callable[[str, str, str, str], bytes]
# publish(text, ref, env) -> "unchanged" | "written"; raises FetchError on failure
Publish = Callable[[str, str, Mapping[str, str]], str]


class FetchError(RuntimeError):
    pass


def github_fetch(repo: str, path: str, ref: str, token: str) -> bytes:
    url = f"https://api.github.com/repos/{OWNER}/{repo}/contents/{urllib.parse.quote(path)}"
    headers = {
        "Accept": "application/vnd.github.raw",
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        response = httpx.get(url, params={"ref": ref}, headers=headers, timeout=30)
    except httpx.HTTPError as error:
        raise FetchError(f"{repo}:{path} {type(error).__name__}") from None
    if response.status_code != 200:
        raise FetchError(f"{repo}:{path} HTTP {response.status_code}")
    return response.content


def store_fetch(env: Mapping[str, str]) -> Callable[[str], bytes]:
    """path -> bytes from store space architecture-docs (a missing document fails the run)."""
    store = KnowledgeStore(env)

    def fetch(path: str) -> bytes:
        try:
            document = store.get(STORE_SPACE, path)
        except StoreError as error:
            raise FetchError(str(error)) from None
        if document is None:
            raise FetchError(f"{STORE}:{path} not found")
        return document.content

    return fetch


def store_publish(text: str, ref: str, env: Mapping[str, str]) -> str:
    """Write the landscape page to store space architecture-docs when it changed. The store has
    one copy (no branches), so only a run from the registries' main branch may write it."""
    if ref != "main":
        return "skipped"
    try:
        store = KnowledgeStore(env)
        current = store.get(STORE_SPACE, LANDSCAPE_PATH)
        content = text.encode("utf-8")
        if current is not None and current.content == content:
            return "unchanged"
        store.put(STORE_SPACE, LANDSCAPE_PATH, content, if_version=current.version if current else 0)
    except StoreError as error:
        raise FetchError(str(error)) from None
    return "written"


def post_heartbeat(ok: bool) -> None:
    token = os.environ.get("HEARTBEAT_TOKEN", "").strip()
    if not token:
        return
    try:
        httpx.post(HEARTBEAT_URL, timeout=15, json={"ok": ok, "failed": [] if ok else ["registry-export"]},
                   headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT})
    except httpx.HTTPError:
        print(json.dumps({"event": "heartbeat", "status": "error"}))


def _download(target: Path, fetch: Fetch, ref: str, token: str, fetch_store: Callable[[str], bytes]) -> dict[str, Path]:
    dirs: dict[str, Path] = {}
    for name, (repo, directory, files) in SOURCES.items():
        dirs[name] = target / name
        dirs[name].mkdir()
        for file_name in files:
            path = f"{directory}/{file_name}"
            data = fetch_store(path) if repo == STORE else fetch(repo, path, ref, token)
            (dirs[name] / file_name).write_bytes(data)
    return dirs


def run(
    argv: Sequence[str],
    *,
    env: Mapping[str, str],
    fetch: Fetch = github_fetch,
    heartbeat: Callable[[bool], None] = post_heartbeat,
    publish: Publish = store_publish,
    catalog_get: catalog_lake_export.Get = catalog_lake_export.http_get,
    fetch_store: Callable[[Mapping[str, str]], Callable[[str], bytes]] = store_fetch,
) -> int:
    parser = argparse.ArgumentParser(description="Hosted registry export into the lake.")
    parser.add_argument("--dry-run", action="store_true", help="fetch and count; write nothing, no heartbeat")
    parser.add_argument("--registries", type=Path, help="local registries directory instead of GitHub")
    parser.add_argument("--catalog", type=Path, help="local metadata-catalog directory instead of GitHub")
    args = parser.parse_args(list(argv))
    beat = (lambda ok: None) if args.dry_run else heartbeat

    token = env.get("GITHUB_TOKEN", "").strip()
    lake_uri = env.get("SOLO_EMPIRE_DATA_LAKE_URI", "").strip()
    local = args.registries is not None and args.catalog is not None
    missing = [name for name, value in (("GITHUB_TOKEN", token or local), ("SOLO_EMPIRE_DATA_LAKE_URI", lake_uri or args.dry_run)) if not value]
    if not local:
        missing += configured(env)  # the catalog source and the landscape page live in the store
    if missing:
        print(json.dumps({"status": "failed", "error": f"missing {', '.join(missing)}"}))
        beat(False)
        return 1

    try:
        with tempfile.TemporaryDirectory(prefix="registry-export-") as temp_dir:
            dirs: dict[str, Path]
            if args.registries is not None and args.catalog is not None:
                dirs = {"registries": args.registries, "catalog": args.catalog}
            else:
                dirs = _download(Path(temp_dir), fetch, env.get("REGISTRY_REF", "main"), token, fetch_store(env))
            report = export(dirs["registries"], dirs["catalog"], lake_uri, dry_run=args.dry_run)
            page = render(build(load_records(dirs["registries"], dirs["catalog"])))
            landscape = "dry-run" if args.dry_run else "local" if local else publish(page, env.get("REGISTRY_REF", "main"), env)
            catalog = catalog_lake_export.export(env, lake_uri, dry_run=args.dry_run, get=catalog_get)
    except (FetchError, StoreError, catalog_lake_export.CatalogError, ValueError, OSError) as error:
        print(json.dumps({"status": "failed", "error": type(error).__name__, "detail": str(error)[:200]}))
        beat(False)
        return 1

    counts = {name: {k: v for k, v in result.items() if k in ("records", "new")} for name, result in report["datasets"].items()}
    for name, result in catalog.get("datasets", {}).items():
        counts[name] = {k: v for k, v in result.items() if k in ("records", "new")}
    print(json.dumps({"status": report["status"], "datasets": counts, "landscape": landscape, "catalog": catalog["status"]}, sort_keys=True))
    beat(True)
    return 0


def main() -> int:
    return run(sys.argv[1:], env=os.environ)


if __name__ == "__main__":
    sys.exit(run_with_reporting("registry-export", main))
