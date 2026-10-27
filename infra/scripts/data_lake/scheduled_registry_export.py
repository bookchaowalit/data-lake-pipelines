#!/usr/bin/env python3
"""Hosted run of registry_lake_export (Railway cron `registry-export` in `data-schedules`).

Fetches only the four source files from GitHub (`main` of the private solo-empire and
architecture-docs repositories) with a read-only token, writes them to a temporary
directory and exports them to SOLO_EMPIRE_DATA_LAKE_URI. Prints dataset counts only,
reports a failed run to Sentry (opt-in) and posts the `registry-export` heartbeat to the
uptime Worker. Exit code 0 = exported (or nothing new), 1 = failed.

Environment: GITHUB_TOKEN (contents:read on both repositories), SOLO_EMPIRE_DATA_LAKE_URI,
REGISTRY_REF (default main), HEARTBEAT_TOKEN (optional), plus the R2 variables that
data_lake/storage.py needs for s3:// URIs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.observability import run_with_reporting  # noqa: E402
from data_lake.registry_lake_export import export  # noqa: E402

OWNER = "bookchaowalit"
# name -> (repository, directory, files); names match the export() arguments
SOURCES = {
    "registries": ("solo-empire", "repository-catalog/registries", ("data-sources.yaml", "online-systems.yaml")),
    "catalog": ("architecture-docs", "systems/ai-data/metadata-catalog", ("catalog.yaml", "lineage.yaml")),
}
HEARTBEAT_URL = "https://solo-empire-monitor.bookchaowalit.workers.dev/heartbeat/registry-export"
# Cloudflare answers 403 to the default Python-urllib User-Agent on workers.dev.
USER_AGENT = "registry-export/1"

Fetch = Callable[[str, str, str, str], bytes]


class FetchError(RuntimeError):
    pass


def github_fetch(repo: str, path: str, ref: str, token: str) -> bytes:
    url = f"https://api.github.com/repos/{OWNER}/{repo}/contents/{urllib.parse.quote(path)}?ref={urllib.parse.quote(ref)}"
    request = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github.raw",
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    })
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise FetchError(f"{repo}:{path} HTTP {error.code}") from None
    except urllib.error.URLError as error:
        raise FetchError(f"{repo}:{path} {error.reason}") from None


def post_heartbeat(ok: bool) -> None:
    token = os.environ.get("HEARTBEAT_TOKEN", "").strip()
    if not token:
        return
    body = json.dumps({"ok": ok, "failed": [] if ok else ["registry-export"]}).encode()
    request = urllib.request.Request(HEARTBEAT_URL, data=body, method="POST", headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    })
    try:
        urllib.request.urlopen(request, timeout=15).close()
    except (urllib.error.URLError, OSError):
        print(json.dumps({"event": "heartbeat", "status": "error"}))


def _download(target: Path, fetch: Fetch, ref: str, token: str) -> dict[str, Path]:
    dirs: dict[str, Path] = {}
    for name, (repo, directory, files) in SOURCES.items():
        dirs[name] = target / name
        dirs[name].mkdir()
        for file_name in files:
            (dirs[name] / file_name).write_bytes(fetch(repo, f"{directory}/{file_name}", ref, token))
    return dirs


def run(
    argv: Sequence[str],
    *,
    env: Mapping[str, str],
    fetch: Fetch = github_fetch,
    heartbeat: Callable[[bool], None] = post_heartbeat,
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
                dirs = _download(Path(temp_dir), fetch, env.get("REGISTRY_REF", "main"), token)
            report = export(dirs["registries"], dirs["catalog"], lake_uri, dry_run=args.dry_run)
    except (FetchError, ValueError, OSError) as error:
        print(json.dumps({"status": "failed", "error": type(error).__name__, "detail": str(error)[:200]}))
        beat(False)
        return 1

    counts = {name: {k: v for k, v in result.items() if k in ("records", "new")} for name, result in report["datasets"].items()}
    print(json.dumps({"status": report["status"], "datasets": counts}, sort_keys=True))
    beat(True)
    return 0


def main() -> int:
    return run(sys.argv[1:], env=os.environ)


if __name__ == "__main__":
    sys.exit(run_with_reporting("registry-export", main))
