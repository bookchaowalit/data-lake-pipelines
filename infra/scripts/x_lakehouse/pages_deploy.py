"""Upload a built static folder to Cloudflare Pages (Direct Upload), the way ``wrangler pages deploy`` does.

Python only, so the Railway image needs no Node. Steps: project upload token (JWT) → which asset hashes
are missing → upload those (base64, batched) → upsert all hashes → create the deployment with the
manifest (path → hash) plus ``_headers``/``_redirects`` as files. The deploy mark goes in the commit
message; when the live deployment already carries it, nothing is uploaded (spec x-insights B5).

Asset keys are content hashes (sha256 of base64 body + extension, 32 hex) — same shape as wrangler's
blake3 keys, which the API stores as given. Logs and errors carry counts, ids and HTTP status only.
"""

from __future__ import annotations

import base64
import hashlib
import json
import mimetypes
from pathlib import Path
from typing import Any

import httpx

API = "https://api.cloudflare.com/client/v4"
SPECIAL = ("_headers", "_redirects", "_routes.json", "_worker.js")
BATCH_BYTES = 40 * 1024 * 1024
BATCH_FILES = 1000


class PagesError(RuntimeError):
    pass


def _ok(resp: httpx.Response, what: str) -> Any:
    try:
        body = resp.json()
    except ValueError:
        body = {}
    if resp.status_code >= 400 or not body.get("success", False):
        codes = [e.get("code") for e in body.get("errors", []) if isinstance(e, dict)]
        raise PagesError(f"{what}: http {resp.status_code} {codes}")
    return body.get("result")


def asset_key(data: bytes, name: str) -> str:
    ext = Path(name).suffix.lstrip(".")
    return hashlib.sha256(base64.b64encode(data) + ext.encode()).hexdigest()[:32]


def collect(out_dir: Path) -> tuple[dict[str, tuple[str, bytes, str]], dict[str, bytes]]:
    """``{"/path": (key, bytes, content_type)}`` for assets, ``{name: bytes}`` for the special files."""
    assets: dict[str, tuple[str, bytes, str]] = {}
    special: dict[str, bytes] = {}
    for p in sorted(x for x in out_dir.rglob("*") if x.is_file()):
        rel = p.relative_to(out_dir).as_posix()
        data = p.read_bytes()
        if rel in SPECIAL:
            special[rel] = data
            continue
        ctype = mimetypes.guess_type(rel)[0] or "application/octet-stream"
        assets["/" + rel] = (asset_key(data, rel), data, ctype)
    return assets, special


def live_mark(http: httpx.Client, account: str, project: str, token: str, branch: str = "main") -> str:
    res = _ok(http.get(f"{API}/accounts/{account}/pages/projects/{project}",
                       headers={"Authorization": f"Bearer {token}"}), "project")
    # The latest deployment counts too unless it failed: right after a deploy, canonical can still be the
    # previous one while the new one finishes, and a second upload of the same build would be wasted.
    # Only a production deployment of the same branch counts: a preview carrying the mark is not live.
    latest = (res or {}).get("latest_deployment") or {}
    meta = (latest.get("deployment_trigger") or {}).get("metadata") or {}
    if (latest.get("environment") == "production" and meta.get("branch") == branch
            and ((latest.get("latest_stage") or {}).get("status") or "") not in ("failure", "canceled")):
        mark = _mark(latest)
        if mark:
            return mark
    return _mark((res or {}).get("canonical_deployment") or {})


def _mark(dep: dict[str, Any]) -> str:
    return str(((dep.get("deployment_trigger") or {}).get("metadata") or {}).get("commit_message") or "")


def deploy(out_dir: Path, *, account: str, project: str, token: str, mark: str, branch: str = "main",
           http: httpx.Client | None = None, force: bool = False) -> dict[str, Any]:
    own = http is None
    client = http or httpx.Client(timeout=60)
    try:
        if not force and live_mark(client, account, project, token, branch) == mark:
            return {"status": "unchanged", "mark": mark}
        jwt = _ok(client.get(f"{API}/accounts/{account}/pages/projects/{project}/upload-token",
                           headers={"Authorization": f"Bearer {token}"}), "upload-token")["jwt"]
        auth = {"Authorization": f"Bearer {jwt}"}
        assets, special = collect(out_dir)
        by_key = {k: (data, ctype) for k, data, ctype in assets.values()}
        missing = _ok(client.post(f"{API}/pages/assets/check-missing", headers=auth,
                                json={"hashes": sorted(by_key)}), "check-missing") or []
        batch: list[dict[str, Any]] = []
        size = 0
        uploaded = 0
        for key in sorted(missing):
            data, ctype = by_key[key]
            item = {"key": key, "value": base64.b64encode(data).decode(), "metadata": {"contentType": ctype},
                    "base64": True}
            if batch and (size + len(item["value"]) > BATCH_BYTES or len(batch) >= BATCH_FILES):
                _ok(client.post(f"{API}/pages/assets/upload", headers=auth, json=batch), "upload")
                uploaded += len(batch)
                batch, size = [], 0
            batch.append(item)
            size += len(item["value"])
        if batch:
            _ok(client.post(f"{API}/pages/assets/upload", headers=auth, json=batch), "upload")
            uploaded += len(batch)
        _ok(client.post(f"{API}/pages/assets/upsert-hashes", headers=auth, json={"hashes": sorted(by_key)}),
            "upsert-hashes")
        manifest = {path: key for path, (key, _, _) in assets.items()}
        files: list[tuple[str, tuple[str | None, bytes | str, str | None]]] = [
            ("manifest", (None, json.dumps(manifest, sort_keys=True), None)),
            ("branch", (None, branch, None)),
            ("commit_message", (None, mark, None)),
            ("commit_dirty", (None, "true", None)),
        ]
        files += [(name, (name, data, "application/octet-stream")) for name, data in sorted(special.items())]
        res = _ok(client.post(f"{API}/accounts/{account}/pages/projects/{project}/deployments",
                            headers={"Authorization": f"Bearer {token}"}, files=files), "deployment")
        return {"status": "deployed", "mark": mark, "files": len(assets), "uploaded": uploaded,
                "deployment_id": (res or {}).get("id")}
    finally:
        if own:
            client.close()
