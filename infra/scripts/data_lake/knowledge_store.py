#!/usr/bin/env python3
"""Minimal client for the hosted knowledge store (platform-knowledge-api SPEC-002).

Used by the registry-export cron since the architecture-docs cutover (architecture-docs SPEC-001
phase 3, knowledge store plans/architecture-docs-store/): store space ``architecture-docs`` is the
source of the documents, the Git repository is archived.

Tokens are minted per request like the knowledge MCP's ``mint()``: RS256, issuer
``SOLO_WEB_PUBLIC_ORIGIN``, audience ``knowledge-api``, 60 s lifetime, signed with
``KNOWLEDGE_STORE_KEY`` (PEM text) for subject ``KNOWLEDGE_STORE_SUBJECT``. The base URL is
``EMPIRE_KNOWLEDGE_API`` (the edge gateway route). Errors name the status and the error code
only, never a token or document text.
"""

from __future__ import annotations

import base64
import time
import urllib.parse
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

AUDIENCE = "knowledge-api"
READ_SCOPE = "knowledge:read"
WRITE_SCOPE = "knowledge:write"
TOKEN_LIFETIME = 60
ENV_KEYS = ("EMPIRE_KNOWLEDGE_API", "SOLO_WEB_PUBLIC_ORIGIN", "KNOWLEDGE_STORE_KEY", "KNOWLEDGE_STORE_SUBJECT")
USER_AGENT = "registry-export/1"

Request = Callable[..., httpx.Response]


class StoreError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Document:
    content: bytes
    version: int
    sha256: str


def configured(env: Mapping[str, str]) -> list[str]:
    """The store variables that are missing (empty list = configured)."""
    return [key for key in ENV_KEYS if not env.get(key, "").strip()]


def _origin(env: Mapping[str, str]) -> str:
    value = env.get("SOLO_WEB_PUBLIC_ORIGIN", "").strip()
    parts = urllib.parse.urlsplit(value)
    if parts.scheme != "https" or not parts.hostname or value != f"https://{parts.netloc}" or "@" in parts.netloc:
        raise StoreError("SOLO_WEB_PUBLIC_ORIGIN must be an https origin")
    return value


def mint(env: Mapping[str, str], scope: str, now: float | None = None) -> str:
    import jwt  # PyJWT with cryptography (extra "store")

    issued = int(time.time() if now is None else now)
    claims = {
        "iss": _origin(env),
        "aud": AUDIENCE,
        "sub": env["KNOWLEDGE_STORE_SUBJECT"].strip(),
        "tenant_id": "owner",
        "scope": scope,
        "iat": issued,
        "nbf": issued,
        "exp": issued + TOKEN_LIFETIME,
        "jti": str(uuid.uuid4()),
    }
    try:
        return jwt.encode(claims, env["KNOWLEDGE_STORE_KEY"].strip(), algorithm="RS256", headers={"typ": "JWT"})
    except Exception:  # noqa: BLE001 - a bad key must not surface its text
        raise StoreError("KNOWLEDGE_STORE_KEY is not a usable RSA private key") from None


class KnowledgeStore:
    def __init__(self, env: Mapping[str, str], *, request: Request | None = None) -> None:
        missing = configured(env)
        if missing:
            raise StoreError(f"knowledge store not configured: missing {', '.join(missing)}")
        base = env["EMPIRE_KNOWLEDGE_API"].strip().rstrip("/")
        if not base.startswith("https://"):
            raise StoreError("EMPIRE_KNOWLEDGE_API must be an https URL")
        self.env, self.base, self._request = env, base, request

    def _call(self, method: str, params: dict[str, Any], *, write: bool = False, body: Any = None) -> httpx.Response:
        token = mint(self.env, WRITE_SCOPE if write else READ_SCOPE)
        try:
            response = (self._request or httpx.request)(
                method,
                f"{self.base}/api/v1/store/document",
                params=params,
                json=body,
                headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT},
                timeout=30,
            )
        except httpx.HTTPError as error:
            raise StoreError(f"knowledge store {type(error).__name__}") from None
        return response

    @staticmethod
    def _fail(response: httpx.Response, what: str) -> StoreError:
        code = ""
        try:
            code = str(response.json()["error"]["code"])
        except (ValueError, KeyError, TypeError):
            pass
        return StoreError(f"{what}: HTTP {response.status_code} {code}".strip(), status=response.status_code)

    def get(self, space: str, path: str) -> Document | None:
        """The current document, or None when it does not exist (or was deleted)."""
        response = self._call("GET", {"space": space, "path": path})
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise self._fail(response, f"read {space}:{path}")
        document = response.json()["document"]
        if document.get("encoding") == "utf-8":
            content = str(document["content"]).encode("utf-8")
        else:
            content = base64.b64decode(document["content_base64"])
        return Document(content, int(document["version"]), str(document["sha256"]))

    def put(self, space: str, path: str, content: bytes, *, if_version: int) -> int:
        """Write a new version; refused (409) when the document is no longer at ``if_version``."""
        body = {"content_base64": base64.b64encode(content).decode("ascii"), "if_version": if_version}
        response = self._call("PUT", {"space": space, "path": path}, write=True, body=body)
        if response.status_code not in (200, 201):
            raise self._fail(response, f"write {space}:{path}")
        return int(response.json()["document"]["version"])
