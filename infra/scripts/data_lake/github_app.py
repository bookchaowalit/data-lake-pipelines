#!/usr/bin/env python3
"""GitHub App installation tokens (owner decision 2026-11-28: one App replaces the PATs).

Environment: ``GITHUB_APP_ID`` (numeric App id) and ``GITHUB_APP_PRIVATE_KEY`` (the App private
key as PEM, PKCS#1 ``BEGIN RSA PRIVATE KEY`` or PKCS#8 ``BEGIN PRIVATE KEY``; literal ``\\n``
sequences are read as newlines). With both unset the caller keeps its PAT; with only one set
``app_configured`` raises, so a half-configured service fails instead of silently using the PAT.

Minting: an App JWT (RS256, ``iat = now - 60``, ``exp = now + 540``, ``iss = GITHUB_APP_ID``),
the installation id of the owner (``/orgs/{owner}/installation``, then
``/users/{owner}/installation``; cached per process), then
``POST /app/installations/{id}/access_tokens`` with a body that down-scopes the token to the
caller's ``permissions`` and ``repositories``. Installation tokens last one hour; a cached token
is re-minted when less than five minutes remain.

Errors carry the HTTP status and the owner only, never the key, the JWT or a token.
"""

from __future__ import annotations

import datetime as dt
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx

API = "https://api.github.com"
ENV_KEYS = ("GITHUB_APP_ID", "GITHUB_APP_PRIVATE_KEY")
JWT_BACKDATE = 60
JWT_LIFETIME = 540
REFRESH_MARGIN = 300

Request = Callable[..., httpx.Response]


class GitHubAppError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def app_configured(env: Mapping[str, str]) -> bool:
    """True when both App variables are set, False when neither is; raises when only one is."""
    present = [key for key in ENV_KEYS if env.get(key, "").strip()]
    if len(present) == len(ENV_KEYS):
        return True
    if present:
        missing = [key for key in ENV_KEYS if key not in present]
        raise GitHubAppError(f"GitHub App half configured: missing {', '.join(missing)}")
    return False


def load_private_key(text: str) -> Any:
    """The RSA private key from PEM text (PKCS#1 or PKCS#8, literal \\n accepted)."""
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    pem = text.strip().replace("\\n", "\n")
    try:
        key = load_pem_private_key(pem.encode("ascii"), password=None)
    except Exception:  # noqa: BLE001 - a bad key must not surface its text
        raise GitHubAppError("GITHUB_APP_PRIVATE_KEY is not a usable PEM private key") from None
    if not isinstance(key, rsa.RSAPrivateKey):
        raise GitHubAppError("GITHUB_APP_PRIVATE_KEY is not an RSA private key")
    return key


def app_jwt(app_id: str, key: Any, now: float | None = None) -> str:
    import jwt  # PyJWT with cryptography (extra "store")

    issued = int(time.time() if now is None else now)
    claims = {"iat": issued - JWT_BACKDATE, "exp": issued + JWT_LIFETIME, "iss": str(app_id)}
    try:
        return jwt.encode(claims, key, algorithm="RS256")
    except Exception:  # noqa: BLE001 - never surface key material
        raise GitHubAppError("GitHub App JWT could not be signed") from None


def _parse_expiry(value: Any) -> float:
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        raise GitHubAppError("GitHub App token response has no usable expires_at") from None


class GitHubApp:
    """Mints down-scoped installation tokens; one instance per process caches ids and tokens."""

    def __init__(
        self,
        env: Mapping[str, str],
        *,
        user_agent: str,
        request: Request | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not app_configured(env):
            raise GitHubAppError("GitHub App not configured")
        app_id = env["GITHUB_APP_ID"].strip()
        if not app_id.isdigit():
            raise GitHubAppError("GITHUB_APP_ID must be the numeric App id")
        self._app_id = app_id
        self._key = load_private_key(env["GITHUB_APP_PRIVATE_KEY"])
        self._user_agent = user_agent
        self._request = request
        self._clock = clock
        self._installations: dict[str, int] = {}
        self._tokens: dict[tuple[str, tuple[str, ...], tuple[tuple[str, str], ...]], tuple[str, float]] = {}

    def _call(self, method: str, path: str, *, owner: str, body: Any = None) -> httpx.Response:
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {app_jwt(self._app_id, self._key, self._clock())}",
            "User-Agent": self._user_agent,
            "X-GitHub-Api-Version": "2022-11-28",
        }
        try:
            return (self._request or httpx.request)(method, f"{API}{path}", headers=headers, json=body, timeout=30)
        except httpx.HTTPError as error:
            raise GitHubAppError(f"GitHub App {type(error).__name__} for {owner}") from None

    def installation_id(self, owner: str) -> int:
        if owner in self._installations:
            return self._installations[owner]
        response = self._call("GET", f"/orgs/{owner}/installation", owner=owner)
        if response.status_code == 404:
            response = self._call("GET", f"/users/{owner}/installation", owner=owner)
        if response.status_code != 200:
            raise GitHubAppError(f"GitHub App installation lookup for {owner}: HTTP {response.status_code}",
                                 status=response.status_code)
        try:
            installation = int(response.json()["id"])
        except (ValueError, KeyError, TypeError):
            raise GitHubAppError(f"GitHub App installation lookup for {owner}: malformed response") from None
        self._installations[owner] = installation
        return installation

    def token(self, owner: str, *, permissions: Mapping[str, str], repositories: Sequence[str] = ()) -> str:
        """An installation token for ``owner`` limited to ``permissions`` (and ``repositories``)."""
        key = (owner, tuple(sorted(repositories)), tuple(sorted(permissions.items())))
        cached = self._tokens.get(key)
        if cached is not None and cached[1] - self._clock() > REFRESH_MARGIN:
            return cached[0]
        body: dict[str, Any] = {"permissions": dict(permissions)}
        if repositories:
            body["repositories"] = list(repositories)
        installation = self.installation_id(owner)
        response = self._call("POST", f"/app/installations/{installation}/access_tokens", owner=owner, body=body)
        if response.status_code != 201:
            raise GitHubAppError(f"GitHub App token for {owner}: HTTP {response.status_code}",
                                 status=response.status_code)
        try:
            payload = response.json()
            value = str(payload["token"])
        except (ValueError, KeyError, TypeError):
            raise GitHubAppError(f"GitHub App token for {owner}: malformed response") from None
        self._tokens[key] = (value, _parse_expiry(payload.get("expires_at")))
        return value
