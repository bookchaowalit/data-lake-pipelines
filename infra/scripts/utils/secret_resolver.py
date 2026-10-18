#!/usr/bin/env python3
"""Runtime secret resolution for local scripts.

Resolution policy:

* ``SECRETS_PROVIDER=env`` reads the already-injected environment only.
* ``SECRETS_PROVIDER=infisical`` reads Infisical through its CLI or REST API.
* ``SECRETS_PROVIDER=vault`` reads HashiCorp Vault KV v2.
* ``SECRETS_PROVIDER=auto`` prefers configured Infisical, then Vault, then env.

Secret values are kept in process memory only. This module never writes them
to a file, SQLite, logs, or command output.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from functools import lru_cache
from pathlib import Path
import sys
from typing import Any


DEFAULT_INFISICAL_PROJECT_ID = "d0d84520-c3d0-48da-980c-a987c3077122"
DEFAULT_INFISICAL_SECRET_PATH = "/google"
DEFAULT_BOOKFINANCE_TRADING_PROJECT_ID = "65cc2f7f-4e1d-4123-aa06-5978ccaae861"

# Solo Empire keeps provider credentials in separate Infisical folders.  The
# path remains overrideable with INFISICAL_SECRET_PATH; these routes prevent
# non-Google integrations from being looked up in the Google folder by
# default.
SECRET_PATH_BY_KEY = {
    "GOOGLE_CLIENT_ID": "/google",
    "GOOGLE_CLIENT_SECRET": "/google",
    "GOOGLE_REFRESH_TOKEN": "/google",
    "GOOGLE_CLIENT_ID_ALT": "/google",
    "GOOGLE_CLIENT_SECRET_ALT": "/google",
    "GOOGLE_GMAIL_SEND_CLIENT_ID": "/google",
    "GOOGLE_GMAIL_SEND_CLIENT_SECRET": "/google",
    "GOOGLE_GMAIL_SEND_REFRESH_TOKEN": "/google",
    "GOOGLE_SEARCH_CONSOLE_REFRESH_TOKEN": "/google",
    "AHREFS_API_KEY": "/ahrefs",
    "GITHUB_PERSONAL_ACCESS_TOKEN": "/github",
    "GITHUB_TOKEN": "/github",
    "CLOUDFLARE_API_TOKEN": "/cloudflare",
    "CLOUDFLARE_ACCOUNT_ID": "/cloudflare",
    "VERCEL_TOKEN": "/vercel",
    "VERCEL_PROJECT_ID": "/vercel",
    "TODOIST_API_TOKEN": "/todoist",
    "TELEGRAM_BOT_TOKEN": "/telegram",
    "TELEGRAM_NOTI_BOT_TOKEN": "/telegram",
    "TELEGRAM_NOTI_CHAT_ID": "/telegram",
    "TELEGRAM_TRADING_BOT_TOKEN": "/telegram",
    "TELEGRAM_CHAT_ID": "/telegram",
    "TELEGRAM_API_ID": "/telegram",
    "TELEGRAM_API_HASH": "/telegram",
    "LINE_SOLO_EMPIRE_CHANNEL_ACCESS_TOKEN": "/line",
    "LINE_SOLO_EMPIRE_WEBHOOK_SECRET": "/line",
    "LINE_SOLO_EMPIRE_WEBHOOK_URL": "/line",
    "FIRECRAWL_API_KEY": "/firecrawl",
    "STRAVA_ACCESS_TOKEN": "/strava",
    "STRAVA_CLIENT_ID": "/strava",
    "STRAVA_CLIENT_SECRET": "/strava",
    "BINANCE_TH_API_KEY": "/trading",
    "BINANCE_TH_API_SECRET": "/trading",
    "BINANCE_FUTURES_API_KEY": "/trading",
    "BINANCE_FUTURES_API_SECRET": "/trading",
    "TRADING_DATABASE_URL": "/database",
    "SOLO_DATABASE_URL": "/database",
    "SUPABASE_ACCESS_TOKEN": "/supabase",
    "STRIPE_SECRET_KEY": "/stripe",
    "STRIPE_WEBHOOK_SECRET": "/stripe",
    "SENTRY_AUTH_TOKEN": "/sentry",
    "POSTHOG_API_KEY": "/posthog",
    "SLACK_BOT_TOKEN": "/slack",
    "DISCORD_BOT_TOKEN": "/discord",
    "WHATSAPP_CLOUD_API_TOKEN": "/whatsapp-business",
    "MICROSOFT_GRAPH_ACCESS_TOKEN": "/microsoft-graph",
    "ZOOM_ACCESS_TOKEN": "/zoom",
    "MONDAY_API_TOKEN": "/monday",
    "NOTION_API_TOKEN": "/notion",
    "NOTION_BOT_TOKEN": "/notion",
    "ASANA_PERSONAL_ACCESS_TOKEN": "/asana",
    "LINEAR_API_KEY": "/linear",
}

SECRET_PROJECT_BY_KEY = {
    "BINANCE_FUTURES_API_KEY": DEFAULT_BOOKFINANCE_TRADING_PROJECT_ID,
    "BINANCE_FUTURES_API_SECRET": DEFAULT_BOOKFINANCE_TRADING_PROJECT_ID,
}


@lru_cache(maxsize=1)
def _cloud_secret_paths() -> dict[str, str]:
    platform_scripts = Path(__file__).resolve().parents[1] / "platform"
    if str(platform_scripts) not in sys.path:
        sys.path.insert(0, str(platform_scripts))
    from cloud_providers import CloudProviderRegistryError, load_registry, secret_names

    try:
        registry = load_registry()
    except CloudProviderRegistryError as exc:
        raise SecretResolutionError("cloud_registry_invalid") from exc
    paths: dict[str, str] = {}
    for provider in registry["providers"]:
        manager = provider["auth"].get("secret_manager")
        if manager is not None:
            for name in secret_names(provider):
                if name in paths and paths[name] != manager["path"]:
                    raise SecretResolutionError("cloud_secret_path_conflict")
                paths[name] = manager["path"]
    return paths

# Secrets required for the Gmail data pipeline end-to-end check.
GOOGLE_PIPELINE_SECRETS = (
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REFRESH_TOKEN",
)

# Public status vocabulary for health probes (never includes secret values).
STATUS_OK = "ok"
STATUS_CONFIGURED = "configured"
STATUS_CREDENTIALS_NOT_CONFIGURED = "credentials_not_configured"
STATUS_AUTHENTICATION_FAILED = "authentication_failed"
STATUS_SECRET_NOT_FOUND = "secret_not_found"
STATUS_PROVIDER_UNREACHABLE = "provider_unreachable"


class SecretResolutionError(RuntimeError):
    """A configured secret provider could not return a secret."""


class SecretUnavailable(SecretResolutionError):
    """No configured provider or environment contains the requested secret."""


def classify_secret_error(exc: BaseException) -> str:
    """Map provider exceptions to a public status class (no secret material)."""
    msg = str(exc) if exc is not None else ""
    lower = msg.lower()
    if "secret_not_found" in lower or "http_404" in lower:
        return STATUS_SECRET_NOT_FOUND
    if (
        "not_configured" in lower
        or "auth_not_configured" in lower
        or "unknown_secret_provider" in lower
    ):
        return STATUS_CREDENTIALS_NOT_CONFIGURED
    if (
        "http_401" in lower
        or "http_403" in lower
        or "cli_authentication_failed" in lower
        or "cli_unauthorized" in lower
        or "auth_invalid" in lower
        or "authentication" in lower
    ):
        return STATUS_AUTHENTICATION_FAILED
    if (
        "unreachable" in lower
        or "cli_unavailable" in lower
        or "cli_failed" in lower
        or "http_" in lower
        or "invalid_response" in lower
    ):
        return STATUS_PROVIDER_UNREACHABLE
    if isinstance(exc, SecretUnavailable):
        return STATUS_SECRET_NOT_FOUND
    if isinstance(exc, SecretResolutionError):
        return STATUS_PROVIDER_UNREACHABLE
    return STATUS_PROVIDER_UNREACHABLE


def selected_provider_mode() -> str:
    return os.environ.get("SECRETS_PROVIDER", "auto").strip().lower() or "auto"


def resolve_active_provider() -> tuple[str, Any | None]:
    """Return (mode, provider_instance_or_None) without fetching secrets.

    For ``auto``, returns the first configured manager (Infisical, then Vault)
    or the env provider when no manager is configured.
    """
    mode = selected_provider_mode()
    if mode == "env":
        return mode, EnvProvider()
    if mode == "auto":
        for candidate in (InfisicalProvider(), VaultProvider()):
            if candidate.is_configured():
                return mode, candidate
        return mode, EnvProvider()
    provider = _provider(mode)
    return mode, provider


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _safe_http_json(url: str, *, method: str = "GET", data: bytes | None = None,
                   headers: dict[str, str] | None = None, timeout: int = 10) -> dict[str, Any]:
    """Make a JSON request without exposing response bodies in exceptions."""
    request = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read())
            if not isinstance(payload, dict):
                raise SecretResolutionError("secret_provider_invalid_response")
            return payload
    except urllib.error.HTTPError as exc:
        # Do not read or include the provider response: it may contain details
        # that are not safe for logs.
        raise SecretResolutionError(f"secret_provider_http_{exc.code}") from None
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        raise SecretResolutionError("secret_provider_unreachable") from None


class EnvProvider:
    name = "env"

    def is_configured(self) -> bool:
        return True

    def get(self, name: str) -> str | None:
        return os.environ.get(name) or None


class InfisicalProvider:
    name = "infisical"

    def __init__(self) -> None:
        self.site_url = os.environ.get("INFISICAL_SITE_URL", "https://app.infisical.com").rstrip("/")
        self.project_id = os.environ.get("INFISICAL_PROJECT_ID", DEFAULT_INFISICAL_PROJECT_ID)
        self.environment = os.environ.get("INFISICAL_ENVIRONMENT", "dev")
        self.secret_path = os.environ.get(
            "INFISICAL_SECRET_PATH", DEFAULT_INFISICAL_SECRET_PATH
        )
        self.client_id = os.environ.get("INFISICAL_UNIVERSAL_AUTH_CLIENT_ID") or os.environ.get("INFISICAL_CLIENT_ID")
        self.client_secret = os.environ.get("INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET") or os.environ.get("INFISICAL_CLIENT_SECRET")
        self.access_token = os.environ.get("INFISICAL_ACCESS_TOKEN") or os.environ.get("INFISICAL_TOKEN")
        self._token_expires_at = 0.0

    def is_configured(self) -> bool:
        cli_enabled = _truthy(os.environ.get("INFISICAL_CLI_ENABLED")) and shutil.which(
            os.environ.get("INFISICAL_CLI", "infisical")
        )
        return bool(self.access_token or (self.client_id and self.client_secret) or cli_enabled)

    def _get_cli(self, name: str) -> str | None:
        if not _truthy(os.environ.get("INFISICAL_CLI_ENABLED")):
            return None
        binary = shutil.which(os.environ.get("INFISICAL_CLI", "infisical"))
        if not binary:
            return None
        args = [
            binary, "secrets", "get", name,
            "--env", self.environment,
            "--path", self.secret_path,
            "--plain", "--silent",
        ]
        if self.project_id:
            args.extend(["--projectId", self.project_id])
        child_env = os.environ.copy()
        child_env["INFISICAL_DISABLE_UPDATE_CHECK"] = "true"
        try:
            result = subprocess.run(
                args, capture_output=True, text=True, timeout=20,
                check=False, env=child_env, stdin=subprocess.DEVNULL,
            )
        except (OSError, subprocess.TimeoutExpired):
            raise SecretResolutionError("infisical_cli_unavailable") from None
        if result.returncode != 0:
            # Infisical CLI emits authentication failures on stderr. Keep the
            # public error code precise without ever returning provider output
            # (which could contain account or secret-manager details).
            stderr = (result.stderr or "").lower()
            if any(
                marker in stderr
                for marker in (
                    "unauthorized",
                    "unauthorised",
                    "forbidden",
                    "not authenticated",
                    "authentication",
                    "login",
                    "login required",
                    "token expired",
                    "invalid token",
                    "http 401",
                    "http 403",
                )
            ):
                raise SecretResolutionError("infisical_cli_authentication_failed")
            raise SecretResolutionError("infisical_cli_failed")
        value = result.stdout.strip()
        return value or None

    def _get_api_token(self) -> str:
        if self.access_token:
            return self.access_token
        if not (self.client_id and self.client_secret):
            raise SecretUnavailable("infisical_auth_not_configured")
        if self.access_token and time.time() < self._token_expires_at:
            return self.access_token
        body = urllib.parse.urlencode({
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
        }).encode()
        response = _safe_http_json(
            f"{self.site_url}/api/v1/auth/universal-auth/login",
            method="POST", data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        token = response.get("accessToken")
        if not token:
            raise SecretResolutionError("infisical_auth_invalid_response")
        self.access_token = str(token)
        self._token_expires_at = time.time() + max(int(response.get("expiresIn", 3600)) - 30, 30)
        return self.access_token

    def get(self, name: str) -> str | None:
        cli_value = self._get_cli(name)
        if cli_value is not None:
            return cli_value
        token = self._get_api_token()
        query = urllib.parse.urlencode({
            "projectId": self.project_id,
            "environment": self.environment,
            "secretPath": self.secret_path,
            "viewSecretValue": "true",
        })
        response = _safe_http_json(
            f"{self.site_url}/api/v4/secrets/{urllib.parse.quote(name, safe='')}?{query}",
            headers={"Authorization": f"Bearer {token}"},
        )
        secret = response.get("secret") or {}
        value = secret.get("secretValue")
        return str(value) if value is not None and str(value) != "" else None


class VaultProvider:
    name = "vault"

    def __init__(self) -> None:
        self.address = os.environ.get("VAULT_ADDR", "").rstrip("/")
        self.namespace = os.environ.get("VAULT_NAMESPACE")
        self.mount = os.environ.get("VAULT_KV_MOUNT", "secret").strip("/")
        self.secret_path = os.environ.get("VAULT_SECRET_PATH", "solo-empire/google").strip("/")
        self.auth_mount = os.environ.get("VAULT_AUTH_MOUNT", "approle").strip("/")
        self.token = os.environ.get("VAULT_TOKEN")
        self.role_id = os.environ.get("VAULT_ROLE_ID")
        self.secret_id = os.environ.get("VAULT_SECRET_ID")

    def is_configured(self) -> bool:
        return bool(self.address and (self.token or (self.role_id and self.secret_id)))

    def _headers(self) -> dict[str, str]:
        headers = {"X-Vault-Token": self.token or ""}
        if self.namespace:
            headers["X-Vault-Namespace"] = self.namespace
        return headers

    def _login(self) -> None:
        if self.token:
            return
        if not (self.role_id and self.secret_id):
            raise SecretUnavailable("vault_auth_not_configured")
        response = _safe_http_json(
            f"{self.address}/v1/auth/{urllib.parse.quote(self.auth_mount, safe='')}/login",
            method="POST", data=json.dumps({
                "role_id": self.role_id, "secret_id": self.secret_id,
            }).encode(),
            headers={"Content-Type": "application/json", **({"X-Vault-Namespace": self.namespace} if self.namespace else {})},
        )
        self.token = ((response.get("auth") or {}).get("client_token"))
        if not self.token:
            raise SecretResolutionError("vault_auth_invalid_response")

    def get(self, name: str) -> str | None:
        self._login()
        response = _safe_http_json(
            f"{self.address}/v1/{urllib.parse.quote(self.mount, safe='')}/data/"
            f"{urllib.parse.quote(self.secret_path, safe='/')}",
            headers=self._headers(),
        )
        values = ((response.get("data") or {}).get("data") or {})
        value = values.get(name)
        return str(value) if value is not None and str(value) != "" else None


def _provider(name: str):
    if name == "env":
        return EnvProvider()
    if name in {"infisical", "infisical-api", "infisical-cli"}:
        provider = InfisicalProvider()
        if name == "infisical-cli":
            os.environ.setdefault("INFISICAL_CLI_ENABLED", "true")
        return provider
    if name in {"vault", "hashicorp-vault"}:
        return VaultProvider()
    raise SecretResolutionError("unknown_secret_provider")


def get_secret_with_source(
    name: str,
    *,
    default: str | None = None,
    required: bool = False,
) -> tuple[str | None, str | None]:
    """Resolve one secret and return its source without exposing the value."""
    mode = os.environ.get("SECRETS_PROVIDER", "auto").strip().lower()
    fallback_env = _truthy(os.environ.get("SECRETS_FALLBACK_ENV"))
    env_provider = EnvProvider()
    source: str | None = None

    # A caller can force one shared path for a deployment. Otherwise route
    # known provider keys to their canonical folder. This keeps the existing
    # INFISICAL_SECRET_PATH override useful for custom layouts.
    configured_path = os.environ.get("INFISICAL_SECRET_PATH")
    secret_path = (
        configured_path
        if configured_path and configured_path != DEFAULT_INFISICAL_SECRET_PATH
        else _cloud_secret_paths().get(
            name, SECRET_PATH_BY_KEY.get(name, configured_path or DEFAULT_INFISICAL_SECRET_PATH)
        )
    )

    def prepare_provider(provider):
        if isinstance(provider, InfisicalProvider):
            provider.secret_path = secret_path
            provider.project_id = SECRET_PROJECT_BY_KEY.get(name, provider.project_id)
        return provider

    if mode == "env":
        value = env_provider.get(name)
        source = "env" if value else None
    elif mode == "auto":
        candidates = [prepare_provider(InfisicalProvider()), VaultProvider()]
        value = None
        configured_provider = None
        for candidate in candidates:
            if candidate.is_configured():
                configured_provider = candidate
                break
        if configured_provider is None:
            value = env_provider.get(name)
            source = "env" if value else None
        else:
            try:
                value = configured_provider.get(name)
            except SecretResolutionError:
                if not fallback_env:
                    raise
                value = env_provider.get(name)
                source = "env" if value else None
            else:
                if value:
                    source = str(getattr(configured_provider, "name", "secret_manager")).lower()
    else:
        provider = prepare_provider(_provider(mode))
        if not provider.is_configured():
            if fallback_env:
                value = env_provider.get(name)
                source = "env" if value else None
            else:
                raise SecretUnavailable("secret_provider_not_configured")
        else:
            try:
                value = provider.get(name)
            except SecretResolutionError:
                if not fallback_env:
                    raise
                value = env_provider.get(name)
                source = "env" if value else None
            else:
                if value:
                    source = str(getattr(provider, "name", "secret_manager")).lower()

    if value is None or value == "":
        if required:
            raise SecretUnavailable(f"secret_not_found:{name}")
        return default, None
    return value, source


def get_secret(name: str, *, default: str | None = None, required: bool = False) -> str | None:
    """Resolve one secret according to the configured provider policy."""
    value, _ = get_secret_with_source(name, default=default, required=required)
    return value


def probe_secret(name: str) -> dict[str, str]:
    """Resolve one secret name and report only a public status (never the value)."""
    mode, provider = resolve_active_provider()
    provider_name = getattr(provider, "name", "unknown") if provider is not None else "none"
    result: dict[str, str] = {
        "name": name,
        "provider": provider_name,
        "mode": mode,
        "status": STATUS_CREDENTIALS_NOT_CONFIGURED,
    }

    # Explicit manager modes without bootstrap identity.
    if (
        mode not in {"env", "auto"}
        and provider is not None
        and not provider.is_configured()
        and not _truthy(os.environ.get("SECRETS_FALLBACK_ENV"))
    ):
        result["status"] = STATUS_CREDENTIALS_NOT_CONFIGURED
        result["error_class"] = STATUS_CREDENTIALS_NOT_CONFIGURED
        return result

    try:
        # required=True surfaces secret_not_found; provider errors propagate.
        value = get_secret(name, required=True)
        # Presence check only — drop the value immediately.
        del value
        result["status"] = STATUS_OK
        return result
    except (SecretResolutionError, SecretUnavailable) as exc:
        status = classify_secret_error(exc)
        result["status"] = status
        result["error_class"] = status
        return result


def probe_secrets(names: tuple[str, ...] | list[str] | None = None) -> dict[str, Any]:
    """Health-check secret names for the active provider. Never includes values."""
    names = tuple(names or GOOGLE_PIPELINE_SECRETS)
    mode, provider = resolve_active_provider()
    provider_name = getattr(provider, "name", "none") if provider is not None else "none"
    if provider is None:
        configured = False
    elif isinstance(provider, EnvProvider):
        # Env is always "available"; configured means bootstrap manager present.
        configured = True
    else:
        configured = provider.is_configured()

    secrets: list[dict[str, str]] = [probe_secret(name) for name in names]
    statuses = [item["status"] for item in secrets]

    if statuses and all(s == STATUS_OK for s in statuses):
        overall = STATUS_OK
    elif any(s == STATUS_AUTHENTICATION_FAILED for s in statuses):
        overall = STATUS_AUTHENTICATION_FAILED
    elif any(s == STATUS_PROVIDER_UNREACHABLE for s in statuses):
        overall = STATUS_PROVIDER_UNREACHABLE
    elif mode not in {"env", "auto"} and not configured:
        overall = STATUS_CREDENTIALS_NOT_CONFIGURED
    elif mode == "auto" and isinstance(provider, EnvProvider) and all(
        s in {STATUS_SECRET_NOT_FOUND, STATUS_CREDENTIALS_NOT_CONFIGURED} for s in statuses
    ):
        overall = STATUS_CREDENTIALS_NOT_CONFIGURED
    elif any(s == STATUS_CREDENTIALS_NOT_CONFIGURED for s in statuses):
        overall = STATUS_CREDENTIALS_NOT_CONFIGURED
    elif any(s == STATUS_SECRET_NOT_FOUND for s in statuses):
        overall = STATUS_SECRET_NOT_FOUND
    elif configured:
        overall = STATUS_CONFIGURED
    else:
        overall = STATUS_CREDENTIALS_NOT_CONFIGURED

    return {
        "schema_version": "1.0",
        "status": overall,
        "mode": mode,
        "provider": provider_name,
        "provider_configured": configured,
        "fallback_env": _truthy(os.environ.get("SECRETS_FALLBACK_ENV")),
        "secrets": [
            {
                "name": item["name"],
                "status": item["status"],
                **({"error_class": item["error_class"]} if item.get("error_class") else {}),
            }
            for item in secrets
        ],
    }
