"""Settings from environment variables only. No secret has a default.

    GOOGLE_SERVICE_ACCOUNT_JSON  service-account key JSON (Infisical /sheet-summaries holds one)
    X_LAKE_SHEET_ID              Sheet id (default: Chief Findings Warehouse)
    X_LAKE_URI                   r2://<bucket>[/prefix] or file:///abs/dir for local tests
    R2_ACCOUNT_ID                Cloudflare account id (endpoint https://<id>.r2.cloudflarestorage.com)
    R2_ACCESS_KEY_ID             R2 API token, Object Read & Write, scoped to the one bucket
    R2_SECRET_ACCESS_KEY
    X_LAKE_DB_URL                Postgres URL of the curated layer (Supabase session pooler, sslmode=require)
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

DEFAULT_SHEET_ID = "1zajA2iEfa9TS5JNjV1Y7LOurrTmiqrP-8ygn9clvBdI"
ENV_VARS = ("GOOGLE_SERVICE_ACCOUNT_JSON", "X_LAKE_SHEET_ID", "X_LAKE_URI", "R2_ACCOUNT_ID",
            "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "X_LAKE_DB_URL")


class ConfigError(RuntimeError):
    pass


@dataclass
class Config:
    env: Mapping[str, str] = field(default_factory=lambda: dict(os.environ))

    def get(self, name: str, default: str = "") -> str:
        return self.env.get(name, default).strip()

    def require(self, name: str) -> str:
        value = self.get(name)
        if not value:
            raise ConfigError(f"set {name}")
        return value

    @property
    def sheet_id(self) -> str:
        return self.get("X_LAKE_SHEET_ID", DEFAULT_SHEET_ID)

    @property
    def lake_uri(self) -> str:
        return self.get("X_LAKE_URI")

    @property
    def db_url(self) -> str:
        return self.get("X_LAKE_DB_URL")
