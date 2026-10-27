# Registry export (Railway project `data-schedules`, cron service `registry-export`,
# `40 23 * * *` UTC = 06:40 Asia/Bangkok). Docs: README "Metadata ใน lake".
# Build from the repository root: railway up (RAILWAY_DOCKERFILE_PATH=deploy/registry-export/Dockerfile).
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PYTHONUTF8=1

# data_lake/storage.py shells out to `aws` for s3:// lake URIs (R2). Its own tool venv keeps
# botocore apart from the project's s3fs/aiobotocore pins.
RUN UV_TOOL_DIR=/opt/uv-tools UV_TOOL_BIN_DIR=/usr/local/bin uv tool install "awscli>=1.36,<2" \
 && aws --version

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --extra sentry
COPY infra/scripts/data_lake infra/scripts/data_lake
COPY infra/tests/fixtures/registry_lake_export /app/smoke/

# Build-time smoke test: a dry run on the fixture directories (no GitHub, no lake writes).
RUN .venv/bin/python infra/scripts/data_lake/scheduled_registry_export.py --dry-run \
      --registries /app/smoke/registries --catalog /app/smoke/metadata-catalog | grep -q '"status": "dry-run"'

RUN useradd --create-home --uid 10001 exporter
USER exporter
CMD ["/app/.venv/bin/python", "/app/infra/scripts/data_lake/scheduled_registry_export.py"]
