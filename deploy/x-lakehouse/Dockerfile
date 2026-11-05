# X lakehouse parallel run (Railway project `data-schedules`, cron service `x-lakehouse`,
# `20 1,9,15 * * *` UTC = 08:20 / 16:20 / 22:20 Asia/Bangkok). Docs: docs/plans/2026-11-05-x-lakehouse.md.
# Build from the repository root: railway up (RAILWAY_DOCKERFILE_PATH=deploy/x-lakehouse/Dockerfile).
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PYTHONUTF8=1

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --extra sheets --extra sentry
COPY infra/scripts/data_lake/__init__.py infra/scripts/data_lake/observability.py infra/scripts/data_lake/
COPY infra/scripts/x_lakehouse infra/scripts/x_lakehouse

# Build-time smoke test: the package imports and the contract builds its Arrow schemas (no network).
WORKDIR /app/infra/scripts
RUN /app/.venv/bin/python -c "import x_lakehouse.scheduled; from x_lakehouse.schemas import DATASETS; [d.arrow_schema() for d in DATASETS.values()]"

RUN useradd --create-home --uid 10001 lakehouse
USER lakehouse
CMD ["/app/.venv/bin/python", "-m", "x_lakehouse.scheduled"]
