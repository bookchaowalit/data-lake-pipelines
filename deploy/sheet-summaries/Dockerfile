# Daily sheet summaries (Railway project `data-schedules`, cron service `sheet-summaries`,
# `0 12 * * *` UTC = 19:00 Asia/Bangkok). Docs: docs/sheet-summaries.md.
# Build from the repository root: railway up (RAILWAY_DOCKERFILE_PATH=deploy/sheet-summaries/Dockerfile).
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PYTHONUTF8=1

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --extra sheets --extra sentry
COPY infra/scripts/data_lake/__init__.py infra/scripts/data_lake/observability.py infra/scripts/data_lake/
COPY infra/scripts/sync/sheet_summaries.py infra/scripts/sync/
COPY infra/tests/fixtures/sheet_summaries/sheet.json deploy/sheet-summaries/smoke-denylist.txt /app/smoke/

# Build-time smoke test: a dry run on the fake fixture with the fake gateway (no network, no writes).
RUN CLIENT_DENYLIST_PATH=/app/smoke/smoke-denylist.txt .venv/bin/python infra/scripts/sync/sheet_summaries.py \
      --dry-run --fixture /app/smoke/sheet.json --fake-gateway | grep -q '"status": "dry-run"'

RUN useradd --create-home --uid 10001 summaries
USER summaries
# CLIENT_DENYLIST (one term per line) is written to a file the job reads; with neither it nor
# CLIENT_DENYLIST_PATH set, the path is empty and the job fails closed before reading the sheet.
CMD ["/bin/sh", "-c", "if [ -z \"$CLIENT_DENYLIST_PATH\" ] && [ -n \"$CLIENT_DENYLIST\" ]; then printf '%s\\n' \"$CLIENT_DENYLIST\" > /tmp/client-denylist.txt && export CLIENT_DENYLIST_PATH=/tmp/client-denylist.txt; fi; exec /app/.venv/bin/python /app/infra/scripts/sync/sheet_summaries.py"]
