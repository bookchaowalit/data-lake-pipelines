# X lakehouse parallel run (Railway project `data-schedules`, cron service `x-lakehouse`,
# `20 1,9,15 * * *` UTC = 08:20 / 16:20 / 22:20 Asia/Bangkok). Docs: docs/plans/2026-11-05-x-lakehouse.md.
# Build from the repository root: railway up (RAILWAY_DOCKERFILE_PATH=deploy/x-lakehouse/Dockerfile).
FROM python:3.12-slim

# Local OCR for Meta screenshots (X_LAKE_META, spec docs/specs/meta-intake.md): images never leave the container.
RUN apt-get update \
 && apt-get install -y --no-install-recommends tesseract-ocr tesseract-ocr-tha tesseract-ocr-eng \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.13.1 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PYTHONUTF8=1

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --extra sheets --extra sentry
COPY infra/scripts/data_lake/__init__.py infra/scripts/data_lake/observability.py infra/scripts/data_lake/jobs_bi_site.py infra/scripts/data_lake/
COPY infra/scripts/x_lakehouse infra/scripts/x_lakehouse
# The x-insights site (X_LAKE_BI) reuses the jobs site's CSS and design-floor tokens.
COPY infra/bi/jobs/assets infra/bi/jobs/assets

# Build-time smoke test: the package imports and the contract builds its Arrow schemas (no network).
WORKDIR /app/infra/scripts
RUN /app/.venv/bin/python -c "import x_lakehouse.scheduled, x_lakehouse.bi_site, x_lakehouse.pages_deploy, x_lakehouse.meta; from x_lakehouse.schemas import DATASETS; [d.arrow_schema() for d in DATASETS.values()]; from data_lake.jobs_bi_site import ASSETS; assert (ASSETS / 'site.css').is_file()"

RUN tesseract --list-langs 2>&1 | grep -qx tha

RUN useradd --create-home --uid 10001 lakehouse
USER lakehouse
CMD ["/app/.venv/bin/python", "-m", "x_lakehouse.scheduled"]
