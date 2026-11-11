# syntax=docker/dockerfile:1.7
# Two targets: "dev" runs the API's tests and type checks, "runtime" (the default) serves the API.
FROM python:3.14.10-slim AS base

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON=/usr/local/bin/python3.14 \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
WORKDIR /opt/app

COPY pyproject.toml uv.lock .python-version ./
COPY analytics/api/pyproject.toml analytics/api/
COPY analytics/common/pyproject.toml analytics/common/
COPY analytics/pipelines/pyproject.toml analytics/pipelines/
RUN useradd --system --no-create-home api


FROM base AS dev

RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv sync --frozen --package api --group dev --no-install-workspace

COPY analytics/ analytics/
COPY tests/ tests/
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv sync --frozen --package api --group dev

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    COVERAGE_FILE=/tmp/.coverage \
    RUFF_CACHE_DIR=/tmp/.ruff_cache
USER api


FROM base AS runtime

RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv sync --frozen --package api --no-dev --no-install-workspace

COPY analytics/api/src analytics/api/src
COPY analytics/common/src analytics/common/src
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.7,source=/uv,target=/usr/local/bin/uv \
    uv sync --frozen --package api --no-dev --no-editable

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1
USER api
EXPOSE 8000
CMD ["uvicorn", "--factory", "api.main:create_app_from_env", "--host", "0.0.0.0", "--port", "8000"]
