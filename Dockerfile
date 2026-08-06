# syntax=docker/dockerfile:1

FROM python:3.11-slim AS builder

WORKDIR /build
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip

COPY pyproject.toml ./
COPY src ./src
COPY api ./api
RUN pip install --no-cache-dir --no-compile ".[api]"

# sample.duckdb is a derived build artifact (~100MB, not committed - see .gitignore)
# rebuilt here from the tracked sample_optimized.parquet, exactly like local dev does.
COPY scripts/create_cloud_database.py ./scripts/create_cloud_database.py
COPY data/sample/sample_optimized.parquet ./data/sample/sample_optimized.parquet
RUN python scripts/create_cloud_database.py

FROM python:3.11-slim AS runtime

# libgomp1: LightGBM's compiled extension dynamically loads libgomp.so.1 (GNU OpenMP)
# at import time, which python:3.11-slim doesn't ship by default.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system app \
    && useradd --system --gid app --home-dir /app --create-home app

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CIP_ENVIRONMENT=production

COPY --from=builder --chown=app:app /opt/venv /opt/venv

WORKDIR /app
COPY --from=builder --chown=app:app /build/src ./src
COPY --from=builder --chown=app:app /build/api ./api
COPY --from=builder --chown=app:app /build/data/sample/sample.duckdb ./data/sample/sample.duckdb

USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8000/healthz', timeout=3)" || exit 1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
