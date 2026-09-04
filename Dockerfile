# syntax=docker/dockerfile:1

# Base image digest-pinned for reproducible builds (Phase 1, task 4).
# Both stages MUST use the same digest. Tag at pin time:
#   python:3.11-slim == 3.11.16-slim-trixie
# Refresh both lines together with:
#   docker buildx imagetools inspect python:3.11-slim   # -> Digest: sha256:...

FROM python:3.11-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534 AS builder

WORKDIR /build
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY pyproject.toml ./
COPY src ./src
COPY api ./api
RUN pip install --no-cache-dir --no-compile ".[api]"

# sample.duckdb is a derived build artifact (~100MB, not committed - see .gitignore)
# rebuilt here from the tracked sample_optimized.parquet, exactly like local dev does.
COPY scripts/create_cloud_database.py ./scripts/create_cloud_database.py
COPY data/sample/sample_optimized.parquet ./data/sample/sample_optimized.parquet
RUN python scripts/create_cloud_database.py

FROM python:3.11-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534 AS runtime

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

# This image never installs packages at runtime. Remove pip - from the venv and
# from the base image's system Python - plus the system setuptools/wheel. pip's
# bundled _vendor/vendor.txt pins old msgpack / setuptools that a vuln scanner
# flags even though that code is never executed here; dropping pip removes it.
# setuptools (84.x) stays in the venv for any runtime importlib.metadata lookups.
RUN /usr/local/bin/python -m pip uninstall -y pip setuptools wheel 2>/dev/null || true \
    && /opt/venv/bin/python -m pip uninstall -y pip 2>/dev/null || true \
    && rm -rf /usr/local/lib/python3.11/ensurepip \
              /opt/venv/lib/python3.11/site-packages/pip \
              /opt/venv/lib/python3.11/site-packages/pip-*.dist-info \
              /root/.cache

WORKDIR /app
COPY --from=builder --chown=app:app /build/src ./src
COPY --from=builder --chown=app:app /build/api ./api
COPY --from=builder --chown=app:app /build/data/sample/sample.duckdb ./data/sample/sample.duckdb

USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8000/healthz', timeout=3)" || exit 1

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
