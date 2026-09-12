"""Typed runtime configuration for the API/service layer.

This is intentionally separate from the Hydra configs under `config/` (those
drive the offline `src/ingestion`, `src/processing`, `src/models` pipeline
scripts, which are left untouched). This module governs only the FastAPI
service, which always ships and reads the same precomputed sample artifacts
that are baked into the Docker image.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Local dev origins only. Production origins are supplied at deploy time via
# CIP_CORS_ALLOW_ORIGINS (comma-separated) - see Phase 3 task 4.
_DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:8501",
]


class Environment(StrEnum):
    LOCAL = "local"
    CI = "ci"
    PRODUCTION = "production"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CIP_", env_file=".env", extra="ignore")

    environment: Environment = Environment.LOCAL
    database_path: Path = PROJECT_ROOT / "data" / "sample" / "sample.duckdb"
    # Prefer LightGBM's native text format; the legacy pickle path is a one-release
    # fallback (see src/services/propensity.load_model + Phase 3 task 6).
    model_path: Path = PROJECT_ROOT / "src" / "models" / "propensity_lgbm.txt"
    metrics_path: Path = PROJECT_ROOT / "src" / "models" / "metrics.json"
    memory_limit: str = "512MB"
    threads: int = 2
    # NoDecode: stop pydantic-settings from JSON-decoding the env value so the
    # validator below can accept a plain comma-separated string.
    cors_allow_origins: Annotated[list[str], NoDecode] = _DEFAULT_CORS_ORIGINS
    # Trusted proxy hops for uvicorn's ProxyHeadersMiddleware. "*" is correct
    # behind a single managed load balancer (Cloud Run / Cloudflare).
    forwarded_allow_ips: str = "*"
    rate_limit: str = "60/minute"
    # Service build identity for GET /version. Falls back to the model's git_sha
    # (from metrics.json) when the deploy pipeline does not inject these.
    git_sha: str = ""
    built_at: str = ""

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def _split_cors_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value


def get_settings() -> Settings:
    return Settings()
