"""Typed runtime configuration for the API/service layer.

This is intentionally separate from the Hydra configs under `config/` (those
drive the offline `src/ingestion`, `src/processing`, `src/models` pipeline
scripts and `app/db_utils.py`, which are left untouched). This module governs
only the new FastAPI service, which always ships and reads the same
precomputed sample artifacts that are baked into the Docker image.
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Environment(StrEnum):
    LOCAL = "local"
    CI = "ci"
    PRODUCTION = "production"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CIP_", env_file=".env", extra="ignore")

    environment: Environment = Environment.LOCAL
    database_path: Path = PROJECT_ROOT / "data" / "sample" / "sample.duckdb"
    model_path: Path = PROJECT_ROOT / "src" / "models" / "propensity_lgbm.pkl"
    memory_limit: str = "512MB"
    threads: int = 2
    cors_allow_origins: list[str] = [
        "http://localhost:8501",
        "https://customer-intelligence-platform.streamlit.app",
    ]
    rate_limit: str = "60/minute"


def get_settings() -> Settings:
    return Settings()
