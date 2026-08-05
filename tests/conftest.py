from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from src.config import Settings
from src.db import DuckDBConnectionManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings(
        database_path=PROJECT_ROOT / "data" / "sample" / "sample.duckdb",
        model_path=PROJECT_ROOT / "src" / "models" / "propensity_lgbm.pkl",
    )


@pytest.fixture(scope="session")
def connections(settings: Settings) -> Iterator[DuckDBConnectionManager]:
    manager = DuckDBConnectionManager(settings)
    yield manager
    manager.close()
