from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from src.config import Settings
from src.db import DuckDBConnectionManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DB = PROJECT_ROOT / "data" / "sample" / "sample.duckdb"
SAMPLE_PARQUET = PROJECT_ROOT / "data" / "sample" / "sample_optimized.parquet"
BUILD_SCRIPT = PROJECT_ROOT / "scripts" / "create_cloud_database.py"


@pytest.fixture(scope="session", autouse=True)
def sample_database() -> Path:
    """Ensure the derived ``sample.duckdb`` exists before any test runs.

    ``sample.duckdb`` is a build artifact (git-ignored, ~90 MB) rebuilt from the
    tracked parquet by ``scripts/create_cloud_database.py``. On a clean checkout
    it is absent; without this, fixture-based tests raise a bare
    ``FileNotFoundError`` and the ``TestClient``-based API tests silently get a
    503 from ``/ready``. ``autouse`` so it also covers tests that reach the DB
    through the app's own ``get_settings()`` rather than the ``connections``
    fixture. Build it once here, or fail with an actionable message. In CI a
    prior step already builds it, so this is a no-op check.
    See deployment_stages.md Phase 1 -> P1-NEW-5 (F9).
    """
    if SAMPLE_DB.exists():
        return SAMPLE_DB
    if not SAMPLE_PARQUET.exists():
        pytest.exit(
            f"Neither {SAMPLE_DB.relative_to(PROJECT_ROOT)} nor its source "
            f"{SAMPLE_PARQUET.relative_to(PROJECT_ROOT)} is present - cannot run "
            "the DB-backed tests. Restore the sample parquet first.",
            returncode=1,
        )
    result = subprocess.run(
        [sys.executable, str(BUILD_SCRIPT)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or not SAMPLE_DB.exists():
        pytest.exit(
            "Failed to build the sample DuckDB. Run it by hand to see why:\n"
            f"    python {BUILD_SCRIPT.relative_to(PROJECT_ROOT)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
            returncode=1,
        )
    return SAMPLE_DB


@pytest.fixture(scope="session")
def settings(sample_database: Path) -> Settings:
    return Settings(
        database_path=sample_database,
        model_path=PROJECT_ROOT / "src" / "models" / "propensity_lgbm.pkl",
    )


@pytest.fixture(scope="session")
def connections(settings: Settings) -> Iterator[DuckDBConnectionManager]:
    manager = DuckDBConnectionManager(settings)
    yield manager
    manager.close()
