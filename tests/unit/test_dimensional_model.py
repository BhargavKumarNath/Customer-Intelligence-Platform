"""Regression coverage for src/processing/dimensional_model.py.

Before this module existed, scripts/create_cloud_database.py and
app/db_utils.py each carried an independent copy of this SQL, including an
independent copy of a bug that anchored RFM recency to CURRENT_DATE instead
of the dataset's own reference date. Neither copy had any test coverage.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.processing.dimensional_model import build_all

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SAMPLE_PARQUET = PROJECT_ROOT / "data" / "sample" / "sample_optimized.parquet"


@pytest.fixture(scope="module")
def built_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE TABLE events AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_all(con)
    yield con
    con.close()


def test_build_all_creates_every_table_with_rows(built_connection):
    for table in (
        "dim_products",
        "dim_users",
        "fact_sessions",
        "fact_daily_kpis",
        "user_rfm_segments",
        "predictions_product_affinity",
        "weekly_retention",
    ):
        count = built_connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count > 0, f"{table} should have at least one row"


def test_rfm_recency_is_anchored_to_dataset_not_wall_clock(built_connection):
    """Regression test for the CURRENT_DATE bug: recency_days must fall
    within the dataset's own date span, not drift with today's date."""
    min_event_date, max_event_date = built_connection.execute(
        "SELECT MIN(CAST(event_time AS DATE)), MAX(CAST(event_time AS DATE)) FROM events"
    ).fetchone()
    dataset_span_days = (max_event_date - min_event_date).days

    max_recency = built_connection.execute(
        "SELECT MAX(recency_days) FROM user_rfm_segments"
    ).fetchone()[0]

    assert max_recency <= dataset_span_days, (
        f"recency_days ({max_recency}) exceeds the dataset's own span ({dataset_span_days} days), "
        "recency is anchored to something other than the dataset's max event date"
    )
    # A wall-clock CURRENT_DATE anchor against 2019 data would produce
    # recency_days in the thousands by now; this pins the fix in place.
    assert max_recency < 365


def test_product_affinity_respects_lift_threshold(built_connection):
    min_lift = built_connection.execute(
        "SELECT MIN(lift) FROM predictions_product_affinity"
    ).fetchone()[0]
    assert min_lift is None or min_lift > 1.2


def test_rfm_segmentation_is_deterministic_across_rebuilds():
    """Regression test for a tie-break bug: NTILE(5) OVER (ORDER BY frequency)
    with no secondary sort key let rows sharing a frequency/recency/monetary
    value (76.7% of buyers share frequency=1 in the sample dataset) land in
    different quintiles on different runs, silently shifting segment counts
    by hundreds of users between rebuilds of the identical input data."""
    con_a = duckdb.connect(":memory:")
    con_a.execute(f"CREATE TABLE events AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_all(con_a)
    counts_a = con_a.execute(
        "SELECT segment, COUNT(*) FROM user_rfm_segments GROUP BY segment ORDER BY segment"
    ).fetchall()
    con_a.close()

    con_b = duckdb.connect(":memory:")
    con_b.execute(f"CREATE TABLE events AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_all(con_b)
    counts_b = con_b.execute(
        "SELECT segment, COUNT(*) FROM user_rfm_segments GROUP BY segment ORDER BY segment"
    ).fetchall()
    con_b.close()

    assert counts_a == counts_b, (
        "RFM segment counts differ between two builds of identical input data, "
        "the NTILE window functions are missing a deterministic tiebreaker"
    )
