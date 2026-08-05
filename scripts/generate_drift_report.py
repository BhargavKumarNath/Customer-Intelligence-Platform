"""Generates a data-drift HTML report comparing October vs November user
behaviour in the sample dataset. Self-contained - only needs data/sample/sample.duckdb
(see scripts/create_cloud_database.py), so it runs identically locally and in CI,
with no dependency on un-tracked full-dataset artifacts.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
from evidently import Report
from evidently.presets import DataDriftPreset

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "sample" / "sample.duckdb"
OUTPUT_PATH = PROJECT_ROOT / "outputs" / "drift_report.html"

_USER_FEATURES_QUERY = """
    SELECT
        user_id,
        COUNT(*) AS event_count,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_spend
    FROM events
    WHERE {time_filter}
    GROUP BY user_id
"""


def generate_drift_report() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"{DB_PATH} not found - run `python scripts/create_cloud_database.py` first"
        )

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        reference = con.execute(
            _USER_FEATURES_QUERY.format(time_filter="event_time < '2019-11-01'")
        ).fetchdf()
        current = con.execute(
            _USER_FEATURES_QUERY.format(time_filter="event_time >= '2019-11-01'")
        ).fetchdf()
    finally:
        con.close()

    logger.info(
        "Comparing %d October users (reference) against %d November users (current)",
        len(reference),
        len(current),
    )

    report = Report(metrics=[DataDriftPreset()])
    snapshot = report.run(
        current_data=current.drop(columns=["user_id"]),
        reference_data=reference.drop(columns=["user_id"]),
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    snapshot.save_html(str(OUTPUT_PATH))
    logger.info("Drift report written to %s", OUTPUT_PATH)


if __name__ == "__main__":
    generate_drift_report()
