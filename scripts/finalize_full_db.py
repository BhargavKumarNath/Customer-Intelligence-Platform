"""Finalise the full-scale ``data/db/behavior.duckdb`` for the dashboard.

The Hydra pipeline (``src/ingestion`` -> ``src/processing`` -> ``src/analysis``
-> ``src/models``) writes RFM and retention as ``analysis_rfm_segments`` /
``analysis_weekly_retention``. The Streamlit dashboard's full/local mode
(``app/db_utils.py`` opens this DB directly) instead expects the cloud-path
names ``user_rfm_segments`` / ``weekly_retention`` (see
``src/processing/dimensional_model.py``).

This script adds those as *views* over the existing ``analysis_*`` tables - no
recompute, no duplication - plus a defensive rename of the old
``fact_daily_kpis`` column names if an older ``initial_modeling.py`` produced
them. Idempotent: safe to run repeatedly.

    python scripts/finalize_full_db.py            # uses config/config.yaml path
    python scripts/finalize_full_db.py PATH.duckdb
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _default_db_path() -> Path:
    cfg = yaml.safe_load((PROJECT_ROOT / "config" / "config.yaml").read_text())
    raw = cfg["paths"]["database"].replace("${hydra:runtime.cwd}", str(PROJECT_ROOT))
    return Path(raw)


_FACT_KPI_RENAMES = {
    "total_events": "daily_events",
    "total_views": "views",
    "total_carts": "carts",
    "total_purchases": "purchases",
}


def finalize(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"{db_path} not found - run the Hydra pipeline first")
    con = duckdb.connect(str(db_path))
    try:
        # PRAGMA table_info columns: (cid, name, type, notnull, dflt_value, pk)
        cols = {r[1] for r in con.execute("PRAGMA table_info('fact_daily_kpis')").fetchall()}
        for old, new in _FACT_KPI_RENAMES.items():
            if old in cols and new not in cols:
                con.execute(f"ALTER TABLE fact_daily_kpis RENAME COLUMN {old} TO {new}")
                print(f"renamed fact_daily_kpis.{old} -> {new}")

        con.execute("""
            CREATE OR REPLACE VIEW user_rfm_segments AS
            SELECT
                user_id,
                recency_days,
                frequency_count               AS frequency,
                monetary_value                AS monetary,
                r_score, f_score, m_score,
                r_score + f_score + m_score   AS rfm_total,
                segment_name                  AS segment
            FROM analysis_rfm_segments
        """)
        con.execute("""
            CREATE OR REPLACE VIEW weekly_retention AS
            SELECT cohort_week, cohort_size, weeks_since_first, active_users, retention_rate
            FROM analysis_weekly_retention
        """)
        n_rfm = con.execute("SELECT COUNT(*) FROM user_rfm_segments").fetchone()[0]
        n_ret = con.execute("SELECT COUNT(*) FROM weekly_retention").fetchone()[0]
        print(f"created view user_rfm_segments ({n_rfm:,} rows), weekly_retention ({n_ret:,} rows)")
        print(f"finalised {db_path}")
    finally:
        con.close()


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else _default_db_path()
    finalize(target)
