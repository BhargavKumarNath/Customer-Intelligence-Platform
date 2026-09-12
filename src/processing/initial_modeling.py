"""
Dimensional model builder for the full-scale, Hydra-driven local pipeline
(against the full 109M-row `data/db/behavior.duckdb`, built via
`src/ingestion/loader.py`).

This intentionally does NOT share `src/processing/dimensional_model.py`.
That module is the shared builder for the sample/cloud path
(`scripts/create_cloud_database.py`). The two builders still differ (this
one adds `dim_users.is_buyer`, `favorite_category_by_recency`, and a richer
`fact_sessions` via `sessionization.py`; the RFM/retention tables here are
named `analysis_*`), but the overlapping columns are kept name-compatible
by convention.
"""

import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys

from src.utils.duckdb_env import apply_pragmas

# Configure logging with UTF-8 for Windows
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def create_dimensional_models(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)

    # Bounded memory + disk spill; tunable via CIP_DUCKDB_* env vars.
    apply_pragmas(con, db_path=db_path)
    logger.info("DuckDB session tuned (see src/utils/duckdb_env.py)")

    try:
        start_global = time.time()

        # 1. CREATE DIM_PRODUCTS (Memory-optimised approach)
        logger.info(" Creating 'dim_products'...")
        # Use DISTINCT ON instead of window function 
        query_products = """
        CREATE OR REPLACE TABLE dim_products AS 
        SELECT DISTINCT ON (product_id)
            product_id,
            category_id,
            COALESCE(category_code, 'unknown') as category_code,
            COALESCE(brand, 'unknown') as brand,
            price as current_price
        FROM events
        ORDER BY product_id, event_time DESC;
        """
        start = time.time()
        con.execute(query_products)
        row_count = con.execute("SELECT COUNT(*) FROM dim_products").fetchone()[0]
        logger.info(f" 'dim_products' created in {time.time() - start:.2f}s ({row_count:,} products)")

        # 2. CREATE FACT_DAILY_KPIS
        # Column names kept identical to src/processing/dimensional_model.py's
        # fact_daily_kpis (daily_events / views / carts / purchases) so both
        # builders' outputs share one schema. They used to diverge
        # (total_events / total_*).
        logger.info(" Creating 'fact_daily_kpis'...")
        query_daily = """
        CREATE OR REPLACE TABLE fact_daily_kpis AS
        SELECT
            CAST(event_time AS DATE) as date,
            COUNT(*) as daily_events,
            COUNT(DISTINCT user_id) as dau,
            COUNT(DISTINCT user_session) as daily_sessions,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as daily_revenue,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as carts,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views
        FROM events
        GROUP BY 1
        ORDER BY 1;
        """
        start = time.time()
        con.execute(query_daily)
        row_count = con.execute("SELECT COUNT(*) FROM fact_daily_kpis").fetchone()[0]
        logger.info(f" 'fact_daily_kpis' created in {time.time() - start:.2f}s ({row_count:,} days)")

        # 3. CREATE DIM_USERS (Heavy Operation)
        logger.info(" Creating 'dim_users' (This is the heaviest operation)...")
        query_users = """
        CREATE OR REPLACE TABLE dim_users AS 
        SELECT 
            user_id,
            MIN(event_time) as first_seen,
            MAX(event_time) as last_seen,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_session) as session_count,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as total_spend,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
            -- Boolean flags for easy segmentation later
            BOOL_OR(event_type = 'purchase') as is_buyer,
            MAX(CASE WHEN event_type = 'view' THEN category_code END) as favorite_category_by_recency
        FROM events
        GROUP BY user_id;
        """
        start = time.time()
        con.execute(query_users)
        row_count = con.execute("SELECT COUNT(*) FROM dim_users").fetchone()[0]
        logger.info(f" 'dim_users' created in {time.time() - start:.2f}s ({row_count:,} users)")

        # 4. VERIFICATION
        logger.info(" Verification:")
        tables = con.execute("SHOW TABLES").fetchall()
        logger.info(f"   Tables in DB: {[t[0] for t in tables]}")
        
        # Sanity check on Revenue
        total_rev = con.execute("SELECT SUM(daily_revenue) FROM fact_daily_kpis").fetchone()[0]
        logger.info(f"   Total Revenue: ${total_rev:,.2f}")
        
        # User stats
        buyer_count = con.execute("SELECT COUNT(*) FROM dim_users WHERE is_buyer = true").fetchone()[0]
        logger.info(f"   Total Buyers: {buyer_count:,}")

    except Exception as e:
        logger.error(f" Error during modeling: {e}")
    finally:
        con.close()
        logger.info(f" Modeling pipeline finished in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    create_dimensional_models()