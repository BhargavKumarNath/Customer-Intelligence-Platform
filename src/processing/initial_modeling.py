import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys

# Configure logging with UTF-8 for Windows
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def create_dimensional_models(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)

    # Optimisation for 16GB RAM system
    con.execute("SET memory_limit='10GB';")  
    con.execute("SET threads TO 3;")  
    con.execute("SET preserve_insertion_order=false;") 
    logger.info(" Memory: 10GB, Threads: 3, Insertion order: disabled")

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
        logger.info(" Creating 'fact_daily_kpis'...")
        query_daily = """
        CREATE OR REPLACE TABLE fact_daily_kpis AS 
        SELECT 
            CAST(event_time AS DATE) as date,
            COUNT(*) as total_events,
            COUNT(DISTINCT user_id) as dau,
            COUNT(DISTINCT user_session) as daily_sessions,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as daily_revenue,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as total_carts,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as total_views
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