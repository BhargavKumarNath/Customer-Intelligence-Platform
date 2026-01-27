import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def create_sessions(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)
    
    # 10GB limit to handle the large GROUP BY on UUIDs
    con.execute("SET memory_limit='10GB';")
    con.execute("SET threads TO 4;")
    
    try:
        start_global = time.time()
        logger.info("Starting Sessionization Pipeline...")

        # 1. CREATE FACT_SESSIONS
        # Logic:
        # Group by user_session (UUID)
        # Calculate boolean flags for the funnel steps
        # Calculate timestamps for duration analysis
        
        query_sessions = """
        CREATE OR REPLACE TABLE fact_sessions AS
        SELECT 
            user_session,
            MAX(user_id) as user_id,  -- Associate session with user
            MIN(event_time) as session_start,
            MAX(event_time) as session_end,
            date_diff('second', MIN(event_time), MAX(event_time)) as duration_sec,
            COUNT(*) as event_count,
            
            -- Funnel Steps (Binary Flags)
            BOOL_OR(event_type = 'view') as has_view,
            BOOL_OR(event_type = 'cart') as has_cart,
            BOOL_OR(event_type = 'remove_from_cart') as has_remove,
            BOOL_OR(event_type = 'purchase') as has_purchase,
            
            -- Financials
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as session_revenue,
            
            -- Content affinity (What category did they spend most time/clicks on?)
            mode(category_code) as top_category
            
        FROM events
        WHERE user_session IS NOT NULL
        GROUP BY user_session;
        """
        
        logger.info("Aggregating 109M events into Sessions (Group By UUID)...")
        start = time.time()
        con.execute(query_sessions)
        
        # Get Stats
        stats = con.execute("SELECT COUNT(*) as cnt, AVG(duration_sec) as dur FROM fact_sessions").fetchone()
        row_count = stats[0]
        avg_dur = stats[1]
        
        logger.info(f"'fact_sessions' created in {time.time() - start:.2f}s")
        logger.info(f"   Total Sessions: {row_count:,}")
        logger.info(f"   Avg Duration:   {avg_dur:.2f} seconds")

        # 2. GENERATE FUNNEL METRICS
        logger.info("Calculating Session-Based Conversion Funnel...")
        
        funnel_query = """
        SELECT 
            SUM(CAST(has_view AS INT)) as sessions_with_view,
            SUM(CAST(has_cart AS INT)) as sessions_with_cart,
            SUM(CAST(has_purchase AS INT)) as sessions_with_purchase,
            
            -- Conversion Rates
            ROUND(SUM(CAST(has_cart AS INT)) / SUM(CAST(has_view AS INT)), 4) as view_to_cart_rate,
            ROUND(SUM(CAST(has_purchase AS INT)) / SUM(CAST(has_cart AS INT)), 4) as cart_to_purchase_rate,
            ROUND(SUM(CAST(has_purchase AS INT)) / COUNT(*), 4) as overall_conversion
        FROM fact_sessions;
        """
        
        funnel_df = con.execute(funnel_query).fetchdf()
        
        # Transpose for nice printing
        logger.info(f"\nFunnel Analysis Results:\n{funnel_df.T}")

    except Exception as e:
        logger.error(f"Error during sessionization: {e}")
    finally:
        con.close()
        logger.info(f"Session pipeline finished in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    create_sessions()