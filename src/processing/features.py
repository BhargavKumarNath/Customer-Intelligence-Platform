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
def build_feature_store(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)
    
    # 12GB Limit for safety during large joins
    con.execute("SET memory_limit='12GB';")
    con.execute("SET threads TO 4;")
    
    try:
        start_global = time.time()
        logger.info("Starting Feature Engineering Pipeline...")

        # 1. PREPARE SESSION AGGREGATES
        # Need to squash session data down to the user level
        # e.g., "What is this user's average session duration?"
        
        logger.info("   Aggregating session metrics to user level...")
        query_session_aggs = """
        CREATE OR REPLACE TEMP TABLE session_features AS
        SELECT 
            user_id,
            COUNT(user_session) as total_sessions,
            AVG(duration_sec) as avg_session_duration,
            STDDEV(duration_sec) as std_session_duration,
            AVG(event_count) as avg_events_per_session,
            
            -- Behavior Ratios
            SUM(CAST(has_cart AS INT)) / COUNT(user_session) as cart_rate,
            SUM(CAST(has_purchase AS INT)) / NULLIF(SUM(CAST(has_cart AS INT)), 0) as checkout_rate,
            
            -- Time Preference
            mode(dayname(session_start)) as preferred_weekday,
            mode(part_of_day(session_start)) as preferred_time_of_day
        FROM fact_sessions
        GROUP BY user_id;
        """
        # Actually DuckDB doesn't have `part_of_day`, let's fix that logic in the final join or simplify.
        
        query_session_aggs_safe = """
        CREATE OR REPLACE TEMP TABLE session_features AS
        SELECT 
            user_id,
            COUNT(user_session) as total_sessions,
            AVG(duration_sec) as avg_session_duration,
            COALESCE(STDDEV(duration_sec), 0) as std_session_duration,
            AVG(event_count) as avg_events_per_session,
            
            -- Interaction Rates (Cast to Double to avoid integer division)
            CAST(SUM(CAST(has_cart AS INT)) AS DOUBLE) / COUNT(user_session) as cart_rate,
            
            -- Checkout Rate (Purchases / Carts). Avoid Division by Zero.
            CASE 
                WHEN SUM(CAST(has_cart AS INT)) = 0 THEN 0
                ELSE CAST(SUM(CAST(has_purchase AS INT)) AS DOUBLE) / SUM(CAST(has_cart AS INT)) 
            END as checkout_rate
            
        FROM fact_sessions
        GROUP BY user_id;
        """
        con.execute(query_session_aggs_safe)

        # 2. BUILD THE GOLDEN TABLE (LEFT JOIN)
        # Base: dim_users (All users)
        # Join: rfm_segments (Only buyers)
        # Join: session_features (All active users)
        
        logger.info("   Joining Users + RFM + Session Features...")
        
        query_features = """
        CREATE OR REPLACE TABLE features_users AS
        SELECT 
            u.user_id,
            
            -- Profile Features
            u.total_spend,
            u.purchase_count,
            u.event_count,
            u.first_seen,
            u.last_seen,
            
            -- RFM Features (Coalesce Nulls for Non-Buyers)
            COALESCE(r.recency_days, -1) as recency_days,
            COALESCE(r.frequency_count, 0) as frequency_raw,
            COALESCE(r.monetary_value, 0) as monetary_raw,
            COALESCE(r.segment_name, 'Browser') as rfm_segment,
            COALESCE(r.rfm_code, '000') as rfm_code,
            
            -- Behavioral Features
            COALESCE(s.total_sessions, 0) as total_sessions,
            COALESCE(s.avg_session_duration, 0) as avg_session_duration,
            COALESCE(s.std_session_duration, 0) as std_session_duration,
            COALESCE(s.avg_events_per_session, 0) as avg_events_per_session,
            COALESCE(s.cart_rate, 0) as cart_rate,
            COALESCE(s.checkout_rate, 0) as checkout_rate
            
        FROM dim_users u
        LEFT JOIN analysis_rfm_segments r ON u.user_id = r.user_id
        LEFT JOIN session_features s ON u.user_id = s.user_id;
        """
        
        start = time.time()
        con.execute(query_features)
        logger.info(f"'features_users' created in {time.time() - start:.2f}s")
        
        # 3. VERIFICATION
        row_count = con.execute("SELECT COUNT(*) FROM features_users").fetchone()[0]
        cols = con.execute("DESCRIBE features_users").fetchall()
        
        logger.info(f"Feature Store Summary:")
        logger.info(f"   Total Rows: {row_count:,}")
        logger.info(f"   Total Features: {len(cols)}")
        
        # Preview Segment breakdown in Feature Store
        logger.info("   Verifying Segment Coverage:")
        seg_dist = con.execute("SELECT rfm_segment, COUNT(*) FROM features_users GROUP BY 1 ORDER BY 2 DESC").fetchdf()
        print(seg_dist)

    except Exception as e:
        logger.error(f"Error during feature engineering: {e}")
    finally:
        con.close()
        logger.info(f"Feature pipeline finished in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    build_feature_store()