import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys

from src.utils.duckdb_env import apply_pragmas

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def build_feature_store(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)
    
    # Bounded memory + disk spill for the large user-level joins.
    apply_pragmas(con, db_path=db_path)

    try:
        start_global = time.time()
        logger.info("Starting Feature Engineering Pipeline...")

        # 1. PREPARE SESSION AGGREGATES
        # Squash ~23M session rows down to one row per user.
        #
        # `preferred_weekday` / `preferred_time_of_day` used to be computed with
        # `mode()` here. `mode()` is a *holistic* aggregate DuckDB cannot spill,
        # so over ~5.3M user groups it pushed RSS well past `memory_limit` and
        # froze a 10 GB box. They are now derived in `time_pref` below with a
        # plain count + `arg_max`, both of which spill. Everything in this
        # SELECT (AVG/STDDEV/SUM/COUNT) is a spillable streaming aggregate.
        logger.info("   Aggregating session metrics to user level...")
        query_session_aggs = """
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
        con.execute(query_session_aggs)

        # Per-user modal weekday / part-of-day, spillable form: count sessions
        # per (user, bucket) then keep the bucket with the highest count via
        # arg_max (a normal aggregate, unlike mode()).
        logger.info("   Deriving time-of-day preferences (arg_max, spillable)...")
        con.execute("""
        CREATE OR REPLACE TEMP TABLE time_pref AS
        WITH wd AS (
            SELECT user_id, dayname(session_start) AS wd, COUNT(*) AS c
            FROM fact_sessions GROUP BY 1, 2
        ),
        tod AS (
            SELECT user_id,
                CASE
                    WHEN EXTRACT(HOUR FROM session_start) BETWEEN 5 AND 11 THEN 'Morning'
                    WHEN EXTRACT(HOUR FROM session_start) BETWEEN 12 AND 16 THEN 'Afternoon'
                    WHEN EXTRACT(HOUR FROM session_start) BETWEEN 17 AND 21 THEN 'Evening'
                    ELSE 'Night'
                END AS tod, COUNT(*) AS c
            FROM fact_sessions GROUP BY 1, 2
        ),
        wd_top AS (SELECT user_id, arg_max(wd, c) AS preferred_weekday FROM wd GROUP BY 1),
        tod_top AS (SELECT user_id, arg_max(tod, c) AS preferred_time_of_day FROM tod GROUP BY 1)
        SELECT wd_top.user_id, preferred_weekday, preferred_time_of_day
        FROM wd_top JOIN tod_top USING (user_id);
        """)

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
            COALESCE(s.checkout_rate, 0) as checkout_rate,
            COALESCE(t.preferred_weekday, 'Unknown') as preferred_weekday,
            COALESCE(t.preferred_time_of_day, 'Unknown') as preferred_time_of_day

        FROM dim_users u
        LEFT JOIN analysis_rfm_segments r ON u.user_id = r.user_id
        LEFT JOIN session_features s ON u.user_id = s.user_id
        LEFT JOIN time_pref t ON u.user_id = t.user_id;
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