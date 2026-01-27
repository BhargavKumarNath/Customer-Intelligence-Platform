import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def calculate_retention(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)

    # Moderate settings for analytical queries
    con.execute("SET memory_limit='10GB';")
    con.execute("SET threads TO 4;")

    try:
        start_global = time.time()
        logger.info("Starting Cohort Retention Analysis...")
        # 1. Compute Weekly Retention
        query_retention = """
        CREATE OR REPLACE TABLE analysis_weekly_retention AS
        WITH user_activity AS (
            SELECT 
                u.user_id,
                date_trunc('week', u.first_seen) as cohort_week,
                date_trunc('week', e.event_time) as activity_week
            FROM dim_users u
            JOIN events e ON u.user_id = e.user_id
        ),
        cohort_sizes AS (
            -- Get the starting size of each cohort (Week 0)
            SELECT 
                date_trunc('week', first_seen) as cohort_week,
                COUNT(DISTINCT user_id) as cohort_size
            FROM dim_users
            GROUP BY 1
        )
        SELECT 
            ua.cohort_week,
            cs.cohort_size,
            date_diff('week', ua.cohort_week, ua.activity_week) as weeks_since_first,
            COUNT(DISTINCT ua.user_id) as active_users,
            CAST(COUNT(DISTINCT ua.user_id) AS DOUBLE) / cs.cohort_size as retention_rate
        FROM user_activity ua
        JOIN cohort_sizes cs ON ua.cohort_week = cs.cohort_week
        GROUP BY 1, 2, 3
        ORDER BY 1, 3;
        """

        logger.info("Executing Retention Query (Activate Users per Cohort/Week)...")
        start = time.time()
        con.execute(query_retention)
        logger.info(f"Retention table build in {time.time() - start:.2f}s")

        # 2. Compute Churn Risk
        logger.info("Identidying At-Risk Users...")

        max_date = con.execute("SELECT MAX(event_time) FROM events").fetchone()[0]
        logger.info(f"Dataset Cutoff Date: {max_date}")
        
        query_churn = f"""
        CREATE OR REPLACE TABLE analysis_churn_risk AS
        SELECT 
            user_id,
            last_seen,
            date_diff('day', last_seen, TIMESTAMP '{max_date}') as days_inactive,
            CASE 
                WHEN date_diff('day', last_seen, TIMESTAMP '{max_date}') > 14 THEN 'Churned'
                WHEN date_diff('day', last_seen, TIMESTAMP '{max_date}') > 7 THEN 'At Risk'
                ELSE 'Active'
            END as status
        FROM dim_users;
        """
        con.execute(query_churn)
        
        # 3. VERIFICATION & PREVIEW
        logger.info("Retention Matrix Preview (Pandas formatted):")
        
        # Fetch data to display a nice pivot table in the logs
        df = con.execute("SELECT * FROM analysis_weekly_retention").fetchdf()
        
        # Pivot for readability: Rows=Cohort, Cols=Weeks Out, Val=Retention Rate
        pivot = df.pivot(index='cohort_week', columns='weeks_since_first', values='retention_rate')
        print(pivot.round(3).fillna(''))

        # Churn Stats
        churn_counts = con.execute("SELECT status, count(*) FROM analysis_churn_risk GROUP BY 1").fetchdf()
        logger.info(f"\nUser Status Distribution:\n{churn_counts}")

    except Exception as e:
        logger.error(f"Error during retention analysis: {e}")
    finally:
        con.close()
        logger.info(f"Retention pipeline finished in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    calculate_retention()