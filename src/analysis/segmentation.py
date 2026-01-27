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
def perform_segmentation(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)
    
    try:
        start_global = time.time()
        logger.info("Starting RFM Segmentation Pipeline...")

        # 1. CALCULATE RFM RAW METRICS
        # We only segment 'Buyers'. Non-buyers are implicitly 'Browsers'.
        # We need the max_date to calculate Recency (days since last purchase)
        
        max_date = con.execute("SELECT MAX(event_time) FROM events").fetchone()[0]
        
        logger.info("   Calculating raw RFM metrics for buyers...")
        query_rfm_base = f"""
        CREATE OR REPLACE TEMP TABLE rfm_base AS
        SELECT 
            user_id,
            date_diff('day', MAX(event_time), TIMESTAMP '{max_date}') as recency_days,
            COUNT(*) as frequency_count,
            SUM(price) as monetary_value
        FROM events
        WHERE event_type = 'purchase'
        GROUP BY user_id;
        """
        con.execute(query_rfm_base)
        
        # 2. CALCULATE RFM SCORES (1-5)
        # Use NTILE to divide users into 5 equal buckets for each metric
        # Recency: Lower days is better (so we reverse the bucket: 1=Old, 5=Recent)
        # Frequency/Monetary: Higher is better (1=Low, 5=High)
        
        logger.info("   Computing NTILE quantiles (1-5)...")
        query_rfm_scores = """
        CREATE OR REPLACE TABLE analysis_rfm_segments AS
        WITH scores AS (
            SELECT 
                user_id,
                recency_days,
                frequency_count,
                monetary_value,
                -- Invert Recency: Shortest time = Score 5
                6 - NTILE(5) OVER (ORDER BY recency_days) as r_score,
                NTILE(5) OVER (ORDER BY frequency_count) as f_score,
                NTILE(5) OVER (ORDER BY monetary_value) as m_score
            FROM rfm_base
        )
        SELECT 
            *,
            -- Concatenate scores for easy lookup (e.g., '555')
            CAST(r_score AS VARCHAR) || CAST(f_score AS VARCHAR) || CAST(m_score AS VARCHAR) as rfm_code,
            
            -- RULE-BASED SEGMENTATION LOGIC
            CASE 
                WHEN (r_score >= 4 AND f_score >= 4) THEN 'Champions'
                WHEN (r_score >= 3 AND f_score >= 3) THEN 'Loyal Customers'
                WHEN (r_score >= 4 AND f_score = 1) THEN 'New Customers'
                WHEN (r_score >= 3 AND f_score = 1) THEN 'Promising'
                WHEN (r_score = 2 AND f_score >= 2) THEN 'Need Attention'
                WHEN (r_score = 1 AND f_score >= 4) THEN 'Cant Lose Them'
                WHEN (r_score = 1 AND f_score <= 2) THEN 'Hibernating'
                ELSE 'At Risk'
            END as segment_name
        FROM scores;
        """
        start = time.time()
        con.execute(query_rfm_scores)
        logger.info(f"RFM Segments created in {time.time() - start:.2f}s")

        # 3. ANALYSIS & REPORTING
        logger.info("Segmentation Distribution:")
        
        # Summary Query
        summary_query = """
        SELECT 
            segment_name,
            COUNT(*) as user_count,
            ROUND(AVG(monetary_value), 2) as avg_spend,
            ROUND(AVG(recency_days), 1) as avg_recency
        FROM analysis_rfm_segments
        GROUP BY 1
        ORDER BY avg_spend DESC;
        """
        
        df = con.execute(summary_query).fetchdf()
        
        # Calculate percent of total buyers
        total_buyers = df['user_count'].sum()
        df['pct_buyers'] = (df['user_count'] / total_buyers * 100).round(1)
        
        # Print nice table
        print(df[['segment_name', 'user_count', 'pct_buyers', 'avg_spend', 'avg_recency']])

    except Exception as e:
        logger.error(f"Error during segmentation: {e}")
    finally:
        con.close()
        logger.info(f"Segmentation pipeline finished in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    perform_segmentation()
