import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys

from src.processing.dimensional_model import build_user_rfm_segments
from src.utils.duckdb_env import apply_pragmas

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def perform_segmentation(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)
    apply_pragmas(con, db_path=db_path)
    
    try:
        start_global = time.time()
        logger.info("Starting RFM Segmentation Pipeline...")

        # Same rule the sample database, API and dashboard use (one definition,
        # in src/processing/dimensional_model.py). Buyers only; non-buyers are
        # labelled 'Browser' downstream in features.py.
        start = time.time()
        n = build_user_rfm_segments(con, table="analysis_rfm_segments")
        logger.info(f"RFM Segments created for {n:,} buyers in {time.time() - start:.2f}s")

        # 3. ANALYSIS & REPORTING
        logger.info("Segmentation Distribution:")
        
        # Summary Query
        summary_query = """
        SELECT 
            segment as segment_name,
            COUNT(*) as user_count,
            ROUND(AVG(monetary), 2) as avg_spend,
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
