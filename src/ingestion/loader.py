import duckdb
import hydra
from omegaconf import DictConfig
import logging
import os

from src.utils.duckdb_env import apply_pragmas

# Configure simple logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def ingest_data(cfg: DictConfig):
    
    db_path = cfg.paths.database
    raw_path = cfg.paths.raw_data
    
    # Check if raw data exists
    if not os.path.exists(raw_path):
        logger.error(f"Raw data not found at {raw_path}. Please move your parquet file there.")
        return

    logger.info(f" Connecting to DuckDB at {db_path}...")
    
    # Connect to DuckDB (Persistent Disk-Based)
    con = duckdb.connect(db_path)
    
    try:
        # 1. Hardware Optimization. Bounded memory + disk spill.
        #    Tunable via CIP_DUCKDB_* env vars (see src/utils/duckdb_env.py).
        apply_pragmas(con, db_path=db_path)
        logger.info("DuckDB session tuned (see src/utils/duckdb_env.py)")

        # 2. Ingestion Logic
        table_name = cfg.database.main_table

        logger.info(f"Ingesting {raw_path} into table '{table_name}'...")

        # No global `ORDER BY event_time` here. `summarise/optimize_dataset.py`
        # writes the two monthly files in order, so the parquet is already
        # time-ordered end-to-end (verified: 218 out-of-order adjacent rows in
        # 109.95M). A re-sort of the full 9-column payload spilled >8 GB to disk
        # and added ~30 min on a 10 GB box for zero downstream benefit: every
        # consumer does a full-table GROUP BY, and the date-range filters in
        # train_propensity / build_static_artifacts already prune via parquet
        # row-group statistics (an `event_time < '2019-11-01'` count returns in
        # <10 ms). Add `ORDER BY event_time` back here only if a consumer starts
        # relying on physical row order.
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{raw_path}');
        """)
                
        # 3. Verification
        row_count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Ingestion Complete. Total Rows: {row_count:,}")
        
        # 4. Preview
        logger.info("Data Preview (First 3 rows):")
        preview = con.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
        print(preview)
        
    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    ingest_data()