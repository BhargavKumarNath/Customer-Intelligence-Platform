import duckdb
import hydra
from omegaconf import DictConfig
import logging
import os

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
        # 1. Hardware Optimization
        logger.info(f"Setting memory limit to {cfg.database.memory_limit}...")
        con.execute(f"SET memory_limit='{cfg.database.memory_limit}';")
        
        con.execute("SET threads TO 4;")
        logger.info("Using 4 threads for stable ingestion")
        
        # 2. Ingestion Logic - OPTIMIZED
        table_name = cfg.database.main_table
        
        logger.info(f"Ingesting {raw_path} into table '{table_name}'...")
        logger.info("This might take 2-3 minutes (optimized for stability)...")
        
        # Option A: If you NEED the data sorted by event_time
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{raw_path}')
            ORDER BY event_time;
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