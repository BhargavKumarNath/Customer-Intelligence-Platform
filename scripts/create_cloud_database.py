"""
Cloud Database Builder

This script builds a cloud-ready DuckDB database from the sample dataset,
including all dimensional modeling, fact tables, and ML predictions.

This creates a production-ready database optimized for Streamlit Cloud.
"""

import duckdb
import logging
from pathlib import Path
import sys

# Add src to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from processing.dimensional_model import build_all  # noqa: E402

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SAMPLE_PARQUET = PROJECT_ROOT / "data" / "sample" / "sample_optimized.parquet"
SAMPLE_DB = PROJECT_ROOT / "data" / "sample" / "sample.duckdb"

# Database settings for cloud (reduced memory)
MEMORY_LIMIT = "512MB"
THREADS = 2


def build_cloud_database():
    """Build complete database from sample data"""
    
    if not SAMPLE_PARQUET.exists():
        logger.error(f"Sample parquet not found: {SAMPLE_PARQUET}")
        logger.error("Run create_sample_dataset.py first!")
        return
    
    logger.info(f"Building cloud database from: {SAMPLE_PARQUET}")
    
    # Remove existing database if present
    if SAMPLE_DB.exists():
        SAMPLE_DB.unlink()
        logger.info("Removed existing database")
    
    # Connect to new database
    con = duckdb.connect(str(SAMPLE_DB))
    
    try:
        # Configure for cloud constraints
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}';")
        con.execute(f"SET threads TO {THREADS};")
        logger.info(f"Database configured: {MEMORY_LIMIT} memory, {THREADS} threads")
        
        # Step 1: Ingest sample data
        logger.info("Ingesting sample parquet...")
        con.execute(f"""
            CREATE TABLE events AS 
            SELECT * FROM read_parquet('{str(SAMPLE_PARQUET)}')
            ORDER BY event_time
        """)
        
        event_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        logger.info(f"Loaded {event_count:,} events")
        
        # Step 2: Build the star schema + ML prediction tables (shared builder,
        # also used by app/db_utils.py's cloud-mode path, so both stay in sync)
        logger.info("Creating dimensional model...")
        build_all(con, logger=logger)

        # Step 3: Get database statistics
        logger.info("\n" + "="*60)
        logger.info("CLOUD DATABASE SUMMARY")
        logger.info("="*60)
        
        tables = con.execute("""
            SELECT table_name, estimated_size
            FROM duckdb_tables()
            WHERE schema_name = 'main'
            ORDER BY table_name
        """).fetchdf()
        
        for _, row in tables.iterrows():
            size_mb = row['estimated_size'] / (1024 * 1024)
            logger.info(f"{row['table_name']:30s} {size_mb:8.2f} MB")
        
        # Total size
        db_size_mb = SAMPLE_DB.stat().st_size / (1024 * 1024)
        logger.info(f"{'':30s} {'--------':>8s}")
        logger.info(f"{'Total Database Size':30s} {db_size_mb:8.2f} MB")
        logger.info("="*60)
        
        logger.info(f"\n✅ Cloud database created successfully at: {SAMPLE_DB}")
        
        if db_size_mb > 100:
            logger.warning(f"⚠️  Database size ({db_size_mb:.2f} MB) exceeds 100MB recommendation")
            logger.warning("   Consider reducing sample size or optimizing tables")
        else:
            logger.info(f"✅ Database size ({db_size_mb:.2f} MB) is within cloud limits")
        
    except Exception as e:
        logger.error(f"Error building database: {e}", exc_info=True)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    build_cloud_database()
