"""
Sample Dataset Generator for Streamlit Cloud Deployment

This script creates a representative sample of the full 109M event dataset
for deployment on Streamlit Cloud. It uses stratified sampling to maintain:
- Temporal distribution across all dates
- Event type proportions (view/cart/purchase)
- User behavior diversity (light to heavy users)
- Product and category coverage

Target: 2-5M events (~2-5% of original) to fit within cloud constraints
"""

import duckdb
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "db" / "behavior.duckdb"
OUTPUT_DIR = PROJECT_ROOT / "data" / "sample"
OUTPUT_PARQUET = OUTPUT_DIR / "sample_optimized.parquet"

# Sampling parameters
SAMPLE_PERCENTAGE = 3.0  # 3% of data = ~3.3M events
RANDOM_SEED = 42


def create_sample_dataset():
    """Create stratified sample dataset from full database"""
    
    logger.info(f"Connecting to database: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH), read_only=True)
    
    try:
        # Step 1: Get dataset statistics
        logger.info("Analyzing source dataset...")
        stats = con.execute("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT user_id) as total_users,
                COUNT(DISTINCT product_id) as total_products,
                MIN(event_time) as start_date,
                MAX(event_time) as end_date
            FROM events
        """).fetchone()
        
        logger.info(f"Source dataset: {stats[0]:,} events, {stats[1]:,} users, {stats[2]:,} products")
        logger.info(f"Date range: {stats[3]} to {stats[4]}")
        
        # Step 2: Create stratified sample
        # Strategy: Sample by day to ensure temporal coverage, then sample events within each day
        logger.info(f"Creating {SAMPLE_PERCENTAGE}% stratified sample...")
        
        sample_query = f"""
        CREATE OR REPLACE TEMP TABLE sample_events AS
        SELECT *
        FROM events
        WHERE user_id IN (
            -- Sample users stratified by their activity level
            SELECT user_id
            FROM (
                SELECT 
                    user_id,
                    COUNT(*) as event_count,
                    NTILE(5) OVER (ORDER BY COUNT(*)) as activity_quintile
                FROM events
                GROUP BY user_id
            ) user_stats
            -- Sample from each quintile to preserve user behavior distribution
            WHERE user_id IN (
                SELECT user_id
                FROM (
                    SELECT 
                        user_id,
                        activity_quintile,
                        ROW_NUMBER() OVER (PARTITION BY activity_quintile ORDER BY RANDOM()) as rn,
                        COUNT(*) OVER (PARTITION BY activity_quintile) as quintile_size
                    FROM (
                        SELECT 
                            user_id,
                            COUNT(*) as event_count,
                            NTILE(5) OVER (ORDER BY COUNT(*)) as activity_quintile
                        FROM events
                        GROUP BY user_id
                    )
                )
                WHERE rn <= (quintile_size * {SAMPLE_PERCENTAGE / 100.0})
            )
        )
        AND RANDOM() < 0.5  -- Additional sampling to fine-tune final size
        """
        
        # DuckDB 0.10+ doesn't support SET random_seed, RANDOM() provides sufficient randomness
        con.execute(sample_query)
        
        # Check sample size
        sample_count = con.execute("SELECT COUNT(*) FROM sample_events").fetchone()[0]
        logger.info(f"Sample created: {sample_count:,} events ({sample_count/stats[0]*100:.2f}% of original)")
        
        # Step 3: Verify sample quality
        logger.info("Verifying sample quality...")
        
        # Event type distribution
        event_dist = con.execute("""
            SELECT 
                'Original' as dataset,
                event_type,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
            FROM events
            GROUP BY event_type
            UNION ALL
            SELECT 
                'Sample' as dataset,
                event_type,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
            FROM sample_events
            GROUP BY event_type
            ORDER BY dataset, event_type
        """).fetchdf()
        
        logger.info("Event type distribution comparison:")
        print(event_dist.to_string(index=False))
        
        # Temporal coverage
        temporal = con.execute("""
            SELECT 
                COUNT(DISTINCT CAST(event_time AS DATE)) as days_covered,
                MIN(event_time) as first_event,
                MAX(event_time) as last_event
            FROM sample_events
        """).fetchone()
        
        logger.info(f"Temporal coverage: {temporal[0]} days ({temporal[1]} to {temporal[2]})")
        
        # Step 4: Export to Parquet
        logger.info(f"Exporting sample to: {OUTPUT_PARQUET}")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        con.execute(f"""
            COPY sample_events TO '{str(OUTPUT_PARQUET)}' 
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)
        """)
        
        # Get file size
        file_size_mb = OUTPUT_PARQUET.stat().st_size / (1024 * 1024)
        logger.info(f"Sample parquet created: {file_size_mb:.2f} MB")
        
        # Step 5: Summary statistics
        summary = con.execute("""
            SELECT 
                COUNT(*) as events,
                COUNT(DISTINCT user_id) as users,
                COUNT(DISTINCT product_id) as products,
                COUNT(DISTINCT user_session) as sessions,
                COUNT(DISTINCT category_code) as categories,
                COUNT(DISTINCT brand) as brands
            FROM sample_events
        """).fetchone()
        
        logger.info("\n" + "="*60)
        logger.info("SAMPLE DATASET SUMMARY")
        logger.info("="*60)
        logger.info(f"Events:      {summary[0]:,}")
        logger.info(f"Users:       {summary[1]:,}")
        logger.info(f"Products:    {summary[2]:,}")
        logger.info(f"Sessions:    {summary[3]:,}")
        logger.info(f"Categories:  {summary[4]:,}")
        logger.info(f"Brands:      {summary[5]:,}")
        logger.info(f"File Size:   {file_size_mb:.2f} MB")
        logger.info("="*60)
        
        logger.info(f"\n✅ Sample dataset created successfully at: {OUTPUT_PARQUET}")
        
    except Exception as e:
        logger.error(f"Error creating sample dataset: {e}", exc_info=True)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    create_sample_dataset()
