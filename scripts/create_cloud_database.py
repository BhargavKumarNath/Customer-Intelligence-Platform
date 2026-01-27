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
        
        # Step 2: Create dimension tables
        logger.info("Creating dimensional model...")
        
        # dim_products
        logger.info("  - Creating dim_products...")
        con.execute("""
            CREATE TABLE dim_products AS
            SELECT DISTINCT ON (product_id)
                product_id,
                category_id,
                COALESCE(category_code, 'unknown') as category_code,
                COALESCE(brand, 'unknown') as brand,
                price as current_price
            FROM events
            WHERE product_id IS NOT NULL
            ORDER BY product_id, event_time DESC
        """)
        
        product_count = con.execute("SELECT COUNT(*) FROM dim_products").fetchone()[0]
        logger.info(f"    Created {product_count:,} products")
        
        # dim_users
        logger.info("  - Creating dim_users...")
        con.execute("""
            CREATE TABLE dim_users AS
            SELECT 
                user_id,
                MIN(event_time) as first_seen,
                MAX(event_time) as last_seen,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_session) as session_count,
                SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
                SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as total_spend,
                MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as is_buyer
            FROM events
            GROUP BY user_id
        """)
        
        user_count = con.execute("SELECT COUNT(*) FROM dim_users").fetchone()[0]
        logger.info(f"    Created {user_count:,} users")
        
        # fact_sessions
        logger.info("  - Creating fact_sessions...")
        con.execute("""
            CREATE TABLE fact_sessions AS
            SELECT 
                user_session,
                user_id,
                MIN(event_time) as session_start,
                MAX(event_time) as session_end,
                CAST(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) AS INTEGER) as duration_sec,
                COUNT(*) as event_count,
                COUNT(DISTINCT product_id) as unique_products,
                MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as has_purchase,
                SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as session_revenue
            FROM events
            GROUP BY user_session, user_id
        """)
        
        session_count = con.execute("SELECT COUNT(*) FROM fact_sessions").fetchone()[0]
        logger.info(f"    Created {session_count:,} sessions")
        
        # fact_daily_kpis
        logger.info("  - Creating fact_daily_kpis...")
        con.execute("""
            CREATE TABLE fact_daily_kpis AS
            SELECT 
                CAST(event_time AS DATE) as date,
                COUNT(*) as daily_events,
                COUNT(DISTINCT user_id) as dau,
                COUNT(DISTINCT user_session) as daily_sessions,
                SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
                SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as carts,
                SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
                SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as daily_revenue
            FROM events
            GROUP BY CAST(event_time AS DATE)
            ORDER BY date
        """)
        
        days = con.execute("SELECT COUNT(*) FROM fact_daily_kpis").fetchone()[0]
        logger.info(f"    Created {days} daily KPI records")
        
        # Step 3: Create RFM segments
        logger.info("  - Creating user_rfm_segments...")
        con.execute("""
            CREATE TABLE user_rfm_segments AS
            WITH buyer_rfm AS (
                SELECT 
                    user_id,
                    DATE_DIFF('day', MAX(CAST(event_time AS DATE)), CURRENT_DATE) as recency_days,
                    COUNT(DISTINCT CAST(event_time AS DATE)) as frequency,
                    SUM(price) as monetary
                FROM events
                WHERE event_type = 'purchase'
                GROUP BY user_id
            ),
            rfm_scores AS (
                SELECT 
                    user_id,
                    recency_days,
                    frequency,
                    monetary,
                    NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,
                    NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
                    NTILE(5) OVER (ORDER BY monetary ASC) as m_score
                FROM buyer_rfm
            )
            SELECT 
                user_id,
                recency_days,
                frequency,
                monetary,
                r_score,
                f_score,
                m_score,
                r_score + f_score + m_score as rfm_total,
                CASE 
                    WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
                    WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
                    WHEN r_score >= 4 AND f_score <= 2 THEN 'Promising'
                    WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
                    WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
                    ELSE 'Regular'
                END as segment
            FROM rfm_scores
        """)
        
        rfm_count = con.execute("SELECT COUNT(*) FROM user_rfm_segments").fetchone()[0]
        logger.info(f"    Created {rfm_count:,} RFM segments")
        
        # Step 4: Create product affinity rules (market basket analysis)
        logger.info("  - Creating predictions_product_affinity...")
        con.execute("""
            CREATE TABLE predictions_product_affinity AS
            WITH product_pairs AS (
                SELECT 
                    a.product_id as product_a,
                    b.product_id as product_b,
                    COUNT(DISTINCT a.user_session) as pair_count
                FROM events a
                JOIN events b 
                    ON a.user_session = b.user_session 
                    AND a.product_id < b.product_id
                WHERE a.event_type = 'purchase' 
                    AND b.event_type = 'purchase'
                GROUP BY a.product_id, b.product_id
                HAVING COUNT(DISTINCT a.user_session) >= 3
            ),
            product_counts AS (
                SELECT 
                    product_id,
                    COUNT(DISTINCT user_session) as session_count
                FROM events
                WHERE event_type = 'purchase'
                GROUP BY product_id
            ),
            total_sessions AS (
                SELECT COUNT(DISTINCT user_session) as total
                FROM events
                WHERE event_type = 'purchase'
            )
            SELECT 
                pp.product_a,
                pp.product_b,
                pp.pair_count,
                pp.pair_count * 1.0 / pa.session_count as confidence,
                (pp.pair_count * 1.0 / pa.session_count) / (pb.session_count * 1.0 / ts.total) as lift
            FROM product_pairs pp
            JOIN product_counts pa ON pp.product_a = pa.product_id
            JOIN product_counts pb ON pp.product_b = pb.product_id
            CROSS JOIN total_sessions ts
            WHERE (pp.pair_count * 1.0 / pa.session_count) / (pb.session_count * 1.0 / ts.total) > 1.2
            ORDER BY lift DESC
        """)
        
        affinity_count = con.execute("SELECT COUNT(*) FROM predictions_product_affinity").fetchone()[0]
        logger.info(f"    Created {affinity_count:,} product affinity rules")
        
        # Step 5: Get database statistics
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
