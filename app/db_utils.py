import duckdb
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import timedelta
import os
from pathlib import Path

# Detect environment
IS_CLOUD = os.getenv('STREAMLIT_SHARING', 'false').lower() == 'true'
SAMPLE_MODE = not os.path.exists("data/db/behavior.duckdb")

# Database paths
if SAMPLE_MODE or IS_CLOUD:
    PARQUET_PATH = "data/sample/sample_optimized.parquet"
else:
    DB_PATH = "data/db/behavior.duckdb"
    PARQUET_PATH = None

# Initialize connection once and cache it
@st.cache_resource
def get_connection():
    """Get database connection, creating from parquet for cloud compatibility"""
    
    # For cloud/sample mode, create database from parquet to avoid Windows/Linux compatibility
    if IS_CLOUD or SAMPLE_MODE:
        if not os.path.exists(PARQUET_PATH):
            st.error(f"Sample data not found at: {PARQUET_PATH}")
            st.info("Please ensure sample_optimized.parquet is in the repository.")
            st.stop()
        
        # Create in-memory database from parquet (avoids binary compatibility issues)
        try:
            con = duckdb.connect(':memory:')
            
            # Configure for cloud
            con.execute("SET memory_limit='512MB';")
            con.execute("SET threads TO 2;")
            
            # Load events from parquet
            con.execute(f"""
                CREATE TABLE events AS 
                SELECT * FROM read_parquet('{PARQUET_PATH}')
            """)
            
            # Create dim_products
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
            
            # Create dim_users
            con.execute("""
                CREATE TABLE dim_users AS
                SELECT 
                    user_id,
                    MIN(event_time) as first_seen,
                    MAX(event_time) as last_seen,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT user_session) as session_count,
                    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
                    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as total_spend
                FROM events
                GROUP BY user_id
            """)
            
            # Create fact_daily_kpis
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
            
            # Create fact_sessions
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
            
            # Create user_rfm_segments (for User Intelligence page)
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
            
            # Create predictions_product_affinity (for ML Engine page)
            # Market basket analysis - which products are purchased together
            con.execute("""
                CREATE TABLE predictions_product_affinity AS
                WITH purchase_pairs AS (
                    SELECT 
                        e1.product_id as product_a,
                        e2.product_id as product_b,
                        e1.user_session
                    FROM events e1
                    JOIN events e2 
                        ON e1.user_session = e2.user_session 
                        AND e1.product_id < e2.product_id
                    WHERE e1.event_type = 'purchase' 
                        AND e2.event_type = 'purchase'
                        AND e1.product_id IS NOT NULL 
                        AND e2.product_id IS NOT NULL
                ),
                pair_stats AS (
                    SELECT 
                        product_a,
                        product_b,
                        COUNT(DISTINCT user_session) as pair_count
                    FROM purchase_pairs
                    GROUP BY product_a, product_b
                    HAVING COUNT(DISTINCT user_session) >= 3
                ),
                product_support AS (
                    SELECT 
                        product_id,
                        COUNT(DISTINCT user_session) as product_count
                    FROM events
                    WHERE event_type = 'purchase' AND product_id IS NOT NULL
                    GROUP BY product_id
                )
                SELECT 
                    ps.product_a,
                    ps.product_b,
                    ps.pair_count,
                    CAST(ps.pair_count AS FLOAT) / NULLIF(pa.product_count, 0) as confidence,
                    (CAST(ps.pair_count AS FLOAT) / NULLIF(pa.product_count, 0)) / 
                        (CAST(pb.product_count AS FLOAT) / NULLIF((SELECT COUNT(DISTINCT user_session) FROM events WHERE event_type = 'purchase'), 0)) 
                        as lift
                FROM pair_stats ps
                JOIN product_support pa ON ps.product_a = pa.product_id
                JOIN product_support pb ON ps.product_b = pb.product_id
                WHERE ps.pair_count >= 3
                ORDER BY lift DESC
            """)
            
            return con
            
        except Exception as e:
            st.error(f"❌ Error creating database from parquet: {e}")
            st.stop()
    
    else:
        # Local mode - use existing full database
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
            con.execute("SET memory_limit='8GB';")
            con.execute("SET threads TO 4;")
            return con
        except Exception as e:
            st.error(f"❌ Database connection error: {e}")
            st.stop()

def run_query(query):
    con = get_connection()
    return con.execute(query).fetchdf()

def format_currency(val):
    return f"${val:,.0f}"

def format_number(val):
    if val >= 1_000_000:
        return f"{val/1_000_000:.1f}M"
    elif val >= 1_000:
        return f"{val/1_000:.1f}K"
    else:
        return f"{val:,.0f}"

def is_sample_mode():
    """Check if running in sample data mode"""
    return SAMPLE_MODE or IS_CLOUD

def get_dataset_info():
    """Get information about the current dataset"""
    try:
        con = get_connection()
        event_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        
        is_sample = is_sample_mode()
        
        if is_sample:
            return {
                'event_count': event_count,
                'is_sample': True,
                'mode': 'Cloud Sample' if IS_CLOUD else 'Sample',
                'notice': '🌐 Cloud Demo Mode - Using representative sample',
                'display_text': f'{event_count:,} events (sample dataset)'
            }
        else:
            return {
                'event_count': event_count,
                'is_sample': False,
                'mode': 'Full Dataset',
                'notice': '🖥️ Local Mode - Using complete dataset',
                'display_text': f'{event_count:,} events (full dataset)'
            }
    except Exception as e:
        return {
            'event_count': 0,
            'is_sample': True,
            'mode': 'Unknown',
            'notice': '⚠️ Database error',
            'display_text': '0 events (error)'
        }
