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
            st.error(f"❌ Sample data not found at: {PARQUET_PATH}")
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
        
        return {
            'event_count': event_count,
            'is_sample': is_sample,
            'mode': 'Cloud Sample' if IS_CLOUD else ('Sample' if SAMPLE_MODE else 'Full Dataset')
        }
    except:
        return {
            'event_count': 0,
            'is_sample': True,
            'mode': 'Unknown'
        }
