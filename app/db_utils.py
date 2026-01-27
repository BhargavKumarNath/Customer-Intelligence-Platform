import duckdb
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import timedelta
import os
from pathlib import Path

# Detect environment (cloud vs local)
# Streamlit Cloud sets this environment variable
IS_CLOUD = os.getenv('STREAMLIT_SHARING', 'false').lower() == 'true' or \
           not os.path.exists("data/db/behavior.duckdb")

# Database path based on environment
if IS_CLOUD:
    DB_PATH = "data/sample/sample.duckdb"
    SAMPLE_MODE = True
else:
    # Check if sample database exists for local testing
    if os.path.exists("data/sample/sample.duckdb"):
        DB_PATH = "data/sample/sample.duckdb"
        SAMPLE_MODE = True
    else:
        DB_PATH = "data/db/behavior.duckdb"
        SAMPLE_MODE = False

# Initialize connection once and cache it
@st.cache_resource
def get_connection():
    """Get DuckDB connection with environment-appropriate settings"""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        
        # Configure based on environment
        if IS_CLOUD or SAMPLE_MODE:
            # Cloud optimization: reduced memory and threads
            con.execute("SET memory_limit='512MB';")
            con.execute("SET threads TO 2;")
        else:
            # Local: can use more resources
            con.execute("SET memory_limit='8GB';")
            con.execute("SET threads TO 3;")
        
        return con
    except Exception as e:
        st.error(f"❌ Database connection error: {e}")
        st.error(f"Looking for database at: {DB_PATH}")
        st.info("💡 If running locally, make sure to generate the sample database first:\n```bash\npython scripts/create_sample_dataset.py\npython scripts/create_cloud_database.py\n```")
        raise

def run_query(query):
    """Execute query and return DataFrame"""
    con = get_connection()
    return con.execute(query).fetchdf()

def is_sample_mode():
    """Check if running with sample data"""
    return SAMPLE_MODE

def get_dataset_info():
    """Get information about current dataset"""
    try:
        con = get_connection()
        count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        if SAMPLE_MODE:
            return {
                'is_sample': True,
                'event_count': count,
                'display_text': f'{count:,} events (sample dataset)',
                'notice': '🌐 Cloud Demo Mode - Using representative sample'
            }
        else:
            return {
                'is_sample': False,
                'event_count': count,
                'display_text': f'{count:,} events (full dataset)',
                'notice': '🖥️ Local Mode - Using complete dataset'
            }
    except:
        return {
            'is_sample': True,
            'event_count': 0,
            'display_text': 'Database not loaded',
            'notice': '⚠️ Database not found'
        }

def format_currency(val):
    """Format value as currency"""
    return f"${val:,.0f}"

def format_number(val):
    """Format value as number with thousands separator"""
    return f"{val:,.0f}"
