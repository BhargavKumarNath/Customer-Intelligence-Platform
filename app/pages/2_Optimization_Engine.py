import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'components'))
from components.code_viewer import show_code_reference, show_optimization_highlight
from glossary import show_glossary

st.set_page_config(page_title="Optimization Engine", page_icon="⚡", layout="wide")

st.title("⚡ Optimization Engine: 110M Rows in a 3 GB Memory Budget")
st.markdown("""
**Processing 109.95M Events on Commodity Hardware**

This page documents the **systematic optimization strategies** that let the full
109,950,743-row dataset (13.7 GB of raw CSV) run end-to-end on a 10 GB-RAM machine
without a cluster. Key achievements:

- **On-Disk Compression**: 13.7 GB CSV → 1.82 GB Parquet (**86.7% reduction**)
- **Bounded Memory**: the pipeline never loads the full table into RAM — a streaming
  DuckDB `COPY` on ingest, then every stage capped at a 3 GB `memory_limit` with
  disk spill (a naive `pandas.read_csv` of this data would need **~40 GB**)
- **Query Performance**: Sub-second latency via DuckDB OLAP engine

These techniques made advanced behavioral analytics feasible on consumer hardware without distributed computing.
""")

st.markdown("---")

# Executive Summary
st.header("📊 Optimization Impact Summary")

# Use wider columns and explicit formatting to prevent truncation
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("**Memory Reduction**")
    st.markdown("<h2 style='margin-top: 0;'>~40 GB → 3 GB</h2>", unsafe_allow_html=True)
    st.markdown("<p style='color: #10b981; font-size: 14px;'>pipeline memory budget</p>", unsafe_allow_html=True)
    st.caption("Naive pandas load vs. capped DuckDB memory_limit (disk spill on)")

with col2:
    st.markdown("**Storage Compression**") 
    st.markdown("<h2 style='margin-top: 0;'>13.7 GB → 1.82 GB</h2>", unsafe_allow_html=True)
    st.markdown("<p style='color: #10b981; font-size: 14px;'>↓ -86.7%</p>", unsafe_allow_html=True)
    st.caption("Parquet + ZSTD compression")

with col3:
    st.metric(
        "Query Performance",
        "< 1 second",
        help="Average analytical query latency"
    )

with col4:
    st.metric(
        "Sessions Created",
        "109.95M → 23.0M",
        help="Events aggregated into sessions (~45s)"
    )

st.markdown("---")

# Section 1: Data Type Optimization
st.header("1️⃣ Data Type Optimization Strategy")

st.markdown("""
**Challenge:** Pandas/NumPy defaults to 64-bit types, wasting memory when smaller types suffice.

**Solution:** `summarise/optimize_dataset.py` reads both raw CSVs with an explicit
column-type map and writes Parquet, so the id columns are never widened and low-cardinality
strings are dictionary-encoded per row group. `event_type` / `brand` / `category_code` stay
as strings on the way in — Parquet's own dictionary + ZSTD handles them, which avoids
building a 23M-entry `user_session` category map in RAM (that was the OOM in the old
two-pass Polars version).
""")

# Optimization table
optimization_df = pd.DataFrame({
    "Column": ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session"],
    "Raw CSV Type": ["string", "string", "Int64", "Int64", "string", "string", "Float64", "Int64", "string (UUID)"],
    "Optimised Parquet Type": ["Timestamp", "string (dict)", "Int32", "Int64", "string (dict)", "string (dict)", "Float32", "Int32", "string"],
    "Technique": ["parse once", "Parquet dict + ZSTD", "downcast Int64→Int32", "kept (real ids > 2^31)", "Parquet dict + ZSTD", "Parquet dict + ZSTD", "downcast Float64→Float32", "downcast Int64→Int32", "left as-is (near-unique)"],
})

st.dataframe(optimization_df, width='stretch', hide_index=True)

# Show code reference
st.markdown("#### 🔍 Implementation Details")
show_code_reference(
    file_path="summarise/optimize_dataset.py",
    description="Streaming DuckDB COPY that reads the raw CSVs and writes the optimised Parquet"
)

show_glossary("Categorical Encoding")
show_glossary("Polars")

st.markdown("---")

# Section 2: Compression Strategy
st.header("2️⃣ Compression & Storage Optimization")

st.markdown("""
**Challenge:** Raw CSV files are 13.7 GB on disk and slow to load.

**Solution:** Parquet format with ZSTD compression level 3 (balanced speed/compression ratio).
""")

# Compression comparison chart
compression_data = pd.DataFrame({
    "Format": ["Raw CSV", "Parquet (Snappy)", "Parquet (ZSTD L3)", "Parquet (ZSTD L10)"],
    "Size (GB)": [13.7, 2.6, 1.82, 1.6],
    "Write Time (min)": ["-", 1.5, 2.5, 9.0],
    "Read Time (s)": [200, 6, 4, 4]
})

fig_compression = px.bar(
    compression_data,
    x="Format",
    y="Size (GB)",
    title="Storage Format Comparison (110M rows)",
    text_auto=True,
    color="Size (GB)",
    color_continuous_scale="Reds_r"
)
fig_compression.update_layout(showlegend=False, height=400)
st.plotly_chart(fig_compression, width='stretch')

st.info("""
**Why ZSTD Level 3?**
- **Level 1-5:** Fast compression, good for iterative development
- **Level 6-10:** Slower but higher compression, only for final archival
- **Level 3:** Sweet spot: 86.7% reduction (13.7 GB → 1.82 GB) with minimal write overhead
""")

show_code_reference(
    file_path="summarise/optimize_dataset.py",
    description="Parquet compression with ZSTD codec (COMPRESSION ZSTD, COMPRESSION_LEVEL 3)"
)

st.markdown("---")

# Section 3: DuckDB Query Optimization
st.header("3️⃣ DuckDB Query Engine Optimization")

st.markdown("""
**Challenge:** Running complex aggregations on 109M rows can exceed available RAM.

**Solution:** Configure DuckDB with memory limits and thread control for stable execution.
""")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### Configuration Applied")
    st.code("""
# src/utils/duckdb_env.py, applied by every pipeline script
con.execute("SET memory_limit='3GB';")   # CIP_DUCKDB_MEMORY_LIMIT
con.execute("SET threads TO 2;")          # CIP_DUCKDB_THREADS
con.execute("SET preserve_insertion_order=false;")
con.execute("SET temp_directory='.../.duckdb_spill';")
    """, language="sql")
    
    st.markdown("""
    **Rationale:**
    - **3 GB limit:** the full pipeline was frozen a 10 GB box at 10-12 GB; 3 GB + spill is safe
    - **2 threads:** fewer parallel partial-aggregate hash tables = lower peak RSS
    - **temp_directory + spill:** disk is the cheap resource; large GROUP BYs spill instead of OOM
    - override all three with `CIP_DUCKDB_*` env vars on a bigger machine
    """)

with col2:
    st.markdown("#### Query Performance Metrics")
    
    query_perf = pd.DataFrame({
        "Operation": ["dim_products", "fact_daily_kpis", "dim_users", "fact_sessions", "RFM segments"],
        "Rows In": ["110M", "110M", "110M", "110M", "1.66M purchases"],
        "Rows Out": ["207K", "61", "5.32M", "23.0M", "697K buyers"],
        "Time (seconds)": [21, 10, 46, 45, 2],
        "Optimization": ["DISTINCT ON", "GROUP BY date", "GROUP BY user_id", "GROUP BY session UUID", "NTILE windowing"]
    })
    
    st.dataframe(query_perf, width='stretch', hide_index=True)

show_code_reference(
    file_path="src/utils/duckdb_env.py",
    description="Shared DuckDB session tuning (memory_limit / threads / temp_dir) for every pipeline script"
)

show_glossary("DuckDB")

st.markdown("---")

# Section 4: Dimensional Modeling
st.header("4️⃣ Dimensional Modeling for Analytics")

st.markdown("""
**Challenge:** Repeatedly scanning 109M events for every query is inefficient.

**Solution:** Build a **star schema** with dimension tables (users, products) and fact tables (sessions, daily KPIs).
""")

st.markdown("""
**Star Schema Benefits:**
- **Simplicity:** Easy to understand and query
- **Performance:** Denormalized for fast aggregations (no complex joins)
- **Scalability:** Fact tables can grow independently of dimensions
- **Analytics-First:** Optimized for OLAP queries, not OLTP
""")

# Display dimensional modeling diagram
import os
dim_model_path = "dimensional_modeling.svg"

if os.path.exists(dim_model_path):
    st.image(dim_model_path, width='stretch', caption="Star Schema Architecture")
else:
    st.info("Dimensional model diagram not found")

st.markdown("""
**Tables Created:**
- `dim_users` - 5.32M user profiles with metadata
- `dim_products` - 206,876 product catalog with categories
- `fact_sessions` - 23.0M sessions with behavioral metrics
- `fact_daily_kpis` - 61 days of aggregated performance
- `events` - raw 109.95M event log (kept for drill-down)

**Key Insight:** By separating dimensions from facts, we avoid data duplication while maintaining query speed.
For example, user attributes (name, location) are stored once in `dim_users`, while behavior (clicks, purchases) 
lives in fact tables with foreign key references.
""")

show_glossary("Dimensional Modeling")

st.markdown("---")

# Section 5: Query-Specific Optimizations
st.header("5️⃣ Query-Specific Optimization Techniques")

# Use expanders for different techniques
with st.expander("**Technique 1: DISTINCT ON instead of Window Functions**"):
    st.markdown("""
    **Use Case:** Getting the latest product price.
    
    **Naive Approach (High Memory):**
    ```sql
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY event_time DESC) as rn
    FROM events
    WHERE rn = 1
    ```
    Memory: Creates full row numbering for 110M rows (~2GB overhead)
    
    **Optimized Approach:**
    ```sql
    SELECT DISTINCT ON (product_id)
        product_id, category_code, brand, price
    FROM events
    ORDER BY product_id, event_time DESC
    ```
    Memory: Streaming operation, minimal overhead
    """)
    
    show_code_reference(
        file_path="src/processing/initial_modeling.py",
        start_line=45,
        end_line=63,
        description="dim_products via DISTINCT ON (latest row per product)"
    )

with st.expander("**Technique 2: TEMP Tables for Multi-Step Aggregations**"):
    st.markdown("""
    **Use Case:** RFM calculation requires multiple aggregations.
    
    **Strategy:**
    1. Create TEMP TABLE with base RFM metrics
    2. Apply NTILE scoring in second query
    3. Join results without re-scanning raw events
    
    **Benefit:** Intermediate results cached in memory, avoiding redundant computation.
    """)
    
    show_code_reference(
        file_path="src/analysis/segmentation.py",
        start_line=28,
        end_line=45,
        description="TEMP table for RFM base metrics"
    )

with st.expander("**Technique 3: Streaming ingest instead of an in-memory pass**"):
    st.markdown("""
    **Use Case:** Turning the two 13.7 GB raw CSVs into one optimised Parquet.

    **Strategy:**
    - A single DuckDB `COPY (SELECT ... FROM read_csv([...])) TO 'x.parquet'`
    - DuckDB reads the CSVs in bounded-memory chunks and spills to `temp_directory`
    - No stage ever holds all 110M rows; `user_session` is left as a plain string so
      no 23M-entry category dictionary is built in RAM
    - The old approach (`pl.concat` of two `scan_csv` + a second `.cast(pl.Categorical)`
      pass) OOM'd on sub-16 GB machines
    
    **Benefit:** Query optimizer can push down filters, avoid unnecessary column reads.
    """)
    
    st.code("""
    # Memory-efficient pattern
    df = (
        pl.scan_parquet('data.parquet')
        .filter(pl.col('event_type') == 'purchase')  # Pushed to scan
        .select(['user_id', 'price'])                 # Only read needed columns
        .groupby('user_id')
        .agg(pl.sum('price'))
        .collect()  # Execute
    )
    """, language="python")

st.markdown("---")

# Section 6: Scalability Analysis
st.header("6️⃣ Scalability & Bottleneck Analysis")

st.markdown("""
**Question:** Where are the bottlenecks, and how would this scale to 1B rows?
""")

# Bottleneck analysis
bottleneck_df = pd.DataFrame({
    "Operation": [
        "Data Loading (Parquet)",
        "Type downcast (in the COPY)",
        "DuckDB Ingestion",
        "Sessionization (GROUP BY UUID)",
        "RFM Calculation",
        "Market Basket (Self-Join)"
    ],
    "Bound Type": ["I/O", "CPU", "I/O + Memory", "Memory + CPU", "CPU", "Memory + CPU"],
    "110M Rows Time": ["~90s (CSV->Parquet)", "included", "~90s", "45s", "2s", "7s"],
    "Estimated 1B Rows": ["~5min", "~7min", "~25min", "~12min", "~3min", "~15min*"],
    "Scaling Notes": [
        "Linear with disk speed",
        "Linear with CPU cores",
        "Depends on memory_limit",
        "UUID cardinality matters",
        "Linear if buyers stay ~5M",
        "Quadratic risk if not filtered"
    ]
})

st.dataframe(bottleneck_df, width='stretch', hide_index=True)

st.warning("""
**Critical for 1B rows:**
- **Market Basket Self-Join:** Filter to top N products *before* self-join to avoid OOM
- **Sessionization:** UUID cardinality determines groupby cost (23.0M sessions is manageable; drop non-spillable `mode()`)
- **Memory Limit:** Would need to increase to 32GB or use chunked processing
""")

st.markdown("---")

# Section 7: Before/After Comparison
st.header("7️⃣ Before/After Comparison")

col1, col2 = st.columns(2)

with col1:
    st.error("### ❌ Before Optimization")
    st.markdown("""
    **Attempted Pandas Approach:**
    ```python
    df = pd.read_csv('data.csv')  # 12GB file
    # MemoryError: Unable to allocate 120GB
    ```
    
    **Why it failed:**
    - Default Int64/Float64 types
    - String columns stored as objects (pointers)
    - No compression
    - Full dataset loaded into RAM
    """)
    
    failure_metrics = pd.DataFrame({
        "Metric": ["Estimated RAM", "Load Time", "Query Performance", "Feasibility"],
        "Value": ["~40 GB (naive pandas)", "N/A (crashed)", "N/A", "❌ Impossible on a 10-16 GB box"]
    })
    st.table(failure_metrics)

with col2:
    st.success("### ✅ After Optimization")
    st.markdown("""
    **Optimized Pipeline:**
    ```python
    # ingest: one streaming DuckDB COPY (bounded memory)
    # every stage: SET memory_limit='3GB'; disk spill on
    con = duckdb.connect('behavior.duckdb')
    ```
    
    **Why it succeeded:**
    - Int32/Float32 downcast + Parquet dict + ZSTD
    - streaming ingest, never the full table in RAM
    - capped `memory_limit` with disk spill on every stage
    - dimensional modeling (pre-aggregation)
    """)
    
    success_metrics = pd.DataFrame({
        "Metric": ["Peak RSS", "Full Pipeline Time", "Query Performance", "Feasibility"],
        "Value": ["~3 GB", "~15 minutes", "< 1 second", "✅ Runs on a 10 GB box"]
    })
    st.table(success_metrics)

st.markdown("---")

# Section 8: Industry Comparison
st.header("8️⃣ Industry Comparison: Why Not Use Spark/Cloud?")

comparison_df = pd.DataFrame({
    "Aspect": ["Infrastructure", "Cost", "Latency", "Scalability", "Best For"],
    "This Approach (DuckDB)": [
        "Single node, local",
        "$0 (commodity hardware)",
        "< 1s query",
        "Up to ~500M rows on 32GB RAM",
        "Analytics, prototyping, small-medium data"
    ],
    "PySpark": [
        "Cluster (3-10 nodes)",
        "$50-500/month cloud",
        "~5-30s overhead",
        "Billions of rows",
        "ETL pipelines, distributed processing"
    ],
    "Cloud Warehouse (BigQuery)": [
        "Fully managed",
        "$5-50/TB processed",
        "~1-5s query",
        "Petabyte scale",
        "Multi-user, production analytics"
    ]
})

st.dataframe(comparison_df, width='stretch', hide_index=True)

st.info("""
**Verdict:** For this project (110M rows, single analyst, cost-sensitive), DuckDB is the optimal choice. 
Moving to Spark or cloud warehouse would add complexity and cost without meaningful performance gains at this scale.
""")

st.markdown("---")

# Footer with key takeaways
st.success("""
### 🎯 Key Takeaways

1. **Type Optimization:** Int32/Float32 downcast + Parquet dictionary encoding
2. **Compression:** ZSTD Level 3 achieved 86.7% storage reduction (13.7 GB -> 1.82 GB)
3. **DuckDB Configuration:** Memory limits and thread control ensure stable execution on limited RAM
4. **Dimensional Modeling:** Pre-aggregation enables 10x faster analytical queries
5. **Right Tool for Scale:** DuckDB is optimal for 100M-500M row analytics on single node

**These techniques are directly applicable to any data science project facing resource constraints.**
""")
