import streamlit as st
import plotly.graph_objects as go
from db_utils import run_query
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'components'))
from glossary import show_glossary

st.set_page_config(page_title="Project Overview", page_icon="🎯", layout="wide")

st.title("🎯 Customer Intelligence Platform - Project Overview")

# Hero Section
st.markdown("""
This platform demonstrates an **end to end production grade data science pipeline** processing **109 Million e-commerce events** 
within a **3 GB DuckDB memory budget** on a single machine, showcasing optimization techniques, analytical rigor, and ML insights typically 
found in FAANG companies.
""")

# Key Stats
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Events Processed",
        "109M",
        help="Total behavioral events from Oct-Nov 2019"
    )

with col2:
    st.metric(
        "Optimised Parquet",
        "1.82 GB",
        delta="-87%",
        delta_color="inverse",
        help="On disk, from the 13.7 GB raw CSV (a naive pandas load would need ~40 GB RAM)"
    )

with col3:
    st.metric(
        "Query Latency",
        "< 1 sec",
        help="Sub-second analytical queries via DuckDB"
    )

with col4:
    st.metric(
        "ML Lift",
        "4.6x",
        help="Top-5% propensity cohort vs baseline (full-data model; see src/models/metrics.json)"
    )

st.markdown("---")

# Local vs Cloud Setup
from db_utils import is_sample_mode, get_dataset_info

dataset_info = get_dataset_info()

st.header("🌐 Deployment Modes")

if dataset_info['is_sample']:
    st.info("""
    **💡 You're viewing the Cloud Demo**
    
    This deployment uses a representative sample dataset to work within Streamlit Cloud constraints. 
    All features, visualizations, and ML models work identically just with a smaller dataset.
    """)

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🌐 Cloud Deployment (Current)")
    st.markdown(f"""
    **What's Included:**
    - ✅ Sample dataset ({dataset_info['event_count']:,} events)
    - ✅ All dashboard pages functional
    - ✅ Complete ML models and analytics
    - ✅ Interactive visualizations
    - ✅ Instant access, no setup
    
    **Limitations:**
    - ⚠️ Sample data (~{dataset_info['event_count']/1000000:.1f}M events vs full 109M)
    - ⚠️ Metrics reflect sample size
    """)

with col2:
    st.markdown("#### 🖥️ Local Setup (Full Experience)")
    st.markdown("""
    **Complete Dataset:**
    - ✅ All 109M behavioral events
    - ✅ Full analytics and insights
    - ✅ Faster performance (more RAM)
    - ✅ Customization capabilities
    
    **Setup Steps:**
    1. Clone repository from GitHub
    2. Download [Kaggle dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)
    3. Run preprocessing scripts
    4. Build database and launch
    
    **📚 Detailed Guide:** See [LOCAL_SETUP.md](https://github.com/BhargavKumarNath/Customer-Intelligence-Platform/blob/main/docs/LOCAL_SETUP.md)
    """)

st.markdown("---")

# Architecture Diagram
st.header("🏗️ System Architecture")

st.markdown("""
The platform follows a modern data science pipeline with optimization at every layer:

**Data Provenance:**
- **Original Dataset:** [Kaggle - eCommerce Behavior Data (2019-Oct/Nov)](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- **Scale:** 109,950,743 rows / 13.7 GB of CSV (Oct + Nov 2019)
- **Ingestion Strategy:** `summarise/optimize_dataset.py` streams both raw CSVs through DuckDB in one bounded-memory pass, downcasts numeric types, and writes a 1.82 GB ZSTD Parquet. The full pipeline then runs on that within a 3 GB DuckDB memory budget (no sampling) — see the Optimization Engine page.
""")

# Display the SVG diagram
import os
svg_path = "system_design.svg"

if os.path.exists(svg_path):
    st.image(svg_path, width='stretch', caption="End-to-End System Architecture")
else:
    # Fallback to simplified text description if SVG not found
    st.info("""
    **Pipeline Flow:**
    1. Raw CSV (13.7GB) → Optimized Parquet (1.82GB) via a streaming DuckDB COPY + type downcast
    2. Parquet → DuckDB (3GB memory limit, disk spill on)
    3. DuckDB → Star Schema (dim_users, dim_products, fact_sessions, fact_daily_kpis)
    4. Feature Engineering → RFM Analysis, LightGBM Propensity Model, Market Basket Analysis
    5. All outputs → Streamlit Dashboard with 4 modules
    """)


st.markdown("---")

# The Challenge
st.header("💪 The Challenge")

st.markdown("""
**Problem:** Analyze 109 million e-commerce events with the following constraints:

- **Hardware:** Single machine, full run done within a 3 GB DuckDB memory budget (no cluster/cloud infrastructure)
- **Performance:** Interactive dashboard with sub-second query response
- **Scale:** Process billions of potential product pairs for recommendations
- **Complexity:** Multi-dimensional analysis (user segments, cohorts, propensity scoring, A/B testing)
""")

col1, col2 = st.columns(2)

with col1:
    st.error("#### ❌ Naive Approach Would Fail")
    st.markdown("""
    - **Pandas DataFrame:** ~40 GB in memory (OOM on the target box)
    - **Default data types:** Int64, Float64, String (8-16 bytes/value)
    - **PySpark overhead:** Cluster required, slow on single node
    - **No optimization:** Full scans, repeated computations
    """)

with col2:
    st.success("#### ✅ Optimized Solution")
    st.markdown("""
    - **DuckDB OLAP:** Columnar storage, vectorized execution
    - **Polars preprocessing:** Categorical encoding, Int32, Float32
    - **Star schema:** Denormalized for analytical workloads
    - **Smart aggregations:** Pre-computed summaries (products, users, daily)
    """)

st.markdown("---")

# The Solution
st.header("🎯 The Solution: Industry-Grade Optimizations")

# Create tabs for different solution aspects
tab1, tab2, tab3, tab4 = st.tabs(["Data Engineering", "Analytics", "Machine Learning", "Results"])

with tab1:
    st.subheader("Optimization Techniques Applied")
    
    optimization_data = {
        "Technique": [
            "Type Casting (Int64→Int32)",
            "Type Casting (Float64→Float32)",
            "Categorical Encoding (UUIDs)",
            "ZSTD Compression (Level 3)",
            "Dimensional Modeling",
            "Query Optimization (Memory Limit)"
        ],
        "Impact": [
            "50% memory reduction",
            "50% memory reduction",
            "90% memory reduction",
            "70% storage reduction",
            "10x faster joins",
            "Stable processing within a 3 GB memory budget"
        ],
        "Tool": [
            "Polars",
            "Polars",
            "Polars",
            "Parquet",
            "DuckDB SQL",
            "DuckDB Config"
        ]
    }
    
    import pandas as pd
    st.table(pd.DataFrame(optimization_data))
    
    show_glossary("Categorical Encoding")
    show_glossary("DuckDB")

with tab2:
    st.subheader("Analytical Capabilities")
    
    st.markdown("""
    **RFM Segmentation:**
    - 700K buyers classified into 8 behavioral segments
    - NTILE(5) quantile scoring on Recency, Frequency, Monetary
    - Actionable segments: Champions, Loyal, At-Risk, Hibernating
    
    **Cohort Analysis:**
    - Weekly user retention tracking
    - Identifies Week-1 churn (65% drop-off)
    - Enables product-market fit assessment
    
    **Conversion Funnel:**
    - Session-based: View → Cart → Purchase
    - Overall conversion: ~X% (from fact_sessions)
    """)
    
    show_glossary("RFM")
    show_glossary("Cohort Analysis")

with tab3:
    st.subheader("Machine Learning Models")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**🎯 Propensity Model (LightGBM)**")
        st.markdown("""
        - **Objective:** Predict November purchases from October behavior
        - **Model:** Gradient Boosted Trees
        - **Features:** Recency, session count, cart events, purchase history
        - **Performance:** 4.6x lift vs random targeting
        - **Application:** Targeted marketing to top 5% high-propensity users
        """)
    
    with col2:
        st.markdown("**🛒 Recommendation Engine (Market Basket)**")
        st.markdown("""
        - **Algorithm:** Association Rules (Lift-based)
        - **Scale:** Computed 7,704 product-pair rules in ~7 seconds
        - **Metric:** Lift > 1.2 (positive correlation threshold)
        - **Application:** "Customers who bought A also bought B" suggestions
        """)
    
    show_glossary("LightGBM")
    show_glossary("Lift (Market Basket)")

with tab4:
    st.subheader("Key Achievements")
    
    st.success("""
    #### 🏆 Technical Excellence
    - **Data Engineering:** Processed 109.95M rows within a 3 GB memory budget (87% smaller on disk)
    - **Performance:** Sub-second analytical queries via DuckDB OLAP engine
    - **Scalability:** Dimensional modeling enables 10x faster joins
    """)
    
    st.info("""
    #### 💡 Business Impact
    - **Segmentation:** Identified 8 distinct user segments with tailored strategies
    - **ML-Driven Targeting:** 4.6x conversion lift (36.9% vs 8.0% baseline)
    - **Personalization:** Market basket analysis for cross-sell opportunities
    """)
    
    st.warning("""
    #### 🔬 Experimental Rigor
    - **A/B Testing:** Bayesian simulation with power analysis
    - **Statistical Testing:** T-tests, confidence intervals, multiple testing correction
    - **Decision Framework:** Amazon-inspired bar raiser principles
    """)

st.markdown("---")

# Tech Stack
st.header("🛠️ Technology Stack")

tech_stack = {
    "Layer": ["Data Ingestion", "Storage & Query Engine", "Data Processing", "Machine Learning", "Experimentation", "Visualization"],
    "Technology": ["Polars 0.20+", "DuckDB 0.9+", "DuckDB SQL + Polars", "LightGBM + Scikit-Learn", "SciPy Stats + NumPy", "Streamlit + Plotly"],
    "Why This Choice": [
        "Lazy evaluation, parallel processing, 10x faster than Pandas",
        "Embedded OLAP, columnar storage, zero infrastructure",
        "SQL for complex joins, Polars for transformations",
        "Fast GBDT with GPU support, handles class imbalance",
        "Statistical testing, power analysis, Bayesian simulation",
        "Rapid prototyping, interactive widgets, production-ready"
    ]
}

import pandas as pd
st.dataframe(pd.DataFrame(tech_stack), width='stretch', hide_index=True)

st.markdown("---")

# Navigation Guide
st.header("🗺️ Dashboard Navigation Guide")

st.markdown("""
This dashboard is organized to tell a complete story. Here's what each section covers:
""")

pages_info = {
    "Page": [
        "📈 Executive Overview",
        "👥 User Intelligence",
        "🧪 Experiment Lab",
        "🤖 ML Engine",
        "⚡ Optimization Engine",
        "🔍 Data Explorer"
    ],
    "What You'll Learn": [
        "High-level KPIs, revenue trends, conversion funnel health",
        "RFM segmentation, cohort retention, churn risk analysis",
        "A/B testing methodology, statistical rigor, experiment simulation",
        "Propensity modeling, recommendation engine, feature importance",
        "All optimization techniques, performance metrics, architecture decisions",
        "Dataset characteristics, distributions, data quality insights"
    ],
    "Key Takeaway": [
        "Business performance at a glance",
        "Who our users are and how to retain them",
        "How to run rigorous experiments",
        "AI-driven decision making and personalization",
        "How the full 110M-row pipeline runs in a 3 GB memory budget",
        "Understanding the data foundation"
    ]
}

st.dataframe(pd.DataFrame(pages_info), width='stretch', hide_index=True)

st.markdown("---")

# What Makes This Industry-Grade
st.header("🌟 What Makes This Industry-Grade?")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### Scalable Architecture")
    st.markdown("""
    - **OLAP-first design:** Columnar storage, vectorized execution
    - **Dimensional modeling:** Optimized for analytical workloads
    - **Query optimization:** Memory limits, thread control, indexed lookups
    - **Preprocessing:** Categorical encoding, type optimization, compression
    """)
    
    st.markdown("#### Rigorous Experimentation")
    st.markdown("""
    - **Statistical testing:** T-tests, confidence intervals, p-values
    - **Power analysis:** Sample size justification, effect size calculation
    - **Multiple testing correction:** Bonferroni, family-wise error rate
    - **Decision framework:** Clear success criteria, guardrail metrics
    """)

with col2:
    st.markdown("#### ML-Driven Insights")
    st.markdown("""
    - **Propensity modeling:** Temporal split, class imbalance handling
    - **Feature engineering:** Behavioral signals, aggregation patterns
    - **Model evaluation:** AUC-ROC, precision-recall, calibration
    - **Business impact:** 4.6x lift, ROI quantification
    """)
    
    st.markdown("#### Production Engineering")
    st.markdown("""
    - **Single-node processing:** No cluster required (cost optimization)
    - **Storage efficiency:** 87% smaller on disk; 3 GB pipeline memory budget
    - **Query performance:** Sub-second latency on 100M+ rows
    - **Code quality:** Modular pipeline, version control, documentation
    """)

st.markdown("---")

# Footer
st.info("""
**👉 Start exploring:** Use the sidebar to navigate through different modules. Each page builds on the foundation 
established here, diving deeper into specific analytical capabilities and technical implementations.
""")
