# Customer Intelligence Platform
## From 109M Events to Actionable Business Insights

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10.2-yellow)](https://duckdb.org/)
[![Polars](https://img.shields.io/badge/Polars-0.20.10-orange)](https://www.pola.rs/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32.0-red)](https://streamlit.io/)

> **An end to end analytical data platform processing 109M e-commerce events on commodity hardware (16GB RAM laptop), unlocking high-value customer segments, predicting purchase probability, and identifying multi-million dollar revenue opportunities.**

---

**Live Dashboard:** [![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://customer-intelligence-platform.streamlit.app/)

## Project Overview

The Customer Intelligence Platform bridges the gap between raw behavioral data and actionable business strategy, proving that sophisticated analytics and machine learning do not require expensive cloud infrastructure. 

By employing advanced data engineering techniques like precise typification, dictionary encoding, columnar storage, and dimensional modeling, this repository processes a massive scale of e-commerce interactions natively. The project delivers an interactive application offering executive KPIs, deep dive user segmentations (RFM), LightGBM-based purchase propensity models, and statistical A/B test simulations.

## Problem Statement

**Business Challenge**: E-commerce platforms generate massive volumes of event data but struggle to answer strategic questions proactively:
- *Which users are showing churn signals?*
- *Who possesses the highest probability to purchase next month?*
- *Which products should be bundled together to maximize Average Order Value (AOV)?*

**Technical Challenge**: Processing 100M+ row event datasets typically necessitates expensive cloud data warehouses (e.g., Snowflake, BigQuery) or distributed compute clusters (e.g., Apache Spark), increasing operational overhead and slowing iteration speed.

**Solution**: This platform demonstrates how **advanced optimization and modern OLAP engines (DuckDB + Polars)** allow complex, large-scale data workflows to execute entirely on commodity hardware.

---

## System Architecture & Data Pipeline

The system is built on an OLAP-first, meticulously optimized in-memory processing architecture that guarantees extreme performance on consumer-grade hardware.

![Alt text](system_design.svg)

### 1. Ingestion & Memory Optimization Layer
- **Raw Data Ingestion**: The pipeline originates with a 12GB CSV comprising 109M distinct behavioral events recorded between Oct-Nov 2019.
- **Transformation Engine** (`summarise/optimize_dataset.py`):
    - **Polars Streaming**: Leverages lazy evaluation strategies (`pl.scan_parquet()`) to ingest and process data sequentially in bounded chunks, definitively neutralizing Out-of-Memory (OOM) risks commonly encountered on local setups.
    - **Resource Compaction**: Orchestrates deep data typification logic (`Int64` → `Int32`) coupled with stringent categorical dictionary encoding strategies (UUID strings → integer-keyed maps), reducing the operational memory footprint by an unprecedented **97%**.
    - **Storage Serialization**: Compiles localized optimized states as highly compressed Parquet blobs leveraging ZSTD level 3 compression, truncating massive 14.7GB raw file allocations to an optimal **1.9GB** footprint on-disk.

### 2. Dimensional Modeling & OLAP Layer
- **Analytical Setup** (`scripts/create_cloud_database.py`):
    - Deploys a dedicated **DuckDB** instance functioning as a highly performant, in-memory analytical OLAP processing engine configured for sub-second aggregations spanning 100M+ elements.
    - Transitions optimized Parquet representations immediately into a scalable Star Schema matrix, segregating compute logic into distributed Fact Tables (`fact_sessions`, `fact_daily_kpis`) and static Dimension subsets (`dim_users`, `dim_products`).

### 3. Feature Engineering & ML Layer
- **Feature Store Operations** (`src/processing/features.py`):
    - Synthesizes session events and dimensional properties uniformly via pure SQL processing, merging behavioral interaction metrics (e.g., checkout density, duration volatility) alongside discrete RFM flags to cultivate a rich `features_users` store.
- **Predictive Modeling Algorithms** (`src/models/train_propensity.py`):
    - Actuates an advanced **LightGBM Gradient Boosting** orchestration, explicitly trained strictly against October's behavioral datasets to infer November purchase probabilities. Operates enforcing an absolute out-of-time temporal barrier split to inherently negate target leakage logic.
    - Reaches verification states exceeding **75% ROC-AUC**, successfully partitioning top-tier users to drive a validated **4.5x predictive conversion lift**.
- **Calculated Intelligence** (`src/models/recommendations.py`):
    - Facilitates embedded Market Basket Analysis directly through DuckDB's vectorized processing algorithms, successfully surfacing mathematically stable `predictions_product_affinity` cross-selling associations.

---

## Executive Summary & Business Impact

Through programmatic analysis over 5.3M users, 15M sessions, and 206K products, the platform surfaced high-converting strategies:

| Discovery | Impact | Recommendation |
|-----------|--------|----------------|
| **High-Intent Prediction** | ML model flags users exhibiting **36% purchase probability** (4.5x baseline). | Deploy targeted personalized campaigns for the highest 5% of users → **+350% marketing efficiency**. |
| **At-Risk VIPs** | Discovered 36,000 top-decile users ($890 avg spend) displaying actionable churn signals. | Automate "Can't Lose Them" reactivation pipelines → Recovers an estimated **$32M in lifetime value**. |
| **High-Confidence Cross-Sells** | Identified 10M+ product affinity associations displaying >1.2 lift confidence. | Implement dynamically-injected "Frequently Bought Together" modules → Drives evaluated **+15% AOV**. |

### Insights Highlights:
- **The Recency Trap**: Users who browsed within 24 hours are **6x more likely** to purchase than equivalent 30-day cohorts. Real-time behavior triggers drastically outperform cumulative historical behavior.
- **World-Class Checkout Success**: Discovered industry-leading 60.6% cart-to-purchase conversion workflows, isolating the business bottleneck cleanly up-funnel to discovery workflows (10.1% view-to-cart).

---

## Project Structure

```text
customer-intelligence-platform/
├── app/                  # Streamlit application UI (7 interactive pages)
│   ├── components/       # Reusable UI components and state machines
│   └── pages/            # View logic for Data Explorer, ML Engine, etc.
├── config/               # Environment and build YAML configurations 
├── data/                 # Raw/Sample Parquet and DuckDB artifacts (Not checked in)
├── scripts/              # Data generation and localized build utilities
│   ├── create_cloud_database.py
│   └── create_sample_dataset.py
├── src/                  # Core analytics and ML pipeline modules
│   ├── analysis/         # RFM, cohort retention, and A/B statistical logic
│   ├── ingestion/        # Data loading orchestration and schema validation
│   ├── models/           # Propensity model training, inference, and recommendation
│   ├── processing/       # Sessionization and complex feature engineering 
│   └── utils/            # Helper utilities and shared context functions
├── summarise/            # Heavily-optimized ETL pipelines compressing the raw dataset
├── tests/                # Unit and integration test suites
├── requirements.txt      # Python dependencies targeting cloud deployments
└── readme.md             # Architecture documentation (This file)
```

---

## Installation & Setup

You can deploy the application using an automated sample dataset setup (ideal for rapid exploration or Cloud bounds) or optimize the full 109M event dataset locally. Note: The Kaggle _Multi-Category E-commerce Events_ dataset represents the full data source.

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/BhargavKumarNath/Customer-Intelligence-Platform.git
cd Customer-Intelligence-Platform

# Create and activate virtual environment
python -m venv .venv

# On Linux/MacOS
source .venv/bin/activate
# On Windows
.venv\Scripts\activate

# Install core dependencies
pip install -r requirements.txt
```

### 2. Data Pipeline Execution

**Option A: Running the Sample Dataset Workflow (Recommended)**
Generates a representative, stratified sample database optimized for Streamlit Cloud constraints.
```bash
# 1. Start by building the optimized sample Parquet
python scripts/create_sample_dataset.py

# 2. Compile the DuckDB dimensional models & RFM segments 
python scripts/create_cloud_database.py
```

**Option B: Full Dataset Optimization**
Ensure you have the full 12GB CSV downloaded and placed appropriately inside `/data/`.
```bash
# Execute the massive memory reduction pipeline
python summarise/optimize_dataset.py
```

### 3. Launching the Control Center Dashboard

Spin up the 7-page interactive Streamlit dashboard locally:
```bash
streamlit run app/Home.py
```

---

## Key Technologies

- **Data Engineering**: [DuckDB](https://duckdb.org/) (In-process analytical SQL OLAP), [Polars](https://pola.rs/) (Lightning-fast DataFrame library in Rust), [Apache Arrow/Parquet](https://arrow.apache.org/) (Columnar memory & storage serialization)
- **Machine Learning**: [LightGBM](https://lightgbm.readthedocs.io/) (Gradient Boosting tree framework optimized for speed), Scikit-Learn
- **Dashboard & Visualization**: [Streamlit](https://streamlit.io/), [Plotly](https://plotly.com/)
- **Architecture**: Dimensional Modeling (Star Schema), Config-driven development

---

## Limitations & Future Improvements

1. **Causal Inference**: Planning to integrate `DoWhy` and `EconML` to measure and prove true *incrementality* instead of mere correlation during simulated campaign interventions.
2. **Graph Algorithms**: Initiating migration of the monolithic recommendation engine towards `Neo4j` to leverage complex relational graph traversal (e.g., PageRank, Node2vec).
3. **Real-Time Streaming Ingestion**: Designing infrastructure integration linking `Kafka` and DuckDB for live, intra-day streaming event ingestion and real-time model scoring.

---

**If this system architecture or optimization approach helps your data engineering journey, please consider starring the repository!**

*Questions or collaborations? Open an issue or reach out via [LinkedIn](https://www.linkedin.com/in/bhargavkumarnath/).*
