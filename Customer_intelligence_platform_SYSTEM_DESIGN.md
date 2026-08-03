# 🏗️ Customer Intelligence Platform Detailed System Design
### A Complete Technical Architecture Document

> **Version:** 1.0  
> **Last Updated:** June 12, 2026  
> **Reverse Engineered From:** Source code, execution logs, and git history

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Data Architecture](#2-data-architecture)
3. [Pipeline Architecture](#3-pipeline-architecture)
4. [Storage Design](#4-storage-design)
5. [Dimensional Model (Star Schema)](#5-dimensional-model-star-schema)
6. [Machine Learning Architecture](#6-machine-learning-architecture)
7. [Recommendation Engine Design](#7-recommendation-engine-design)
8. [Analytical Module Designs](#8-analytical-module-designs)
9. [Application Layer Design](#9-application-layer-design)
10. [Deployment Architecture](#10-deployment-architecture)
11. [Performance Architecture](#11-performance-architecture)
12. [Data Flow Diagrams](#12-data-flow-diagrams)
13. [Scalability Analysis](#13-scalability-analysis)
14. [Security & Reliability](#14-security--reliability)
15. [Known Limitations & Future Improvements](#15-known-limitations--future-improvements)

---

## 1. System Overview

### 1.1 Purpose

The Customer Intelligence Platform is a **single-node OLAP analytics system** designed to process large-scale e-commerce behavioral data, derive customer intelligence, and deliver actionable ML-driven insights through an interactive dashboard — entirely on commodity hardware with zero cloud infrastructure cost.

### 1.2 System Objectives

| Objective | Target | Achieved |
|---|---|---|
| Process 109M events on 16GB RAM | In-memory footprint ≤ 6GB peak | ✅ 3.7GB baseline, 6GB peak |
| Sub-second dashboard query latency | P95 < 1 second | ✅ All pre-aggregated queries |
| ML targeting improvement | >2x conversion lift | ✅ 4.5x lift achieved |
| Zero infrastructure cost | $0/month | ✅ Streamlit Cloud free tier |
| Production-deployable | Cloud + local modes | ✅ Auto-detecting deployment |

### 1.3 High-Level Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    CUSTOMER INTELLIGENCE PLATFORM             │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────┐ │
│  │  RAW DATA    │    │  PROCESSING  │    │  INTELLIGENCE  │ │
│  │  LAYER       │───▶│  LAYER       │───▶│  LAYER         │ │
│  │              │    │              │    │                │ │
│  │ CSV 14.7GB   │    │ Polars ETL   │    │ RFM Segments   │ │
│  │ Oct+Nov 2019 │    │ DuckDB OLAP  │    │ LightGBM ML    │ │
│  │ 109.9M rows  │    │ Star Schema  │    │ Market Basket  │ │
│  └──────────────┘    └──────────────┘    └────────────────┘ │
│                                                  │           │
│                                                  ▼           │
│                             ┌─────────────────────────────┐ │
│                             │      PRESENTATION LAYER      │ │
│                             │   Streamlit Dashboard (7pg)  │ │
│                             │   Plotly Charts              │ │
│                             │   Cloud + Local Deployment   │ │
│                             └─────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

---

## 2. Data Architecture

### 2.1 Data Sources

**Primary Dataset:** Kaggle — eCommerce Events History in Cosmetics Shop  
**Origin:** Multi-category Russian e-commerce store, 2019  
**License:** Public domain / CC0

| File | Size on Disk | Format | Time Period |
|---|---|---|---|
| `2019-Oct.csv` | 5.67 GB | CSV, UTF-8 | Oct 1–31, 2019 |
| `2019-Nov.csv` | 9.01 GB | CSV, UTF-8 | Nov 1–30, 2019 |
| **Total Raw** | **14.68 GB** | — | **61 days** |

### 2.2 Raw Schema

| Column | Original Type | Example Value | Nullability |
|---|---|---|---|
| `event_time` | String (datetime) | `"2019-10-01 00:00:00 UTC"` | 0% null |
| `event_type` | String (enum) | `"view"`, `"cart"`, `"purchase"`, `"remove_from_cart"` | 0% null |
| `product_id` | Int64 | `1004856` | ~0% null |
| `category_id` | Int64 | `2053013555631882655` | ~0% null |
| `category_code` | String (nullable) | `"electronics.smartphone"` | **~35% null** |
| `brand` | String (nullable) | `"samsung"` | **~42% null** |
| `price` | Float64 | `489.07` | <1% zero/null |
| `user_id` | Int64 | `512428568` | 0% null |
| `user_session` | String (UUID) | `"3bfb6827-b..."` | <1% null |

### 2.3 Data Volume Statistics

| Metric | Value | Source Evidence |
|---|---|---|
| Total events | **109,950,743** | `outputs/2026-01-17/14-02-13/loader.log` line 4 |
| Unique users | ~3M (est.) | `dim_users` row count |
| Unique products | ~100K | `dim_products` row count |
| Unique sessions | ~15M | `fact_sessions` row count |
| Unique brands | ~thousands | `dim_products.brand` |
| Unique categories | hundreds | `dim_products.category_code` |

### 2.4 Event Type Distribution

| Event Type | Approximate % | Business Meaning |
|---|---|---|
| `view` | ~68% | User viewed a product page |
| `cart` | ~15% | User added to cart |
| `remove_from_cart` | ~9% | User removed from cart (cart abandonment signal) |
| `purchase` | ~8% | User completed a purchase |

### 2.5 Data Quality Issues

| Issue | Severity | Handling Strategy |
|---|---|---|
| 35% null `category_code` | HIGH | `COALESCE(category_code, 'unknown')` |
| 42% null `brand` | HIGH | `COALESCE(brand, 'unknown')` |
| UUID session strings | PERFORMANCE | Cast to `pl.Categorical` |
| Zero-price events | LOW | Filter `WHERE price > 0` for price analysis |
| Duplicate user-session mappings | LOW | `MAX(user_id)` in sessionization |

---

## 3. Pipeline Architecture

### 3.1 Pipeline Overview

The system is composed of **8 sequential batch pipeline stages** plus **1 online serving layer**:

```
Stage 0: Data Acquisition (manual)
    ↓
Stage 1: CSV → Parquet Optimization (summarise/optimize_dataset.py)
    ↓
Stage 2: Parquet → DuckDB Ingestion (src/ingestion/loader.py)
    ↓
Stage 3: Dimensional Modeling — Star Schema (src/processing/initial_modeling.py)
    ↓
Stage 4: Sessionization — Events → Sessions (src/processing/sessionization.py)
    ↓
Stage 5: RFM Segmentation (src/analysis/segmentation.py)
    ↓
Stage 6: Feature Engineering — Golden Table (src/processing/features.py)
    ↓
Stage 7A: Propensity Model Training (src/models/train_propensity.py)
Stage 7B: Recommendation Engine (src/models/recommendations.py)
Stage 7C: Retention/Cohort Analysis (src/analysis/retention.py)
Stage 7D: A/B Testing Engine (src/analysis/ab_testing.py)
    ↓
Stage 8: Dashboard Serving (app/Home.py + pages/)
```

### 3.2 Stage 1: Data Optimization (Polars ETL)

**File:** `summarise/optimize_dataset.py`  
**Pattern:** Lazy evaluation with streaming sink

```python
# Memory-efficient pattern: lazy read → transform → streaming write
df = pl.scan_parquet(input_path)        # LazyFrame — nothing loaded yet

df_optimized = df.select([
    pl.col("event_time").str.to_datetime("%Y-%m-%d %H:%M:%S UTC"),  # Parse once
    pl.col("event_type").cast(pl.Categorical),                       # 87% savings
    pl.col("product_id").cast(pl.Int32),                             # 50% savings
    pl.col("category_code").cast(pl.Categorical),                    # 90% savings
    pl.col("brand").cast(pl.Categorical),                            # 90% savings
    pl.col("price").cast(pl.Float32),                                # 50% savings
    pl.col("user_id").cast(pl.Int32),                                # 50% savings
    pl.col("user_session").cast(pl.Categorical),                     # 95% savings (UUID)
])

# Streaming write — never loads entire dataset into memory
df_optimized.sink_parquet(
    output_path,
    compression="zstd",      # ZSTD beats Snappy by 33% on size
    compression_level=3,     # Sweet spot: speed vs compression
    statistics=True,         # Enables predicate pushdown for DuckDB
    row_group_size=500_000,  # Large row groups = better analytical scan performance
)
```

**Memory budget during this stage:**  
- Input: Lazy frame, zero memory until `.collect()` or `.sink_*()`
- Streaming sink: Processes row groups sequentially, never loads full dataset

### 3.3 Stage 2: DuckDB Ingestion

**File:** `src/ingestion/loader.py`  
**Execution time:** ~3.5 minutes (logged: 14:02:13 → 14:05:33)

```sql
-- Sorted ingestion for optimal time-range scan performance
CREATE OR REPLACE TABLE events AS 
SELECT * FROM read_parquet('{raw_path}')
ORDER BY event_time;
```

**Configuration:**
```sql
SET memory_limit='8GB';  -- Safe limit on 16GB system
SET threads TO 4;         -- Parallelism for ingestion
```

**Hardware tuning rationale:**
- 8GB limit leaves 8GB for OS + Python process overhead
- 4 threads saturates I/O without context-switching overhead

### 3.4 Stage 3: Dimensional Modeling

**File:** `src/processing/initial_modeling.py`  
**Configuration:** `memory_limit=10GB`, `threads=3`, `preserve_insertion_order=false`

Three tables created:

**`dim_products`** (using DISTINCT ON — memory-optimized vs window function):
```sql
CREATE OR REPLACE TABLE dim_products AS 
SELECT DISTINCT ON (product_id)
    product_id,
    category_id,
    COALESCE(category_code, 'unknown') as category_code,
    COALESCE(brand, 'unknown') as brand,
    price as current_price
FROM events
ORDER BY product_id, event_time DESC;  -- Gets latest price per product
```

**Design decision:** `DISTINCT ON` instead of `ROW_NUMBER() OVER (PARTITION BY product_id)` avoids materializing the full window function result for 109M rows (~2GB overhead).

**`fact_daily_kpis`** (61 rows — ultra-compact KPI store):
```sql
CREATE OR REPLACE TABLE fact_daily_kpis AS 
SELECT 
    CAST(event_time AS DATE) as date,
    COUNT(*) as total_events,
    COUNT(DISTINCT user_id) as dau,
    COUNT(DISTINCT user_session) as daily_sessions,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as daily_revenue,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
    SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as total_carts,
    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as total_views
FROM events
GROUP BY 1
ORDER BY 1;
```

**`dim_users`** (~3M rows — user-level lifetime aggregates):
```sql
CREATE OR REPLACE TABLE dim_users AS 
SELECT 
    user_id,
    MIN(event_time) as first_seen,
    MAX(event_time) as last_seen,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_session) as session_count,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as total_spend,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
    BOOL_OR(event_type = 'purchase') as is_buyer,
    MAX(CASE WHEN event_type = 'view' THEN category_code END) as favorite_category_by_recency
FROM events
GROUP BY user_id;
```

**Execution time:** ~60 seconds for `dim_users` (heaviest operation — aggregates 109M rows by UUID)

### 3.5 Stage 4: Sessionization

**File:** `src/processing/sessionization.py`  
**Output:** `fact_sessions` — 15M rows

```sql
CREATE OR REPLACE TABLE fact_sessions AS
SELECT 
    user_session,
    MAX(user_id) as user_id,           -- Sessions can span multiple users (edge case)
    MIN(event_time) as session_start,
    MAX(event_time) as session_end,
    date_diff('second', MIN(event_time), MAX(event_time)) as duration_sec,
    COUNT(*) as event_count,
    
    -- Funnel flags (binary) — enables fast funnel analysis
    BOOL_OR(event_type = 'view') as has_view,
    BOOL_OR(event_type = 'cart') as has_cart,
    BOOL_OR(event_type = 'remove_from_cart') as has_remove,
    BOOL_OR(event_type = 'purchase') as has_purchase,
    
    -- Financials
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as session_revenue,
    
    -- Content affinity
    mode(category_code) as top_category  -- Most frequent category in session
    
FROM events
WHERE user_session IS NOT NULL
GROUP BY user_session;
```

**Funnel derived from sessions:**
```sql
-- Conversion rates computed post-sessionization
ROUND(SUM(has_cart::INT) / SUM(has_view::INT), 4) as view_to_cart_rate,
ROUND(SUM(has_purchase::INT) / SUM(has_cart::INT), 4) as cart_to_purchase_rate,
ROUND(SUM(has_purchase::INT) / COUNT(*), 4) as overall_conversion
```

### 3.6 Stage 5: RFM Segmentation

**File:** `src/analysis/segmentation.py`  
**Input:** `events` table, buyers only (`WHERE event_type = 'purchase'`)  
**Output:** `analysis_rfm_segments` (~700K buyers)

**Algorithm:**
1. Calculate raw RFM metrics per buyer (TEMP table)
2. NTILE(5) window functions for quantile scoring
3. Rule-based segment labeling

```sql
-- Scoring with inverted recency (lower days = better score)
6 - NTILE(5) OVER (ORDER BY recency_days) as r_score,  -- Inversion trick
NTILE(5) OVER (ORDER BY frequency_count) as f_score,
NTILE(5) OVER (ORDER BY monetary_value) as m_score

-- Segmentation rules
CASE 
    WHEN (r_score >= 4 AND f_score >= 4) THEN 'Champions'
    WHEN (r_score >= 3 AND f_score >= 3) THEN 'Loyal Customers'
    WHEN (r_score >= 4 AND f_score = 1) THEN 'New Customers'
    WHEN (r_score >= 3 AND f_score = 1) THEN 'Promising'
    WHEN (r_score = 2 AND f_score >= 2) THEN 'Need Attention'
    WHEN (r_score = 1 AND f_score >= 4) THEN 'Cant Lose Them'
    WHEN (r_score = 1 AND f_score <= 2) THEN 'Hibernating'
    ELSE 'At Risk'
END as segment_name
```

**Design note:** TEMP table for `rfm_base` avoids re-scanning 109M events in the scoring query — the two-step approach trades a single scan + small intermediate table.

### 3.7 Stage 6: Feature Engineering (Golden Table)

**File:** `src/processing/features.py`  
**Output:** `features_users` — the ML training table

**Multi-source join:**
```sql
CREATE OR REPLACE TABLE features_users AS
SELECT 
    u.user_id,
    
    -- From dim_users (lifetime profile)
    u.total_spend, u.purchase_count, u.event_count, u.first_seen, u.last_seen,
    
    -- From RFM (buyers only, COALESCE for non-buyers)
    COALESCE(r.recency_days, -1) as recency_days,
    COALESCE(r.frequency_count, 0) as frequency_raw,
    COALESCE(r.monetary_value, 0) as monetary_raw,
    COALESCE(r.segment_name, 'Browser') as rfm_segment,
    COALESCE(r.rfm_code, '000') as rfm_code,
    
    -- From session features (behavioral signals)
    COALESCE(s.total_sessions, 0) as total_sessions,
    COALESCE(s.avg_session_duration, 0) as avg_session_duration,
    COALESCE(s.std_session_duration, 0) as std_session_duration,
    COALESCE(s.avg_events_per_session, 0) as avg_events_per_session,
    COALESCE(s.cart_rate, 0) as cart_rate,           -- cart sessions / total sessions
    COALESCE(s.checkout_rate, 0) as checkout_rate     -- purchase sessions / cart sessions
    
FROM dim_users u
LEFT JOIN analysis_rfm_segments r ON u.user_id = r.user_id
LEFT JOIN session_features s ON u.user_id = s.user_id;
```

**Design choice — LEFT JOINs:** Non-buyers have no RFM record. COALESCE fills `'Browser'` as their default segment, allowing the ML model to see the full user population.

---

## 4. Storage Design

### 4.1 Storage Hierarchy

```
data/
├── raw/                           # Source data (gitignored)
│   ├── 2019-Oct.csv               # 5.67 GB
│   ├── 2019-Nov.csv               # 9.01 GB
│   └── ecommerce_optimized.parquet  # 1.87 GB (Polars-optimized)
│
├── db/                            # Full analytical database (gitignored)
│   └── behavior.duckdb            # 5.44 GB (full star schema + ML tables)
│
├── processed/                     # ETL outputs (empty, future use)
│
└── sample/                        # Cloud deployment subset (committed to git)
    ├── sample_optimized.parquet   # 37.4 MB (3% sample, Polars-optimized)
    └── sample.duckdb              # 104 MB (3% sample star schema)

analysis_subsets/                  # Pre-aggregated Polars outputs
├── product_summary.parquet        # 2.77 MB (206,876 products)
├── user_summary.parquet           # 38.6 MB (5,316,649 users)
└── daily_summary.parquet          # 3.6 KB (61 days × 4 event types)

src/models/
└── propensity_lgbm.pkl            # 3.48 MB (trained LightGBM model)
```

### 4.2 Format Comparison

| Format | Size | Read Speed | Write Speed | Query Pattern |
|---|---|---|---|---|
| Raw CSV | 14.7 GB | ~180s | N/A | Sequential only |
| Parquet (Snappy) | 4.8 GB | ~8s | ~2.5 min | Columnar, predicate pushdown |
| Parquet (ZSTD L3) | 3.2 GB | ~6s | ~3.8 min | ✅ Best read/size tradeoff |
| Parquet (ZSTD L10) | 2.9 GB | ~6s | ~12.5 min | Over-compressed, slow write |
| DuckDB (full) | 5.44 GB | <1s (queries) | ~15 min total | ✅ OLAP analytical queries |

### 4.3 DuckDB Internal Organization

DuckDB uses a columnar file format internally. The `behavior.duckdb` file contains:
- `events` table: 109.9M rows, sorted by `event_time`
- `dim_users`: ~3M rows
- `dim_products`: ~100K rows
- `fact_sessions`: ~15M rows
- `fact_daily_kpis`: 61 rows
- `analysis_rfm_segments`: ~700K rows
- `features_users`: ~3M rows
- `predictions_product_affinity`: millions of association rules

---

## 5. Dimensional Model (Star Schema)

### 5.1 Entity-Relationship Diagram

```
                    ┌─────────────────┐
                    │  fact_events    │
                    │  (109.9M rows)  │
                    │                 │
                    │ event_time      │
                    │ event_type      │──── Central fact table
                    │ product_id ─────┼──────────────────┐
                    │ category_id     │                   │
                    │ category_code   │                   │
                    │ brand           │                   ▼
                    │ price           │        ┌──────────────────┐
                    │ user_id ────────┼──┐     │  dim_products    │
                    │ user_session ───┼──┼─┐   │  (~100K rows)    │
                    └─────────────────┘  │ │   │                  │
                                         │ │   │ product_id (PK)  │
                    ┌────────────────┐   │ │   │ category_id      │
                    │  dim_users     │◄──┘ │   │ category_code    │
                    │  (~3M rows)    │     │   │ brand            │
                    │                │     │   │ current_price    │
                    │ user_id (PK)   │     │   └──────────────────┘
                    │ first_seen     │     │
                    │ last_seen      │     │   ┌──────────────────┐
                    │ event_count    │     │   │  fact_sessions   │
                    │ session_count  │     └──▶│  (~15M rows)     │
                    │ total_spend    │         │                  │
                    │ purchase_count │         │ user_session(PK) │
                    │ is_buyer       │         │ user_id (FK)     │
                    └────────────────┘         │ session_start    │
                                               │ session_end      │
                    ┌────────────────┐         │ duration_sec     │
                    │ fact_daily_kpis│         │ event_count      │
                    │  (61 rows)     │         │ has_view (bool)  │
                    │                │         │ has_cart (bool)  │
                    │ date (PK)      │         │ has_purchase     │
                    │ dau            │         │ session_revenue  │
                    │ daily_revenue  │         └──────────────────┘
                    │ total_purchases│
                    │ total_carts    │
                    │ total_views    │
                    │ total_events   │
                    └────────────────┘
```

### 5.2 Derived/ML Tables

```
┌───────────────────────────┐     ┌──────────────────────────────┐
│  analysis_rfm_segments    │     │  features_users              │
│  (~700K buyers)           │     │  (~3M all users)             │
│                           │     │                              │
│  user_id (FK→dim_users)   │     │  user_id (FK→dim_users)      │
│  recency_days             │     │  [All dim_users fields]      │
│  frequency_count          │     │  [RFM fields + COALESCE]     │
│  monetary_value           │     │  [Session behavioral signals]│
│  r_score (1-5)            │     │  → The "Golden Table" for ML │
│  f_score (1-5)            │     └──────────────────────────────┘
│  m_score (1-5)            │
│  rfm_code ('555' format)  │
│  segment_name             │     ┌──────────────────────────────┐
└───────────────────────────┘     │  predictions_product_affinity│
                                  │  (millions of rules)         │
┌──────────────────────────┐      │                              │
│  analysis_weekly_retention│     │  product_a (FK→dim_products) │
│  (cohort × week matrix)   │     │  product_b (FK→dim_products) │
│                           │     │  pair_count                  │
│  cohort_week              │     │  confidence (P(B|A))         │
│  cohort_size              │     │  lift (corr vs random)       │
│  weeks_since_first        │     └──────────────────────────────┘
│  active_users             │
│  retention_rate           │
└──────────────────────────┘
```

---

## 6. Machine Learning Architecture

### 6.1 Problem Formulation

| Attribute | Details |
|---|---|
| Problem type | Binary classification (propensity scoring) |
| Target | `converted_in_nov` — will user purchase in November? |
| Positive class | Purchase in November 2019 |
| Negative class | No purchase in November 2019 |
| Class imbalance | ~8% positive (severe imbalance) |

### 6.2 Temporal Train/Test Split

```
Timeline:
Oct 2019: ──────────────────────────┐
                                    │ Features (X)
                                    ↓
Nov 2019:                     ┌─────────────────
                               │ Target (y): Purchase? Yes/No
                               └─────────────────

Split: 80% train / 20% test (stratified on y to preserve 8% positive rate)
```

**Why temporal split?**  
Random split would allow the model to learn from November data in training, then test on October data — the reverse of real deployment. Temporal split enforces the causal direction: past behavior predicts future purchases.

### 6.3 Feature Engineering

| Feature | Source | Business Meaning |
|---|---|---|
| `oct_events` | COUNT(*) WHERE < 2019-11-01 | Total engagement volume |
| `oct_sessions` | COUNT(DISTINCT user_session) | Session breadth |
| `oct_views` | SUM(event_type='view') | Browsing intensity |
| `oct_carts` | SUM(event_type='cart') | Purchase intent signals |
| `oct_removes` | SUM(event_type='remove_from_cart') | Cart hesitation |
| `active_span_days` | date_diff(MIN, MAX event_time) | User tenure depth |
| `recency_oct` | date_diff(last_event, 2019-11-01) | Recent vs dormant |

### 6.4 LightGBM Configuration

```python
params = {
    'objective': 'binary',        # Binary cross-entropy loss
    'metric': 'auc',              # AUC-ROC for imbalanced evaluation
    'boosting_type': 'gbdt',      # Gradient Boosted Decision Trees
    'num_leaves': 31,             # Controls tree complexity (default)
    'learning_rate': 0.05,        # Conservative: prevents overfit
    'feature_fraction': 0.9,      # 90% feature sampling per tree
    'device': 'gpu',              # GPU acceleration for speed
}

# Training with validation-based early stopping
model = lgb.train(
    params,
    train_data,
    valid_sets=[test_data],
    num_boost_round=1000,         # Max rounds (early stopping typically fires < 200)
    callbacks=[
        lgb.early_stopping(stopping_rounds=50),  # Stop if no AUC improvement for 50 rounds
        lgb.log_evaluation(100)                  # Log every 100 rounds
    ]
)
```

### 6.5 Prediction Threshold Strategy

Standard binary classification uses 0.5 threshold. This model uses a **percentile-based threshold**:

```python
# Top 5% of users by predicted probability
threshold = np.percentile(y_pred_prob, 95)
y_pred_binary = (y_pred_prob >= threshold).astype(int)
```

**Business rationale:** Marketing budgets are limited. We don't want to target everyone with P(purchase) > 50%. We want to identify the highest-propensity users — the top 5% — for maximum ROI on campaign spend.

**Result:** 
- Random targeting: 8.03% conversion
- Top-5% segment: 36.62% conversion  
- **Lift: 4.56x**

### 6.6 Feature Importance (by Gain)

| Rank | Feature | Gain Score | Interpretation |
|---|---|---|---|
| 1 | `oct_events` | 631,380 | Total activity is the strongest signal |
| 2 | `active_span_days` | 436,977 | Users with longer tenure are more likely buyers |
| 3 | `oct_carts` | 411,642 | Cart additions are strong intent signals |
| 4 | `oct_views` | 222,948 | Browsing volume matters but less than intent |
| 5 | `recency_oct` | 155,719 | Recently active users convert more |

Source: `app/pages/6_ML_Engine.py` lines 143-147 (hardcoded from training log output)

### 6.7 Model Serialization

```python
# Saved as pickle
model_path = "src/models/propensity_lgbm.pkl"
with open(model_path, 'wb') as f:
    pickle.dump(model, f)
```

**File size:** 3.48 MB (from `src/models/propensity_lgbm.pkl`)  
**Note:** The dashboard does not load this pkl file for inference — feature importance is hardcoded. Future improvement: load model dynamically to reflect retraining.

---

## 7. Recommendation Engine Design

### 7.1 Algorithm: Association Rules Mining

**Method:** Session-level co-occurrence → Apriori-style rules  
**Scale:** 10M+ product pairs in 90 seconds  

### 7.2 Three-Step Pipeline

**Step 1: Build Baskets (purchase events only)**
```sql
CREATE OR REPLACE TEMP TABLE baskets AS
SELECT user_session, product_id
FROM events
WHERE event_type = 'purchase'
```

**Step 2: Compute Product Pairs (Self-Join)**
```sql
CREATE OR REPLACE TEMP TABLE product_pairs AS
SELECT 
    a.product_id as product_a,
    b.product_id as product_b,
    COUNT(*) as pair_count
FROM baskets a
JOIN baskets b ON a.user_session = b.user_session
WHERE a.product_id != b.product_id          -- Exclude same-product pairs
GROUP BY 1, 2
HAVING COUNT(*) >= 5;                        -- Min support threshold (removes noise)
```

**Step 3: Calculate Lift & Confidence**
```sql
-- Lift(A→B) = P(A and B) / (P(A) × P(B))
--           = (pair_count / N) / ((cnt_A / N) × (cnt_B / N))
--           = (pair_count × N) / (cnt_A × cnt_B)

(p.pair_count * total_sessions) / (pa.cnt * pb.cnt) as lift

-- Filter: Lift > 1.2 means A and B co-occur 20% more than random
WHERE (p.pair_count * total_sessions) / (pa.cnt * pb.cnt) > 1.2
```

### 7.3 Metrics Defined

| Metric | Formula | Interpretation |
|---|---|---|
| **Support** | pair_count / total_sessions | How often do A and B appear together? |
| **Confidence** | pair_count / count_A | Given a user bought A, what's P(buy B)? |
| **Lift** | (pair_count × N) / (cnt_A × cnt_B) | How much more likely is B given A vs random? |

**Thresholds:**
- Minimum support: 5 co-occurrences (noise filter)
- Minimum lift: 1.2 (positive association)

### 7.4 Query Complexity Analysis

Self-join on purchase events: O(|P|²) where |P| = number of purchase events per session.  

**Worst case:** Sessions with many purchases → many pairs. The HAVING COUNT(*) >= 5 filter dramatically prunes the result set from quadratic to manageable.

**Practical performance:** "10M+ product pairs in 90 seconds" — documented in `app/pages/0_Project_Overview.py` line 257.

---

## 8. Analytical Module Designs

### 8.1 RFM Analysis Design

**Input:** Buyer purchase history  
**Output:** 8 labeled segments with actionable profiles

**NTILE scoring:**
- 5 quantile buckets (score 1-5) for each of R, F, M
- Recency inverted: low days since purchase = high score (score 5 = most recent)
- Concatenated RFM code: `'555'` = Champion, `'111'` = Hibernating

**Segment Playbook:**

| Segment | Profile | Action |
|---|---|---|
| Champions | R≥4, F≥4 | Cross-sell via recommendations |
| Loyal Customers | R≥3, F≥3 | Loyalty program, early access |
| New Customers | R≥4, F=1 | Onboarding campaign |
| Promising | R≥3, F=1 | Nurturing sequence |
| Need Attention | R=2, F≥2 | Win-back offer |
| Can't Lose Them | R=1, F≥4 | High-priority reactivation (avg spend >$900) |
| Hibernating | R=1, F≤2 | Low-investment nudge or sunset |
| At Risk | Else | Targeted discount |

### 8.2 Cohort Retention Analysis Design

**Input:** `dim_users` (first_seen) + `events` (activity)  
**Output:** `analysis_weekly_retention` matrix

```sql
WITH user_activity AS (
    SELECT 
        u.user_id,
        date_trunc('week', u.first_seen) as cohort_week,  -- When did they join?
        date_trunc('week', e.event_time) as activity_week  -- When were they active?
    FROM dim_users u
    JOIN events e ON u.user_id = e.user_id
),
cohort_sizes AS (
    SELECT date_trunc('week', first_seen) as cohort_week, COUNT(DISTINCT user_id) as cohort_size
    FROM dim_users GROUP BY 1
)
SELECT 
    ua.cohort_week,
    cs.cohort_size,
    date_diff('week', ua.cohort_week, ua.activity_week) as weeks_since_first,
    COUNT(DISTINCT ua.user_id) as active_users,
    CAST(COUNT(DISTINCT ua.user_id) AS DOUBLE) / cs.cohort_size as retention_rate
FROM user_activity ua
JOIN cohort_sizes cs ON ua.cohort_week = cs.cohort_week
GROUP BY 1, 2, 3;
```

**Key finding documented:** 65% Week-1 drop-off across all cohorts (Discovery Problem).

### 8.3 Churn Risk Analysis Design

**Rule-based churn scoring on dataset timeframe:**
```sql
CASE 
    WHEN days_inactive > 14 THEN 'Churned'
    WHEN days_inactive > 7  THEN 'At Risk'
    ELSE                         'Active'
END as status
```

**Note:** Thresholds calibrated to 61-day dataset window (Oct-Nov 2019). For a longer dataset, thresholds would be 30/90 days.

### 8.4 A/B Testing Engine Design

**Class:** `ABTestEngine` in `src/analysis/ab_testing.py`

**Statistical framework:**
1. **Simulation:** `generate_synthetic_experiment()` — Bernoulli draws for control/treatment
2. **Test:** Welch's two-sided t-test (`equal_var=False` — robust to unequal variances)
3. **Confidence interval:** Delta method approximation for proportion difference
4. **Power analysis:** `TTestIndPower` (statsmodels) for post-hoc power calculation

```python
# Confidence interval calculation
se_a = sqrt(p_control × (1-p_control) / n_control)
se_b = sqrt(p_treatment × (1-p_treatment) / n_treatment)
se_diff = sqrt(se_a² + se_b²)
z_score = norm.ppf(1 - alpha/2)  # 1.96 for 95% confidence
ci = (diff ± z_score × se_diff)
```

**Decision framework (Amazon-inspired "Bar Raiser"):**
- ✅ p-value < 0.05 (statistical significance)
- ✅ Lift > 10% (business significance threshold)
- ✅ Scalable to full population (operational feasibility)

---

## 9. Application Layer Design

### 9.1 Architecture

```
app/
├── Home.py                      # Entry point, hero metrics, navigation
├── db_utils.py                  # Database connection layer (shared by all pages)
├── components/
│   ├── __init__.py
│   ├── code_viewer.py           # Source code display widget
│   └── glossary.py              # Contextual definitions (expandable)
└── pages/
    ├── 0_Project_Overview.py    # Architecture, tech stack, achievements
    ├── 1_Data_Explorer.py       # Dataset exploration, distributions
    ├── 2_Optimization_Engine.py # All optimization techniques
    ├── 3_Executive_Overview.py  # KPI dashboard
    ├── 4_User_Intelligence.py   # RFM, cohort, churn
    ├── 5_Experiment_Lab.py      # Interactive A/B simulator
    └── 6_ML_Engine.py           # Propensity model, recommendations
```

### 9.2 Database Connection Layer (`db_utils.py`)

**Environment detection:**
```python
IS_CLOUD = os.getenv('STREAMLIT_SHARING', 'false').lower() == 'true'
SAMPLE_MODE = not os.path.exists("data/db/behavior.duckdb")
```

**Connection strategy:**

| Environment | Connection | Database | Memory |
|---|---|---|---|
| Local (full DB) | `duckdb.connect(DB_PATH, read_only=True)` | 5.44 GB DuckDB | 8 GB limit |
| Cloud/Sample | `duckdb.connect(':memory:')` + `read_parquet()` | In-memory from parquet | 512 MB |

**Connection caching:**
```python
@st.cache_resource  # Cached across all user sessions — singleton pattern
def get_connection():
    ...
```

### 9.3 Cloud Database Bootstrap

When no full DuckDB exists, `get_connection()` builds the entire star schema in memory:
1. `CREATE TABLE events AS SELECT * FROM read_parquet(sample.parquet)`
2. `CREATE TABLE dim_products ...`
3. `CREATE TABLE dim_users ...`
4. `CREATE TABLE fact_daily_kpis ...`
5. `CREATE TABLE fact_sessions ...`
6. `CREATE TABLE user_rfm_segments ...`
7. `CREATE TABLE predictions_product_affinity ...` (market basket, self-join)

This runs on every cold start in cloud mode (~30-60 seconds first load).

### 9.4 Page Architecture Pattern

All pages follow the same pattern:
```python
import streamlit as st
from db_utils import run_query  # Shared connection

st.set_page_config(...)
# Query → DataFrame → Plotly Chart → st.plotly_chart()
```

---

## 10. Deployment Architecture

### 10.1 Two-Mode Deployment

```
                    ┌─────────────────────────────────────────┐
                    │             DEPLOYMENT MODES             │
                    └─────────────────────────────────────────┘
                          │                        │
              ┌───────────┴──────────┐  ┌──────────┴─────────────┐
              │     LOCAL MODE       │  │      CLOUD MODE         │
              │  (Full Experience)   │  │   (Demo/Portfolio)      │
              └──────────────────────┘  └─────────────────────────┘
                          │                        │
              • 16GB RAM machine        • Streamlit Cloud (free)
              • Full DuckDB (5.44GB)    • 1GB RAM, ~512MB usable
              • 109M events             • 3% sample (~3M events)
              • All analytics           • All features functional
              • Sub-second queries      • Sub-second queries
              • LightGBM pkl loaded     • Market basket from sample
```

### 10.2 Cloud Deployment Process

```bash
# 1. Generate sample (3% of full dataset)
python scripts/create_sample_dataset.py

# 2. Build cloud database from sample
python scripts/create_cloud_database.py

# 3. Commit sample data (≤100MB each file)
git add data/sample/
git push origin main

# 4. Streamlit Cloud auto-deploys on push
# Main file: app/Home.py
# Python version: 3.10
```

### 10.3 DevContainer Configuration

```json
{
    "image": "mcr.microsoft.com/devcontainers/python:1-3.11-bookworm",
    "updateContentCommand": "pip3 install --user -r requirements.txt",
    "postAttachCommand": {
        "server": "streamlit run app/Home.py --server.enableCORS false"
    },
    "forwardPorts": [8501]
}
```

GitHub Codespaces auto-starts the Streamlit app on container attach.

### 10.4 Configuration Management (Hydra)

**Local pipeline runs** use Hydra for configuration injection:
```yaml
# config/config.yaml
database:
  main_table: events
  memory_limit: 8GB
  read_only: false
paths:
  database: C:\Project\...\behavior.duckdb
  raw_data: C:\Project\...\ecommerce_optimized.parquet
```

```yaml
# config/config.cloud.yaml
database:
  memory_limit: 512MB
  read_only: true
paths:
  database: data/sample/sample.duckdb
environment: cloud
sample_mode: true
sample_percentage: 3.0
```

Hydra automatically creates timestamped output directories (`outputs/YYYY-MM-DD/HH-MM-SS/`) with run configs and logs — enabling reproducibility.

---

## 11. Performance Architecture

### 11.1 Query Performance Profile

| Operation | Input | Output | Time | Technique |
|---|---|---|---|---|
| Parquet optimization | 14.7 GB CSV | 1.9 GB Parquet | ~15 min | Polars streaming sink |
| DuckDB ingestion | 1.9 GB Parquet | 5.44 GB DuckDB | ~3.5 min | Sorted ORDER BY event_time |
| dim_products creation | 109M events | 100K products | 45s | DISTINCT ON (streaming) |
| fact_daily_kpis creation | 109M events | 61 rows | 12s | GROUP BY date |
| dim_users creation | 109M events | 3M users | 60s | GROUP BY user_id |
| fact_sessions creation | 109M events | 15M sessions | 85s | GROUP BY session UUID |
| RFM segmentation | 700K buyers | 700K segments | 18s | NTILE window |
| Market basket | 109M events | 10M+ rules | 90s | Self-join + HAVING filter |
| Dashboard query | Pre-aggregated | <1K rows | <1s | Dimensional model |

### 11.2 Memory Optimization Waterfall

```
Naive Pandas approach:
  Raw columns (Int64/Float64/String): ~120 GB
  ████████████████████████████████████████████████████████

After type optimization (Polars):
  Int64 → Int32:             50% reduction per column
  Float64 → Float32:         50% reduction per column
  String UUID → Categorical: 95% reduction (UUID column)
  String enums → Categorical: 87% reduction (event_type)
  ████████████████████
  ~15 GB estimated

After ZSTD compression:
  Disk: 12 GB CSV → 3.2 GB Parquet (73% disk reduction)
  Memory (DuckDB columnar): ~3.7 GB baseline footprint
  ████
  ~3.7 GB in memory

Peak processing (DuckDB joins):
  ~6 GB peak (during sessionization GROUP BY UUID)
  ██████
  ~6 GB peak — within 16 GB budget with 10 GB headroom
```

### 11.3 Critical Configuration: Why These Numbers

```sql
-- Ingestion: 10GB for stable processing with large Parquet read
SET memory_limit='10GB';    -- Leaves 6GB for OS + Python
SET threads TO 4;            -- Saturates I/O without thrashing

-- Sessionization (heaviest GROUP BY):
SET memory_limit='10GB';    -- UUID cardinality requires more memory
SET threads TO 4;

-- Dimensional modeling:
SET memory_limit='10GB';
SET threads TO 3;            -- Slightly conservative for stability
SET preserve_insertion_order=false;  -- Allows execution optimizer to reorder output
```

### 11.4 Query Optimization Techniques

1. **Pre-aggregation via star schema** — Every dashboard query hits `dim_users` (3M) or `fact_daily_kpis` (61 rows), not raw `events` (109M)

2. **DISTINCT ON vs ROW_NUMBER()** — Streaming deduplication avoids full materialization:
   ```sql
   -- Memory-efficient: DISTINCT ON (streaming)
   SELECT DISTINCT ON (product_id) ...  ORDER BY product_id, event_time DESC
   
   -- Memory-intensive: Window function (materializes all 109M rows)
   SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ...) FROM events WHERE rn=1
   ```

3. **TEMP TABLE two-step aggregation** — Intermediate results cached, avoiding redundant 109M row scans:
   ```sql
   -- Step 1: Aggregate buyers (scan events once)
   CREATE TEMP TABLE rfm_base AS SELECT ... FROM events WHERE event_type='purchase' GROUP BY user_id;
   -- Step 2: NTILE scoring (small table, fast)
   CREATE TABLE analysis_rfm_segments AS SELECT *, NTILE(5) OVER ... FROM rfm_base;
   ```

4. **Polars lazy evaluation** — `pl.scan_parquet()` enables predicate pushdown and column pruning before data is loaded

5. **Parquet statistics** — `statistics=True` in `sink_parquet()` enables DuckDB to skip row groups during predicate pushdown

6. **`@st.cache_resource`** — Database connection singleton prevents reconnection on every Streamlit page navigation

---

## 12. Data Flow Diagrams

### 12.1 Complete End-to-End Data Flow

```
INGESTION FLOW
══════════════════════════════════════════════════════════════

Kaggle Dataset (CSV)
    │
    │ Manual download
    ▼
data/raw/2019-Oct.csv (5.67 GB)
data/raw/2019-Nov.csv (9.01 GB)
    │
    │ summarise/combine_csv_to_parquet.py
    ▼
data/raw/2019-Oct-Nov.parquet (1.9 GB)
    │
    │ summarise/optimize_dataset.py
    │ • Type casting (Int32, Float32, Categorical)
    │ • ZSTD level-3 compression
    │ • 500K row groups for scan performance
    ▼
data/raw/ecommerce_optimized.parquet (1.87 GB)
    │
    │ src/ingestion/loader.py (Hydra config)
    │ • DuckDB persistent write
    │ • Sorted by event_time
    │ • Memory: 8GB limit, 4 threads
    ▼
data/db/behavior.duckdb::events (109.9M rows, sorted)

PROCESSING FLOW
══════════════════════════════════════════════════════════════

behavior.duckdb::events
    │
    ├──▶ src/processing/initial_modeling.py
    │    ├──▶ dim_products (100K rows, ~45s)
    │    ├──▶ fact_daily_kpis (61 rows, ~12s)
    │    └──▶ dim_users (3M rows, ~60s)
    │
    ├──▶ src/processing/sessionization.py
    │    └──▶ fact_sessions (15M rows, ~85s) + funnel metrics
    │
    ├──▶ src/analysis/segmentation.py
    │    └──▶ analysis_rfm_segments (700K buyers, ~18s)
    │
    └──▶ src/processing/features.py
         └──▶ features_users (3M rows — the Golden Table)

ML FLOW
══════════════════════════════════════════════════════════════

features_users + events
    │
    ├──▶ src/models/train_propensity.py
    │    │ • Temporal split: Oct features → Nov target
    │    │ • LightGBM GBDT, GPU, early stopping
    │    ├──▶ src/models/propensity_lgbm.pkl (3.48 MB)
    │    └──▶ [Console output: AUC, precision, feature importance]
    │
    └──▶ src/models/recommendations.py
         │ • Self-join on purchase events
         │ • Lift/Confidence calculation
         └──▶ predictions_product_affinity

ANALYTICS FLOW
══════════════════════════════════════════════════════════════

dim_users + events
    ├──▶ src/analysis/retention.py
    │    └──▶ analysis_weekly_retention + analysis_churn_risk
    │
    └──▶ src/analysis/ab_testing.py
         │ • Simulated experiment on 'Cant Lose Them' segment
         └──▶ [Console report: lift, p-value, CI, power]

SERVING FLOW
══════════════════════════════════════════════════════════════

behavior.duckdb (all tables)
    │
    │ db_utils.py: get_connection() [@st.cache_resource]
    │ • Local: read_only DuckDB connection
    │ • Cloud: in-memory DuckDB from parquet
    │
    ▼
Streamlit Dashboard (7 pages)
    ├── Home.py → Hero metrics (event count, revenue, conversion)
    ├── 0_Project_Overview.py → Architecture, SVG diagrams
    ├── 1_Data_Explorer.py → Distributions, data quality
    ├── 2_Optimization_Engine.py → Memory/query optimization showcase
    ├── 3_Executive_Overview.py → KPI dashboard
    ├── 4_User_Intelligence.py → RFM scatter, cohort retention
    ├── 5_Experiment_Lab.py → Interactive A/B simulator (NumPy)
    └── 6_ML_Engine.py → Feature importance, lift chart, rec engine
         │
         ▼ Plotly charts → Browser
```

---

## 13. Scalability Analysis

### 13.1 Current Scale vs Projected Scaling

| Operation | 109M rows (current) | 1B rows (10x) | 10B rows (100x) |
|---|---|---|---|
| Parquet load | 30s | ~5 min | ~50 min |
| Type casting (Polars) | 45s | ~7 min | ~70 min |
| DuckDB ingestion | 3.5 min | ~35 min | Needs chunking |
| Sessionization (GROUP BY UUID) | 85s | ~12 min | OOM risk on 32GB |
| RFM calculation | 18s | ~3 min | ~30 min |
| Market basket (self-join) | 90s | ~15 min* | OOM without filtering |
| Dashboard queries | <1s | <1s (no change) | <1s (no change) |

*Market basket: Quadratic risk if not pre-filtered to top-N popular products

### 13.2 Bottleneck Analysis

**Primary bottleneck:** Sessionization GROUP BY UUID  
- UUID cardinality: 15M unique sessions in 109M events
- At 1B events with proportional scale: 150M unique sessions
- 150M UUID GROUP BY requires ~6-12GB memory for hash tables
- Solution: Increase `memory_limit` to 32GB+ or switch to chunked streaming

**Secondary bottleneck:** Market basket self-join  
- Complexity: O(n²) where n = purchase events in basket
- Current mitigation: `HAVING COUNT(*) >= 5` reduces candidates dramatically
- At 1B rows: Pre-filter to top 10K most-purchased products before self-join

### 13.3 Path to Distributed Scale

```
Current (single-node DuckDB):           Up to ~500M rows on 32GB RAM
    ↓ If >500M rows needed
MotherDuck (DuckDB cloud):              Same SQL API, automatic scaling
    ↓ If petabyte scale
Apache Iceberg + Spark/Trino:           Cloud-native, column-partitioned
    ↓ If real-time needed
Apache Kafka + Flink:                   Streaming, sub-second updates
```

The dimensional modeling and SQL patterns are **directly compatible** with Trino, Presto, and BigQuery — migration would require changing connection strings, not rewriting queries.

---

## 14. Security & Reliability

### 14.1 Security Properties

| Concern | Implementation |
|---|---|
| Data privacy | All user_ids are anonymized integers (no PII) |
| Read-only dashboard | `duckdb.connect(read_only=True)` in production |
| No write operations in cloud | Sample mode enforces read-only |
| Secret management | Streamlit Secrets API for any keys |
| SQL injection | Static queries only, no user-input interpolation |

**Note:** The ML Engine recommendation query does interpolate `selected_product_id` from a selectbox, but the selectbox values come from a database query (not free text), mitigating injection risk.

### 14.2 Reliability Patterns

**Resource cleanup:**
```python
try:
    con = duckdb.connect(db_path)
    # ... operations ...
except Exception as e:
    logger.error(f"Error: {e}")
finally:
    con.close()  # Always closes, even on exception
```

**NULL safety:**
```sql
COALESCE(r.recency_days, -1) as recency_days,  -- -1 indicates non-buyer
COALESCE(s.total_sessions, 0) as total_sessions,
CASE WHEN SUM(has_cart::INT) = 0 THEN 0         -- Division-by-zero guard
     ELSE SUM(has_purchase::INT) / SUM(has_cart::INT)
END as checkout_rate
```

**Connection singleton:**
```python
@st.cache_resource  # Prevents N connections for N concurrent users
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)
```

---

## 15. Known Limitations & Future Improvements

### 15.1 Current Limitations

| Limitation | Impact | Priority |
|---|---|---|
| Empty test suite | No regression protection | HIGH |
| No MLflow/experiment tracking | Can't compare model versions | HIGH |
| Hardcoded ML metrics in UI | UI doesn't reflect model retraining | MEDIUM |
| No SHAP explainability | Feature importance less convincing | MEDIUM |
| Cohort retention disabled in cloud | Key insight not visible in demo | MEDIUM |
| No real-time inference API | Can't score new users live | LOW |
| No CI/CD pipeline | Manual deployment process | LOW |

### 15.2 Recommended Improvements (Priority Order)

**1. Add MLflow experiment tracking**
```python
import mlflow
with mlflow.start_run():
    mlflow.log_params(params)
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("precision_top5pct", precision)
    mlflow.sklearn.log_model(model, "propensity_model")
```

**2. Add pytest test suite**
```python
# tests/test_segmentation.py
def test_rfm_segments_cover_all_buyers():
    result = con.execute("SELECT COUNT(*) FROM analysis_rfm_segments").fetchone()[0]
    buyer_count = con.execute("SELECT COUNT(*) FROM dim_users WHERE is_buyer").fetchone()[0]
    assert result == buyer_count

def test_lift_threshold():
    low_lift = con.execute("SELECT COUNT(*) FROM predictions_product_affinity WHERE lift < 1.2").fetchone()[0]
    assert low_lift == 0
```

**3. Dynamic model loading in dashboard**
```python
# Instead of hardcoded values:
import pickle
model = pickle.load(open("src/models/propensity_lgbm.pkl", "rb"))
importance_df = pd.DataFrame({
    'feature': feature_names,
    'importance': model.feature_importance(importance_type='gain')
})
```

**4. SHAP explainability**
```python
import shap
explainer = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_test[:1000])  # Sample for speed
shap.summary_plot(shap_values, X_test[:1000])
```

**5. FastAPI inference endpoint**
```python
from fastapi import FastAPI
import pickle

app = FastAPI()
model = pickle.load(open("src/models/propensity_lgbm.pkl", "rb"))

@app.post("/predict")
def predict_propensity(user_features: UserFeatures):
    X = pd.DataFrame([user_features.dict()])
    prob = model.predict(X)[0]
    return {"user_id": user_features.user_id, "propensity_score": float(prob)}
```

---

## Appendix A: File Inventory

| File | Size | Purpose |
|---|---|---|
| `app/Home.py` | 7.6 KB | Dashboard entry point |
| `app/db_utils.py` | 11.1 KB | Database connection + cloud bootstrap |
| `app/pages/0_Project_Overview.py` | 13.9 KB | Architecture page |
| `app/pages/1_Data_Explorer.py` | 17.3 KB | Data exploration (largest page) |
| `app/pages/2_Optimization_Engine.py` | 15.7 KB | Optimization showcase |
| `app/pages/3_Executive_Overview.py` | 4.6 KB | KPI dashboard |
| `app/pages/4_User_Intelligence.py` | 5.7 KB | RFM + cohort page |
| `app/pages/5_Experiment_Lab.py` | 5.0 KB | Interactive A/B simulator |
| `app/pages/6_ML_Engine.py` | 7.0 KB | ML showcase page |
| `src/ingestion/loader.py` | 2.2 KB | DuckDB ingestion |
| `src/processing/features.py` | 5.6 KB | Feature engineering |
| `src/processing/initial_modeling.py` | 4.7 KB | Star schema creation |
| `src/processing/sessionization.py` | 3.9 KB | Session aggregation |
| `src/analysis/ab_testing.py` | 6.5 KB | A/B testing engine |
| `src/analysis/retention.py` | 4.0 KB | Cohort retention |
| `src/analysis/segmentation.py` | 4.5 KB | RFM segmentation |
| `src/models/recommendations.py` | 4.9 KB | Market basket engine |
| `src/models/train_propensity.py` | 6.1 KB | LightGBM training |
| `src/models/propensity_lgbm.pkl` | 3.48 MB | Trained model artifact |
| `summarise/optimize_dataset.py` | 9.6 KB | Polars optimization pipeline |
| `summarise/statistical_summary.py` | 18.4 KB | EDA statistical report |
| `scripts/create_cloud_database.py` | 11.5 KB | Cloud DB builder |
| `scripts/create_sample_dataset.py` | 7.2 KB | Sample generator |
| `config/config.yaml` | 324 B | Local configuration |
| `config/config.cloud.yaml` | 265 B | Cloud configuration |
| `system_design.svg` | 128.9 KB | System architecture diagram |
| `dimensional_modeling.svg` | 179.2 KB | Star schema diagram |
| `requirements.txt` | 348 B | Pinned dependencies |
| `.devcontainer/devcontainer.json` | 1.1 KB | DevContainer spec |
| `docs/DEPLOYMENT.md` | 8.3 KB | Cloud deployment guide |
| `docs/LOCAL_SETUP.md` | 7.3 KB | Local setup guide |

---

## Appendix B: Key Technical Decisions Log

| Decision | Alternative Considered | Rationale | Source Evidence |
|---|---|---|---|
| DuckDB over Pandas | Pandas, PySpark | Zero infrastructure, vectorized OLAP, <1s latency | Optimization page lines 408-441 |
| DuckDB over Cloud Warehouse | BigQuery, Snowflake | $0 cost, no infra management | Same source |
| ZSTD Level 3 over Level 10 | Level 10 (smaller but 3x slower write) | Dev iteration speed > marginal compression | Optimization page lines 126-131 |
| LightGBM over XGBoost | XGBoost, Random Forest | GPU support, speed on imbalanced data | train_propensity.py params |
| Temporal train/test split | Random split | Prevents data leakage, realistic deployment simulation | train_propensity.py lines 33-34 |
| Top-5% threshold over 50% | Standard 0.5 threshold | Business context: limited marketing budget | train_propensity.py lines 134-135 |
| Polars over Pandas for ETL | Pandas | Lazy evaluation, 10x faster, streaming sink | optimize_dataset.py |
| DISTINCT ON over ROW_NUMBER() | Window function | Streaming vs materialization, memory efficiency | initial_modeling.py lines 31-44 |
| LEFT JOIN for features_users | INNER JOIN | Non-buyers need 'Browser' segment for full ML coverage | features.py lines 110-112 |
| Streamlit over Dash/Grafana | Dash, Grafana | Rapid prototyping, Python-native, free cloud deployment | devcontainer.json postAttachCommand |

---

*System Design Document — Customer Intelligence Platform*  
*Reverse engineered and documented by forensic code analysis, June 12, 2026*
