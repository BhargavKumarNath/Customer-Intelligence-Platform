# Customer Intelligence Platform
## From 110M Events to Actionable Business Insights

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-%E2%89%A51.0-yellow)](https://duckdb.org/)
[![Polars](https://img.shields.io/badge/Polars-%E2%89%A51.0-orange)](https://www.pola.rs/)
[![Next.js](https://img.shields.io/badge/Next.js-15-black)](https://nextjs.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-teal)](https://fastapi.tiangolo.com/)

> **An end-to-end analytics platform that processes the full 109.95M-row [eCommerce Behavior Data from a Multi-Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (Oct + Nov 2019) on a single machine, no cloud warehouse required. It surfaces high-value customer segments, scores purchase propensity, and quantifies revenue opportunities.**
>
> Every number in this README below is produced from the **full dataset** unless it is explicitly labelled *(sample)*. The full pipeline was run end-to-end on a 10 GB-RAM Linux box within a **3 GB DuckDB memory budget** (see [Computational Considerations](#computational-considerations)).

---

**Live Site:** the dashboard has migrated from Streamlit to a statically-exported Next.js site, served from Vercel with data precomputed at build time (no backend on the request path). The production URL lands here once the Vercel connection is wired up (an owner infra step — see `deployment_stages.md` Phases 6 and 9); the original Streamlit deployment is being decommissioned as part of that same migration.

## Project Overview

The Customer Intelligence Platform takes raw behavioural event logs and turns them into something a business can actually act on. The main constraint I set for myself: everything had to run on a standard laptop, not a cloud cluster.

That required careful data engineering - aggressive type-casting, dictionary encoding for high-cardinality strings, columnar storage, and a proper star-schema model sitting on top of DuckDB. The result is a set of dashboards (executive KPIs, RFM segmentation, a LightGBM purchase propensity model, and a statistical A/B test simulator) built as a precomputed static site (`frontend/`, Next.js + DuckDB-WASM), plus a FastAPI service that exposes the same segmentation, propensity, recommendation, and A/B testing logic as versioned REST endpoints for programmatic use.

Note that these two consumers are independent: the frontend reads precomputed JSON/Parquet artifacts (and runs ad-hoc SQL in-browser via DuckDB-WASM) rather than calling the API, so there's no runtime coupling between them today. See `deployment_stages.md` for the full migration record (Stack E: precomputed static site, off the FastAPI service).

## Problem Statement

**Business challenge**: E-commerce platforms generate huge volumes of event data but rarely have the tooling to answer strategic questions quickly:
- *Which users are showing churn signals?*
- *Who is most likely to purchase next month?*
- *Which products are commonly bought together, and what does that mean for AOV?*

**Technical challenge**: At 100M+ rows, most teams reach for Snowflake, BigQuery, or Spark. Those are valid choices, but they add cost and operational overhead that slow down early-stage analysis.

**Approach**: Modern OLAP engines (DuckDB + Polars) are surprisingly capable on a single machine. This project is a demonstration of how far you can get before you actually need distributed compute.

---

## Data Science & ML Methodology

### 1. Out-of-Time Validation Split

The biggest trap in e-commerce behavioural modelling is leaking future events into training data via random K-fold splits. If a user purchased in November and some of their November sessions end up in the training fold, the model learns the wrong thing.

To avoid this, the propensity model (`src/models/train_propensity.py`) uses a strict temporal split:
- **Features**: Built entirely from raw October event counts: total events, sessions, views, carts, cart removals, active span in days, and days since the last October event. (A separate, richer `features_users` table with RFM flags and checkout density exists for other analyses, but it isn't what this model trains on, see the Feature Engineering note below.)
- **Target**: Whether the user made a purchase in November.
- **Result**: The reported AUC and top-5% conversion lift reflect genuine out-of-sample performance, not an artefact of the validation methodology. Retrained from scratch on the **full 109.95M-row dataset** (CPU-only, no GPU, fully deterministic run to run, `deterministic: true` + fixed seeds): **0.754 ROC-AUC**, **4.6x lift** — the top 5% of scored users convert at **36.9%** vs. an **8.0%** November baseline. Trained on 2,417,832 users, evaluated on a held-out 604,458 (all 3,022,290 users with October activity; 20% stratified test split). Numbers are written to `src/models/metrics.json` by the training script and read from there by the dashboard, so they cannot silently drift.

### 2. A/B Test Simulation Engine

Correlation is easy to find; knowing whether a segment is worth targeting takes a bit more care. The A/B testing module (`src/analysis/ab_testing.py`) handles:
- **Welch's t-test** (accounts for unequal variances between cohorts, unlike a simple means comparison)
- **Delta method** for 95% confidence intervals on conversion lifts
- **Post-hoc power analysis** to check whether a given cohort size is large enough to reliably detect the minimum effect size you care about

### 3. Market Basket Analysis in Pure SQL

Rather than loading purchase events into a Python graph library, association rule mining runs entirely inside DuckDB (`src/models/recommendations.py`). Self-joins and group-bys handle product support, co-occurrence counts, confidence, and lift, all on disk. On the full dataset this processes **1.66M purchase events** and produces **7,704 directed product-pair rules** (≥ 5 co-purchases in the same session, lift > 1.2). Memory stays flat because the self-join is on purchase rows only, not the full 110M-row log.

---

## System Architecture & Data Pipeline

![System design diagram](system_design.svg)

### 1. Ingestion & Memory Optimisation

- **Raw input**: `2019-Oct.csv` (5.3 GB, 42.4M rows) + `2019-Nov.csv` (8.4 GB, 67.5M rows) = **13.7 GB / 109,950,743 rows**.
- **Optimisation script** (`summarise/optimize_dataset.py`):
    - A single **streaming DuckDB `COPY`** reads both raw CSVs and writes one type-optimised Parquet. DuckDB processes the CSVs in bounded-memory chunks and spills to disk if a step needs more than `memory_limit`, so the pass never has to hold the dataset in RAM. (This replaced an earlier two-pass Polars pipeline whose `.cast(pl.Categorical)` on the 23M-unique `user_session` UUID built a 23M-entry in-memory dictionary and OOM'd sub-16 GB machines.)
    - Downcasts `product_id` / `user_id` `BIGINT -> INTEGER` and `price` `DOUBLE -> FLOAT`; low-cardinality string columns (`event_type`, `brand`, `category_code`) are dictionary-encoded automatically per Parquet row group.
    - **Measured**: a naive `pandas.read_csv()` of this data would need **~40 GB** of RAM (extrapolated from a 3M-row sample at 389 bytes/row) — infeasible on the target machine. The optimised Parquet is **1.82 GB on disk**, an **86.7% reduction** from the 13.7 GB CSV, and reads back an order of magnitude faster.

### 2. Dimensional Modelling & OLAP Layer

- **Full-scale build** (`src/ingestion/loader.py` -> `src/processing/` -> `src/analysis/`):
    - `loader.py` ingests the optimised Parquet into a persistent DuckDB file (`data/db/behavior.duckdb`, ~5.4 GB).
    - `initial_modeling.py` + `sessionization.py` build the star schema: fact tables (`fact_sessions` — 23.0M rows, `fact_daily_kpis` — 61 days) referencing dimension tables (`dim_users` — 5.32M, `dim_products` — 206,876).
    - All DuckDB sessions are tuned by `src/utils/duckdb_env.py` (default `memory_limit=3GB`, `threads=2`, disk spill on; override with `CIP_DUCKDB_*` env vars on a bigger machine).
- **Cloud/sample build** (`scripts/create_cloud_database.py` + `src/processing/dimensional_model.py`):
    - The same star schema, built from the tracked stratified sample. `scripts/build_static_artifacts.py` consumes it to precompute the frontend's JSON/Parquet artifacts (`deployment_stages.md` Phase 2); the same sample also backs the API image and CI.

### 3. Feature Engineering & ML

- **Feature store** (`src/processing/features.py`):
    - Builds user-level features in SQL (session aggregates, checkout density, duration variance, RFM flags), all materialised into a `features_users` table. This table backs the A/B test module's segment lookups. It is a separate, richer feature set from what the propensity model below trains on, not a shared input.
- **Propensity model** (`src/models/train_propensity.py`):
    - Trains a LightGBM classifier on a narrower, purpose-built set of October features (see "Out-of-Time Validation Split" above) with November purchases as the target label. The strict out-of-time split prevents any future data from leaking into training.
    - The checked-in model is **trained on the full 109.95M-row dataset** (deterministic seeds): **0.754 ROC-AUC** on the held-out November period; the top 5% of scored users convert at **4.6x the baseline rate**. It's checked in because CI/Docker can't retrain it (needs the 14 GB raw dataset). Full metrics: `src/models/metrics.json`.
- **Recommendations** (`src/models/recommendations.py`):
    - Runs market basket analysis through DuckDB to produce a `predictions_product_affinity` table of cross-sell candidates (7,704 directed rules on the full data).

---

## API Service

Alongside the static frontend, `api/` (FastAPI) exposes the same analytics as versioned REST endpoints, backed by a read-only `DuckDBConnectionManager` (`src/db.py`) and a thin service layer (`src/services/`) that wraps the domain logic. There is no reimplementation of the segmentation/propensity/recommendation/A-B-test code. Per `deployment_stages.md` (Stack E), the API is intentionally kept off the frontend's request path — it is a best-effort developer/demo service, deployed only if Phase 7 is picked up.

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Liveness probe |
| GET | `/ready` | Readiness probe (503 if the DuckDB file or model artifact is missing) |
| GET | `/v1/users/{user_id}/segment` | RFM segment for a user |
| GET | `/v1/users/{user_id}/propensity` | LightGBM purchase-propensity score |
| GET | `/v1/products/{product_id}/recommendations` | Market-basket cross-sell recommendations |
| POST | `/v1/experiments/ab-test` | Runs the A/B simulation engine against a named RFM segment |

It also ships rate limiting (`slowapi`), structured JSON request logging (`structlog`, with a per-request `X-Request-ID`), and CORS scoped to the known dashboard origins. There's no authentication layer, so treat it as an internal/demo service, not a public API.

**Run it locally:**
```bash
pip install -e ".[api]"
uvicorn api.main:app --reload
# docs at http://localhost:8000/docs
```

**Or via Docker Compose** (builds the sample DuckDB database into the image and serves on port 8000):
```bash
docker compose up --build
```

CI (`.github/workflows/ci.yml`) lints, type-checks, and runs the full test suite (including `tests/api/`) on every push; CD (`.github/workflows/cd.yml`) builds and pushes the image to GHCR (no deploy target wired up — the API stays undeployed unless Phase 7 is picked up). The frontend has its own pipeline: `.github/workflows/precompute.yml` (data build + parity gate + publish) and `.github/workflows/frontend-ci.yml` (lint/typecheck/test/export/Playwright/Lighthouse/deploy).

---

## Results & Business Impact

All figures below are from the **full dataset**: 109,950,743 events, 5,316,649 users, 23,016,650 sessions, 206,876 products, 697,470 buyers, $505.2M purchase revenue, $304 AOV (Oct + Nov 2019).

| Finding | Numbers (full dataset) | What to do with it |
|---|---|---|
| **High-intent users are identifiable** | Top 5% of ML-scored users convert at **4.6x the population baseline** — **36.9%** vs an **8.0%** November baseline (0.754 ROC-AUC, held-out November). | Run targeted campaigns against this cohort rather than the full list. |
| **At-risk VIPs** | The **"Cant Lose Them"** RFM segment: **36,754 users**, **~$956** avg historical spend, **~50 days** since last purchase — high past frequency (avg 3.1 purchase-days) that has gone quiet. | Small enough for a personalised reactivation flow; the spend history makes them worth prioritising. |
| **Product affinities are real but sparse** | **7,704** directed product-pair rules (lift > 1.2, ≥ 5 co-purchases/session) from **1.66M** purchase events; strongest by volume are Samsung/Apple smartphone accessory pairs. | "Frequently bought together" has signal, but only for high-traffic electronics — most of the 206K-product catalogue has too few co-purchases to mine. |
| **Recency dominates history** | Nov purchase rate by days-since-last-October-activity: **≤1 day → 16.8%**, 4–7 days → 10.2%, 15–30 days → 4.9%. A user seen yesterday is **~3.5x** more likely to buy than one last seen 2–4 weeks ago. | When scoring users for a campaign, recency should carry heavy weight — it's also the model's, and RFM's, dominant signal. |
| **The funnel break is up top** | Session cart-to-purchase is **60.6%** (solid); view-to-cart is **10.1%** — that's where sessions drop off. Overall session conversion **6.1%**. | Invest in the browse→cart step (merchandising, PDP quality), not the checkout. |

### RFM segments (buyers only, recency anchored to the dataset's max date)

| Segment | Users | % of buyers | Avg spend | Avg recency (days) |
|---|--:|--:|--:|--:|
| Loyal Customers | 148,165 | 21.2% | $668 | 15 |
| Champions | 136,518 | 19.6% | $1,642 | 8 |
| At Risk | 111,155 | 15.9% | $273 | 25 |
| Need Attention | 109,192 | 15.7% | $691 | 35 |
| Hibernating | 84,444 | 12.1% | $257 | 52 |
| New Customers | 46,725 | 6.7% | $283 | 10 |
| Cant Lose Them | 36,754 | 5.3% | $956 | 50 |
| Promising | 24,517 | 3.5% | $250 | 21 |

Churn status across all 5.32M users (as of 2019-11-30): **Active 1.40M · At Risk 1.03M · Churned 2.88M** — most of the "churned" bucket is the long tail of users who appeared only briefly in October.

### A/B test simulation *(illustrative)*

`src/analysis/ab_testing.py` runs a **synthetic** experiment (seeded Bernoulli outcomes, assumed 12% base rate, +15% treatment effect) against the real 36,754-user "Cant Lose Them" population: control 12.1% vs treatment 13.5%, relative lift 11.6%, p ≈ 6e-5, power 0.98. The population size is real; the outcomes are simulated to exercise the stats engine, not a measured result.

---

## Computational Considerations

### Full dataset vs. sample — what produced each number

| Stage | Full dataset (109.95M rows) | Sample (1.65M rows, tracked) |
|---|---|---|
| Ingestion / optimised Parquet | ✅ `summarise/optimize_dataset.py` on `data/2019-{Oct,Nov}.csv` | n/a |
| Star schema, RFM, retention, churn, feature store | ✅ `src/processing/` + `src/analysis/` on `data/db/behavior.duckdb` | ✅ `scripts/create_cloud_database.py` on `data/sample/` |
| Market-basket affinity | ✅ 7,704 rules | ✅ 11 rules *(sample)* |
| **Propensity model (`propensity_lgbm.pkl`, `metrics.json`)** | ✅ **checked-in model is the full-data one** (0.754 AUC, 4.6x lift) | — earlier revisions trained on the sample (0.72 AUC); superseded |
| Frontend (`frontend/`, static site) | — | ✅ built from precomputed artifacts (sample) |
| API service (`api/`) + `tests/` | — | ✅ always runs on the sample DuckDB |
| `scripts/build_static_artifacts.py` (deployment pre-compute) | — | ✅ feeds the frontend's JSON/Parquet artifacts |

Every "Results & Business Impact" number, the RFM table, the funnel rates, the recency
gradient, and the propensity metrics are **full-dataset**. The A/B test outcome is
simulated (population size is real). The sample is a genuine stratified sample of the
same two months, kept only so the cloud dashboard and CI have something small to run.

### Memory strategy (why it fits a 10 GB box)

The real dataset does not fit in RAM naively (~40 GB as a pandas frame), and early runs
of this pipeline froze a 10 GB machine by letting DuckDB commit 10–12 GB and thrash swap.
Fixes, all targeted:

- **`src/utils/duckdb_env.py`** — one place that tunes every pipeline DuckDB session:
  `memory_limit=3GB`, `threads=2`, `preserve_insertion_order=false`, disk spill enabled.
  Override per-machine with `CIP_DUCKDB_MEMORY_LIMIT` / `CIP_DUCKDB_THREADS` / `CIP_DUCKDB_TEMP_DIR`.
- **Ingestion is one streaming DuckDB `COPY`**, not a two-pass Polars pipeline — the old
  `.cast(pl.Categorical)` on 23M unique `user_session` UUIDs built a huge in-memory
  dictionary.
- **`loader.py` no longer re-sorts** the 110M-row table by `event_time` — the optimised
  Parquet is already time-ordered (verified: 218 out-of-order adjacent rows in 110M), and
  the re-sort spilled > 8 GB for no downstream benefit.
- **`sessionization.py`** dropped an unused `mode(category_code)` column — `mode()` is a
  holistic aggregate DuckDB can't spill, and over 23M session groups it drove RSS to
  ~8.5 GB. It also caps threads to 2 for that one heaviest GROUP BY.
- **`features.py`** replaced two `mode()` calls with a spillable `arg_max`-over-counts form.
- The reproduction script wraps each stage in a `systemd-run --scope -p MemoryMax=4G
  -p MemorySwapMax=0` cgroup, so a runaway stage is OOM-killed cleanly instead of freezing
  the host.

Result: the full pipeline completes with **peak RSS ≈ 3 GB** and no swap.

---

## Project Structure

```text
customer-intelligence-platform/
├── api/                  # FastAPI service: routers, middleware, logging, exception handlers
│   └── routers/          # health, segments, propensity, recommendations, experiments
├── frontend/             # Next.js 15 static-export site (the live dashboard)
│   ├── src/app/           # Routes: /, /executive, /user-intelligence, /ml-engine, /experiments, /data-explorer, /overview
│   ├── src/components/    # Charts, panels, shell, design-system primitives
│   ├── src/lib/           # Data loading, stats engine (A/B), DuckDB-WASM client
│   └── scripts/           # Data-pin sync scripts + the A/B fixture generator
├── config/               # YAML configuration files
├── data/                 # Parquet files and DuckDB database (not checked in)
├── scripts/              # One-off build scripts
│   ├── create_sample_dataset.py       # stratified sample from the full DuckDB
│   ├── create_cloud_database.py       # sample -> sample.duckdb (star schema)
│   ├── finalize_full_db.py            # legacy dashboard-compat views on the full DuckDB
│   └── build_static_artifacts.py      # sample.duckdb -> the frontend's JSON/Parquet artifacts
├── src/                  # Core analytics, ML, and API-service modules
│   ├── analysis/         # RFM, cohort retention, A/B testing
│   ├── domain/           # Pydantic request/response models for the API
│   ├── ingestion/        # Data loading and schema validation
│   ├── models/           # Propensity model, metrics.json, recommendations
│   ├── processing/       # Sessionisation, feature engineering, shared dimensional-model builder
│   ├── services/         # Service layer used by the API (segmentation, propensity, etc.)
│   ├── utils/            # Shared helpers (duckdb_env.py — pipeline memory tuning)
│   ├── config.py         # Pydantic-settings config for the API service
│   └── db.py             # Read-only DuckDB connection manager for the API service
├── summarise/            # ETL scripts for compressing the raw dataset
├── tests/                # Unit, API, and quality-gate tests
├── Dockerfile            # Builds and serves the FastAPI service (port 8000)
├── docker-compose.yml    # Local API container with healthcheck
├── pyproject.toml        # Package + optional-dependency groups (api, pipeline, dev)
└── readme.md             # This file
```

---

## Installation & Setup

You can run against a small representative sample (fast, no download) or rebuild the full pipeline from the raw 109.95M-row dataset. The steps below set up the **data pipeline**; for the **frontend**, see [Running the Frontend](#3-running-the-frontend) below, and for the **API service**, see [API Service](#api-service) above and install with `pip install -e ".[api]"`.

### 1. Environment Setup

```bash
git clone https://github.com/BhargavKumarNath/Customer-Intelligence-Platform.git
cd Customer-Intelligence-Platform
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate

# Full local pipeline (ingestion + processing + training):
pip install -e ".[pipeline,dev]"
```

### 2. Data Pipeline

**Option A: Sample dataset (recommended for exploration — no download)**

Uses the tracked `data/sample/sample_optimized.parquet` (1.65M-row stratified sample of the real 2-month data).
```bash
# Build the sample DuckDB: star schema, RFM segments, retention, affinity
python scripts/create_cloud_database.py
```

**Option B: Full dataset (109.95M rows)**

1. Download `2019-Oct.csv` and `2019-Nov.csv` (~13.7 GB) from the Kaggle dataset
   [*eCommerce behavior data from multi category store*](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
   and place them in `data/`.
2. Run the pipeline (each stage tuned for a small-RAM box by `src/utils/duckdb_env.py`;
   raise `CIP_DUCKDB_MEMORY_LIMIT` / `CIP_DUCKDB_THREADS` on a bigger machine):

```bash
python summarise/optimize_dataset.py        # data/2019-{Oct,Nov}.csv -> data/raw/ecommerce_optimized.parquet (1.82 GB)
python src/ingestion/loader.py              # -> data/db/behavior.duckdb  (events, ~5.4 GB)
python src/processing/initial_modeling.py   # dim_products, dim_users, fact_daily_kpis
python src/processing/sessionization.py     # fact_sessions (23.0M) + funnel metrics
python src/analysis/segmentation.py         # analysis_rfm_segments
python src/processing/features.py           # features_users (5.32M x 19)
python src/analysis/retention.py            # analysis_weekly_retention, analysis_churn_risk
python src/models/recommendations.py        # predictions_product_affinity
python src/models/train_propensity.py       # src/models/propensity_lgbm.pkl + metrics.json
python src/analysis/ab_testing.py           # A/B simulation (prints; writes nothing)
python scripts/finalize_full_db.py          # adds user_rfm_segments / weekly_retention views for the dashboard
```

On a 10 GB-RAM / 8-core Linux box the whole sequence runs in **~15 minutes** end-to-end
(ingestion ~2 min, `initial_modeling` ~90 s, `sessionization` ~45 s, feature/analysis
steps < 20 s each, training ~50 s). Peak resident memory stays under ~3 GB because
every DuckDB session is capped and free to spill to disk.

> `summarise/combine_csv_to_parquet.py` (the old Polars concat step) is retained but no
> longer needed — `optimize_dataset.py` reads the two raw CSVs directly.

### Rebuilding from Scratch — regenerated files

None of these are in Git; all are reproducible from the Kaggle source.

| File | Size | Built by |
|------|------|----------|
| `data/2019-Oct.csv`, `data/2019-Nov.csv` | 13.7 GB | Kaggle download |
| `data/raw/ecommerce_optimized.parquet` | 1.82 GB | `summarise/optimize_dataset.py` |
| `data/db/behavior.duckdb` | ~5.4 GB | `src/ingestion/loader.py` + `src/processing/` + `src/analysis/` |
| `src/models/propensity_lgbm.pkl`, `metrics.json` | ~3.4 MB | `src/models/train_propensity.py` (checked in) |

### 3. Running the Frontend

The dashboard is a statically-exported Next.js site under `frontend/`, built on the precomputed artifacts from `scripts/build_static_artifacts.py` (not on a live DuckDB connection):

```bash
cd frontend
pnpm install
pnpm dev      # http://localhost:3000, pulls/pins the data set via scripts/sync-data.mjs
# or: pnpm build && pnpm start   # production static build
```

---

## Tech Stack

- **Data engineering**: [DuckDB](https://duckdb.org/) (in-process OLAP SQL), [Polars](https://pola.rs/) (Rust-based DataFrame library), [Apache Parquet](https://arrow.apache.org/) (columnar storage)
- **Machine learning**: [LightGBM](https://lightgbm.readthedocs.io/), scikit-learn
- **Frontend**: [Next.js](https://nextjs.org/) 15 (App Router, static export), Tailwind, Radix primitives, Recharts, [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview) for in-browser ad-hoc SQL
- **API service**: [FastAPI](https://fastapi.tiangolo.com/) + Uvicorn, `slowapi` (rate limiting), `structlog` (structured logging), Pydantic v2
- **Deployment**: Docker + GHCR (API image, undeployed by default); Vercel + GitHub Actions (frontend: precompute → build → test → deploy)
- **Architecture**: Star schema dimensional model, config-driven pipeline, precompute-then-serve static frontend (see `deployment_stages.md`)

---

## Limitations & What's Next

1. **Causal inference**: The current A/B simulation assumes correlation implies causation. Integrating `DoWhy` or `EconML` would let you estimate true incrementality from the intervention.
2. **Graph-based recommendations**: The SQL approach works well for pairwise affinities, but moving to `Neo4j` would unlock multi-hop relationships (PageRank, Node2Vec embeddings).
3. **Streaming ingestion**: Single-user scoring is already available in real time via the FastAPI service, but the underlying pipeline is still batch-only. Connecting Kafka to DuckDB for intra-day event streaming would keep segments and propensity scores fresh without a full pipeline rerun.
4. **Wiring the frontend to the API**: the static frontend reads precomputed artifacts rather than calling the FastAPI service at request time (by design — see `deployment_stages.md`, Stack E), so the two remain decoupled. Their star-schema/RFM/retention SQL is consolidated into one shared module (`src/processing/dimensional_model.py`), but a live frontend-to-API integration (beyond the optional, off-the-critical-path Developer API page) is not planned.
5. **API auth**: The service currently has no authentication layer, so it's suitable for internal/demo use but not for a public deployment as-is.

---

**If this helped with your data engineering work, a star on the repo goes a long way!**

*Questions or collaborations? Open an issue or reach out on [LinkedIn](https://www.linkedin.com/in/bhargavkumarnath/).*
