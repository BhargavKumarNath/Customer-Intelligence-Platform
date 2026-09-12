# Customer Intelligence Platform

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-%E2%89%A51.0-yellow)](https://duckdb.org/)
[![Polars](https://img.shields.io/badge/Polars-%E2%89%A51.0-orange)](https://www.pola.rs/)
[![Next.js](https://img.shields.io/badge/Next.js-15-black)](https://nextjs.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110%2B-teal)](https://fastapi.tiangolo.com/)

An analytics platform built on top of the full 109.95M-row [eCommerce Behavior Data from a Multi-Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset (October and November 2019). It turns a raw clickstream log into RFM segments, a purchase-propensity model, product cross-sell rules, and an A/B test calculator, and it does all of that on a single machine rather than a cloud data warehouse.

**Live:** [customer-intelligence-platform-bhargav-kumar-naths-projects.vercel.app](https://customer-intelligence-platform-bhargav-kumar-naths-projects.vercel.app). The site is a static export (Next.js, deployed on Vercel) built from precomputed JSON and Parquet artifacts, so there is no backend on the request path and no cold start. It replaces an earlier Streamlit version of the same dashboard.

## Why this exists

E-commerce platforms generate huge event volumes but rarely have quick answers to simple questions: which users are about to churn, who is likely to buy again next month, which products get bought together. The usual answer is to reach for Snowflake, BigQuery, or Spark. Those are reasonable choices at real scale, but they add cost and setup that get in the way of early analysis.

DuckDB and Polars are capable enough on a laptop that a lot of that distributed infrastructure turns out to be unnecessary. This project is that argument made concrete: the full 110M-row dataset is ingested, modeled, and scored end to end on a 10 GB-RAM machine, with a **peak memory footprint of about 3 GB**.

Every number in this README is computed from the full dataset unless it is explicitly marked *(sample)*.

## How it works

Raw CSVs go through a memory-tuned DuckDB pipeline into a star schema, which feeds a LightGBM propensity model, RFM segmentation, market-basket rules, and an A/B testing engine. Two independent consumers sit on top of that: a statically-exported Next.js site (the dashboard) and an optional FastAPI service that exposes the same logic as REST endpoints. The frontend does not call the API. It reads precomputed artifacts and runs ad-hoc SQL in the browser via DuckDB-WASM, so the dashboard has no runtime dependency on a live backend.

### Ingestion and memory optimization

The raw data is `2019-Oct.csv` (5.3 GB, 42.4M rows) and `2019-Nov.csv` (8.4 GB, 67.5M rows), 13.7 GB and 109,950,743 rows combined. Loading that with `pandas.read_csv()` would need roughly 40 GB of RAM (extrapolated from a 3M-row sample at 389 bytes/row), which does not fit the target machine.

`summarise/optimize_dataset.py` reads both CSVs in a single streaming DuckDB `COPY`, downcasting `product_id`/`user_id` from `BIGINT` to `INTEGER` and `price` from `DOUBLE` to `FLOAT`, and dictionary-encoding the low-cardinality string columns. DuckDB spills to disk in bounded chunks instead of holding everything in RAM. The output Parquet is 1.82 GB, an 86.7% reduction from the CSVs. An earlier two-pass Polars version of this step used to build a 23M-entry in-memory dictionary for the `user_session` UUID column and OOM'd anything under 16 GB, which is why it was replaced.

Everything downstream is tuned the same way, through `src/utils/duckdb_env.py` (`memory_limit=3GB`, `threads=2`, disk spill on, overridable with `CIP_DUCKDB_MEMORY_LIMIT` / `CIP_DUCKDB_THREADS`). A few specific fixes mattered more than the rest: `loader.py` skips a redundant re-sort of the 110M-row table because the Parquet is already time-ordered; `sessionization.py` dropped a `mode()` aggregate that alone pushed memory to 8.5 GB across 23M session groups; and the reproduction script runs each stage under a `systemd-run` cgroup capped at 4 GB so a runaway stage gets killed cleanly instead of freezing the machine.

### Star schema and modeling

`src/ingestion/loader.py` loads the optimized Parquet into a persistent DuckDB database (`data/db/behavior.duckdb`, about 5.4 GB). `src/processing/` builds the star schema on top of it: `fact_sessions` (23.0M rows), `fact_daily_kpis` (61 days), `dim_users` (5.32M), `dim_products` (206,876). `src/analysis/` and `src/models/` run RFM segmentation, retention and churn analysis, market-basket association rules, and the propensity model on that schema.

A second, smaller build (`scripts/create_cloud_database.py` on a tracked 1.65M-row stratified sample) produces the same schema at a size small enough to ship. `scripts/build_static_artifacts.py` turns that sample database into the JSON and Parquet files the frontend serves, and the same sample database backs the API's Docker image and the test suite.

### Propensity model: an out-of-time split

The standard failure mode in behavioral modeling is a random train/test split that lets future events leak into training. If a user bought something in November and some of their November sessions land in the training fold, the model learns from the answer.

`src/models/train_propensity.py` avoids that by construction: features come only from October activity (event counts, sessions, views, carts, cart removals, days active, recency), and the label is whether the user purchased in November. The model is a LightGBM classifier, trained on the full dataset with fixed seeds, and it is checked into the repository as `src/models/propensity_lgbm.txt` because CI and Docker cannot retrain it (that needs the 13.7 GB raw dataset). Its metrics are written to `src/models/metrics.json` by the training run itself, not hand-typed, so they cannot drift from what was actually measured:

- **0.7541 ROC-AUC** on a held-out November test set
- Trained on 2,417,832 users, evaluated on 604,458 (an 80/20 stratified split of the 3,022,290 users active in October)
- Top 5% of scored users convert at **36.9%**, against an **8.0%** November baseline, a **4.6x lift**

### A/B testing and market basket analysis

`src/analysis/ab_testing.py` runs Welch's t-test (valid under unequal variances between cohorts), a delta-method confidence interval on the conversion lift, and a post-hoc power check. The frontend's Experiment Lab page runs a TypeScript port of the same statistics live in the browser, checked against a precomputed grid of 216 cases for parity.

`src/models/recommendations.py` mines product co-occurrence directly in DuckDB with self-joins and group-bys rather than loading purchases into a graph library. On the full dataset, that produces **7,704 directed product-pair rules** (lift greater than 1.2, at least 5 co-purchases per session) from 1.66M purchase events, with memory kept low by joining only the purchase rows rather than the full 110M-row log.

## Results

All figures below are computed from the full dataset: 109,950,743 events, 5,316,649 users, 23,016,650 sessions, 206,876 products, 697,470 buyers, $505.2M in purchase revenue, $304 average order value.

| Finding | Numbers | Read on it |
|---|---|---|
| High-intent users are identifiable | Top 5% of ML-scored users convert at 4.6x the baseline (36.9% vs 8.0%), 0.7541 ROC-AUC | Target campaigns at this cohort instead of the full user base |
| A specific at-risk group stands out | "Cant Lose Them" RFM segment: 36,754 users, ~$956 average historical spend, ~50 days since last purchase, high past purchase frequency | Small enough for a personalized reactivation effort, valuable enough to justify one |
| Product affinities exist but are concentrated | 7,704 directed pair rules from 1.66M purchase events; the strongest by volume are smartphone accessory pairs | "Frequently bought together" has real signal for high-traffic electronics, but most of the 206K-product catalog has too few co-purchases to mine |
| Recency is the dominant signal | November purchase rate by days since last October activity: 16.8% at 1 day, 10.2% at 4 to 7 days, 4.9% at 15 to 30 days | A user seen yesterday is about 3.5x more likely to buy than one last seen two to four weeks ago |
| The funnel breaks early, not late | Cart-to-purchase conversion is 60.6%; view-to-cart is 10.1%. Overall session conversion is 6.1% | The browse-to-cart step is where the volume is lost, not checkout |

### RFM segments (buyers only, recency measured against the dataset's last date)

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

Churn status across all 5.32M users, as of 2019-11-30: 1.40M active, 1.03M at risk, 2.88M churned (most of that last bucket is users who only ever appeared briefly in October).

The A/B test on the live site is **illustrative**, not a measured result: it runs a seeded synthetic experiment (assumed 12% base rate, +15% treatment effect) against the real 36,754-user "Cant Lose Them" population to exercise the statistics engine. The population is real; the outcome is simulated.

## Project structure

```text
customer-intelligence-platform/
├── api/                  # FastAPI service: routers, middleware, logging, exception handlers
│   └── routers/          # health, segments, propensity, recommendations, experiments
├── frontend/             # Next.js 15 static-export site (the live dashboard)
│   ├── src/app/           # Routes: /, /executive, /user-intelligence, /ml-engine, /experiments, /data-explorer, /overview
│   ├── src/components/    # Charts, panels, shell, design-system primitives
│   ├── src/lib/           # Data loading, stats engine (A/B), DuckDB-WASM client
│   └── scripts/           # Data-pin sync scripts and the A/B fixture generator
├── config/               # YAML configuration for the Hydra pipeline
├── data/                 # Parquet files and DuckDB database (not checked in, except the tracked sample)
├── scripts/              # One-off build scripts
│   ├── create_sample_dataset.py       # stratified sample from the full DuckDB
│   ├── create_cloud_database.py       # sample -> sample.duckdb (star schema)
│   ├── build_static_artifacts.py      # sample.duckdb -> the frontend's JSON/Parquet artifacts
│   ├── collect_full_dataset_stats.py  # prints headline figures from the full local database
│   └── generate_drift_report.py       # October-vs-November data drift report (CI)
├── src/                  # Core analytics, ML, and API-service modules
│   ├── analysis/         # RFM, cohort retention, A/B testing
│   ├── domain/           # Pydantic request/response models for the API
│   ├── ingestion/        # Data loading and schema validation
│   ├── models/           # Propensity model, metrics.json, recommendations
│   ├── processing/       # Sessionization, feature engineering, the shared dimensional-model builder
│   ├── services/         # Service layer used by the API (segmentation, propensity, etc.)
│   ├── utils/            # Shared helpers (duckdb_env.py, pipeline memory tuning)
│   ├── config.py         # Pydantic-settings config for the API service
│   └── db.py             # Read-only DuckDB connection manager for the API service
├── summarise/            # ETL scripts for compressing the raw dataset
├── tests/                # Unit, API, precompute-parity, and quality-gate tests
├── Dockerfile            # Builds and serves the FastAPI service (port 8000)
├── docker-compose.yml    # Local API container with healthcheck
├── pyproject.toml        # Package + optional-dependency groups (pipeline, api, dev)
└── readme.md             # This file
```

## Running it

### Data pipeline

```bash
git clone https://github.com/BhargavKumarNath/Customer-Intelligence-Platform.git
cd Customer-Intelligence-Platform
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -e ".[pipeline,dev]"
```

**Sample dataset** (recommended, no download needed). Uses the tracked `data/sample/sample_optimized.parquet`, a 1.65M-row stratified sample of the same two months:

```bash
python scripts/create_cloud_database.py
```

**Full dataset** (109.95M rows). Download `2019-Oct.csv` and `2019-Nov.csv` (about 13.7 GB) from the [Kaggle dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) into `data/`, then run:

```bash
python summarise/optimize_dataset.py        # -> data/raw/ecommerce_optimized.parquet (1.82 GB)
python src/ingestion/loader.py              # -> data/db/behavior.duckdb (~5.4 GB)
python src/processing/initial_modeling.py   # dim_products, dim_users, fact_daily_kpis
python src/processing/sessionization.py     # fact_sessions + funnel metrics
python src/analysis/segmentation.py         # RFM segments
python src/processing/features.py           # user-level feature store
python src/analysis/retention.py            # weekly retention, churn risk
python src/models/recommendations.py        # product affinity rules
python src/models/train_propensity.py       # propensity_lgbm.txt + metrics.json
```

On a 10 GB-RAM, 8-core Linux box, this runs end to end in around 15 minutes with peak memory under 3 GB. Raise `CIP_DUCKDB_MEMORY_LIMIT` / `CIP_DUCKDB_THREADS` on a bigger machine.

None of the generated files above are checked into Git (the propensity model is the one exception, since CI has no way to retrain it) and everything is reproducible from the Kaggle source.

### Frontend

```bash
cd frontend
pnpm install
pnpm dev      # http://localhost:3000, pulls and pins the data set via scripts/sync-data.mjs
# or: pnpm build && pnpm start   # production static build
```

The frontend builds against precomputed artifacts, not a live database connection, so it never needs the Python environment set up.

### API service

The API is not deployed anywhere by default. It exists as a working, tested service that exposes the same segmentation, propensity, recommendation, and A/B testing logic as REST endpoints, kept deliberately off the dashboard's request path (the frontend reads precomputed artifacts, not this API, so the two can be deployed or not deployed independently).

```bash
pip install -e ".[api]"
uvicorn api.main:app --reload
# docs at http://localhost:8000/docs
```

Or with Docker Compose, which builds the sample DuckDB into the image and serves on port 8000:

```bash
docker compose up --build
```

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Liveness probe |
| GET | `/ready` | Readiness probe (503 if the DuckDB file or model artifact is missing) |
| GET | `/v1/users/{user_id}/segment` | RFM segment for a user |
| GET | `/v1/users/{user_id}/propensity` | LightGBM purchase-propensity score |
| GET | `/v1/products/{product_id}/recommendations` | Market-basket cross-sell recommendations |
| POST | `/v1/experiments/ab-test` | Runs the A/B simulation engine against a named RFM segment |

It ships rate limiting (`slowapi`), structured JSON request logging (`structlog`, with a per-request `X-Request-ID`), and CORS scoped to known dashboard origins. There is no authentication layer, so it should be treated as an internal or demo service rather than a public API.

## CI/CD and deployment

Six GitHub Actions workflows cover this: `ci.yml` (backend lint, type-check, tests, security scans, Docker build), `cd.yml` (pushes the API image to GHCR, nothing is deployed from it by default), `codeql.yml` (weekly static analysis), `precompute.yml` (rebuilds the frontend's data artifacts whenever the pipeline or the sample data changes, with a byte-parity test against the previous build), `frontend-ci.yml` (lint, type-check, unit tests, static export, Playwright, Lighthouse, and deploy), and `synthetic.yml` (a scheduled check against the live site once it exists).

The frontend deploys to Vercel as a static export: `next build` with `output: 'export'` produces a plain HTML/CSS/JS bundle with no server, which `frontend-ci.yml` can ship via the Vercel CLI, or which Vercel's own Git integration can build directly. The API's Docker image is built and pushed to GHCR on every push to `main` but has no deploy target wired up; running it anywhere is a manual `docker run` or `gcloud run deploy` against that image.

## Tech stack

- **Data engineering:** [DuckDB](https://duckdb.org/) (in-process OLAP SQL), [Polars](https://pola.rs/), [Apache Parquet](https://arrow.apache.org/)
- **Machine learning:** [LightGBM](https://lightgbm.readthedocs.io/), scikit-learn
- **Frontend:** [Next.js](https://nextjs.org/) 15 (App Router, static export), Tailwind, Radix primitives, Recharts, [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview) for in-browser SQL
- **API:** [FastAPI](https://fastapi.tiangolo.com/) and Uvicorn, `slowapi`, `structlog`, Pydantic v2
- **Deployment:** Vercel (frontend, static, live) and Docker/GHCR (API image, not deployed by default)

## Limitations and what's next

1. **Causal inference.** The A/B module treats correlation as if it were causation, which is fine for a calculator but not for a real experiment. A tool like `DoWhy` or `EconML` would let it estimate true incrementality instead.
2. **Graph-based recommendations.** The SQL approach works well for pairwise affinities. Something like `Neo4j` would support multi-hop relationships (PageRank, Node2Vec embeddings) that a self-join cannot express.
3. **Streaming ingestion.** Single-user scoring is already available in real time through the API, but the pipeline that produces the underlying tables is batch-only. Feeding events through Kafka into DuckDB would keep segments and scores fresh without a full pipeline rerun.
4. **Frontend and API stay decoupled.** The dashboard reads precomputed artifacts rather than calling the API at request time, on purpose, so the two can be deployed independently. Their star-schema and RFM SQL is shared through one module (`src/processing/dimensional_model.py`), but there is no plan to wire a live frontend-to-API path beyond that.
5. **No API authentication.** The service has no auth layer, so treat it as internal or for a demo, not as something to expose publicly as-is.

---

Questions or collaborations: open an issue, or reach out on [LinkedIn](https://www.linkedin.com/in/bhargavkumarnath/).
