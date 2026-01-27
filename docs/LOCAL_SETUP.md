# Local Setup Guide

This guide walks you through setting up the complete Customer Intelligence Platform locally with the full 109M event dataset.

## Requirements

### System Requirements
- **OS**: Windows, macOS, or Linux
- **RAM**: 16GB minimum (32GB recommended for optimal performance)
- **Disk Space**: ~20GB (12GB raw data + 8GB processed)
- **Python**: 3.10 or higher

### Why Local Setup?

Running locally gives you:
- ✅ Complete 109M event dataset
- ✅ Full analytical capabilities
- ✅ Faster query performance (can use more RAM)
- ✅ Ability to experiment and customize
- ✅ No cloud resource constraints

---

## Step-by-Step Installation

### 1. Clone the Repository

```bash
git clone https://github.com/BhargavKumarNath/Customer-Intelligence-Platform.git
cd Customer-Intelligence-Platform
```

### 2. Create Virtual Environment

**Windows:**
```bash
python -m venv .venv
.venv\Scripts\activate
```

**macOS/Linux:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Download the Full Dataset

1. Visit the [Kaggle dataset page](https://www.kaggle.com/datasets/mkechinov/ecommerce-events-history-in-cosmetics-shop)
2. Download `2019-Oct.csv` and `2019-Nov.csv`
3. Create the data directory and move files:

```bash
# Create directory
mkdir -p data/raw

# Move downloaded files (adjust path to your downloads folder)
# Windows
move %USERPROFILE%\Downloads\2019-Oct.csv data\raw\
move %USERPROFILE%\Downloads\2019-Nov.csv data\raw\

# macOS/Linux
mv ~/Downloads/2019-Oct.csv data/raw/
mv ~/Downloads/2019-Nov.csv data/raw/
```

**Expected files:**
- `data/raw/2019-Oct.csv` (~5.4GB)
- `data/raw/2019-Nov.csv` (~8.6GB)

---

## Building the Database

### Step 1: Optimize Raw Data

This converts CSV to optimized Parquet format with type casting and compression:

```bash
python summarise/optimize_dataset.py
```

**Duration**: ~5-10 minutes  
**Output**: `data/raw/ecommerce_optimized.parquet` (~1.9GB)

**What it does:**
- Converts string columns to categorical
- Downcasts Int64 → Int32, Float64 → Float32
- Applies ZSTD Level 3 compression
- Reduces size by 73%

### Step 2: Ingest into DuckDB

Load the optimized parquet into DuckDB:

```bash
python -m src.ingestion.loader
```

**Duration**: ~3-5 minutes  
**Output**: `data/db/behavior.duckdb` (~5.4GB)

**Configuration**: Uses `config/config.yaml` settings:
- Memory limit: 8GB
- Threads: 4
- Sorted by event_time for optimal query performance

### Step 3: Build Dimensional Model

Create fact and dimension tables:

```bash
python src/processing/initial_modeling.py
```

**Duration**: ~5-8 minutes  
**Creates:**
- `dim_users` - User profiles (3M rows)
- `dim_products` - Product catalog (100K rows)
- `fact_sessions` - Session metrics (15M rows)
- `fact_daily_kpis` - Daily aggregates (61 rows)

### Step 4: Generate ML Predictions

Train models and create predictions:

```bash
# User segmentation (RFM)
python src/analysis/segmentation.py

# Propensity model (optional - requires lightgbm)
python src/models/train_propensity.py

# Product recommendations
python src/models/recommendations.py
```

**Duration**: ~10-15 minutes total

---

## Launch the Dashboard

```bash
streamlit run app/Home.py
```

The dashboard will open in your browser at `http://localhost:8501`

---

## Verify Installation

You should see:
- ✅ "Events Processed: 109,421,899" on the homepage
- ✅ All 7 pages load without errors
- ✅ Visualizations render correctly
- ✅ Interactive features work (dropdowns, selectors)
- ✅ Sample data notice should NOT appear

---

## Troubleshooting

### Issue: "Database not found"

**Solution**: Make sure all build steps completed successfully:
```bash
# Check if database exists
ls data/db/behavior.duckdb

# If not, rebuild from Step 2
python -m src.ingestion.loader
python src/processing/initial_modeling.py
```

### Issue: "MemoryError" during ingestion

**Solution**: Reduce memory limit in `config/config.yaml`:
```yaml
database:
  memory_limit: 6GB  # Lower if you have 16GB RAM
```

### Issue: "Module not found"

**Solution**: Ensure virtual environment is activated and dependencies installed:
```bash
# Reactivate environment
source .venv/bin/activate  # macOS/Linux
.venv\Scripts\activate     # Windows

# Reinstall dependencies
pip install -r requirements.txt
```

### Issue: Queries are slow

**Solution**: Increase thread count in `config/config.yaml`:
```yaml
database:
  threads: 6  # Use more CPU cores
```

### Issue: "File too large" when uploading to Git

**Solution**: Large data files are gitignored. Only code and sample data are tracked. This is expected and correct.

---

## Project Configuration

### config/config.yaml

This file controls database and processing settings:

```yaml
database:
  main_table: events
  memory_limit: 8GB      # Adjust based on your system
  read_only: false

paths:
  database: C:\Project\Customer Intelligence Platform\data\db\behavior.duckdb
  processed: C:\Project\Customer Intelligence Platform\data\processed
  raw_data: C:\Project\Customer Intelligence Platform\data\raw\ecommerce_optimized.parquet
```

**Tips:**
- **16GB RAM systems**: Use `memory_limit: 6-8GB`
- **32GB+ RAM systems**: Can use `memory_limit: 16GB` for faster processing
- Adjust paths to absolute paths for your system

---

## Development Workflow

### Making Changes

1. **Edit code** in `app/` or `src/`
2. **Streamlit auto-reloads** when you save files
3. **Database changes** require re-running build scripts

### Rebuilding Specific Tables

```bash
# Just rebuild dimensional model
python src/processing/initial_modeling.py

# Just rebuild ML predictions
python src/models/recommendations.py
```

### Testing with Sample Data

To test cloud deployment locally:

```bash
# Generate sample dataset
python scripts/create_sample_dataset.py
python scripts/create_cloud_database.py

# The app will auto-detect and use sample database if available
streamlit run app/Home.py
```

---

## Performance Expectations

On a typical 16GB RAM system:

| Operation | Time | Notes |
|-----------|------|-------|
| Data optimization | 5-10 min | One-time setup |
| DuckDB ingestion | 3-5 min | One-time setup |
| Dimensional modeling | 5-8 min | One-time setup |
| Dashboard load | 2-5 sec | First page load |
| Page navigation | <1 sec | Cached queries |
| Complex aggregations | <2 sec | Optimized SQL |

---

## Next Steps

✅ **Explore the Dashboard**: Start with the Project Overview page  
✅ **Customize Analytics**: Modify queries in `src/analysis/`  
✅ **Add Features**: Extend pages in `app/pages/`  
✅ **Optimize Further**: Review Optimization Engine page for techniques  

---

## Getting Help

- **GitHub Issues**: Report bugs or request features
- **Documentation**: Check other guides in `docs/`
- **Code Comments**: Most scripts have detailed comments

---

Enjoy exploring your local Customer Intelligence Platform! 🚀
