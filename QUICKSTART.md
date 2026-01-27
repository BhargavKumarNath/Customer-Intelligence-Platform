# Quick Start Guide for Cloud Deployment

## 🚀 Generate Sample Data for Cloud Deployment

Follow these steps to create sample data and deploy to Streamlit Cloud:

### Step 1: Install Dependencies

```bash
# Make sure you're in the project directory
cd "c:\Project\Customer Intelligence Platform"

# Activate virtual environment
.venv\Scripts\activate

# Install requirements 
pip install -r requirements.txt
```

### Step 2: Generate Sample Dataset

```bash
# This creates a representative sample from your full database
python scripts\create_sample_dataset.py
```

**Expected Output:**
- Sample parquet file: `data/sample/sample_optimized.parquet` (~50-150MB)
- Takes ~2-5 minutes depending on your system
- Will sample ~3% of data (approximately 3-5M events)

### Step 3: Build Cloud Database

```bash
# This builds the complete DuckDB database from the sample
python scripts\create_cloud_database.py
```

**Expected Output:**
- Sample database: `data/sample/sample.duckdb` (~50-150MB)
- All dimension tables and fact tables created
- ML predictions pre-computed
- Takes ~3-5 minutes

### Step 4: Test Locally

```bash
# Test with sample data locally before deploying
streamlit run app/Home.py
```

**Verify:**
- ✅ Cloud deployment notice appears on homepage
- ✅ Event count shows sample size (not 109M)
- ✅ All 7 pages load without errors
- ✅ Visualizations render correctly

### Step 5: Commit and Push to GitHub

```bash
# Add sample data files
git add data/sample/
git add docs/
git add README.md
git add scripts/
git add .gitignore
git add .streamlit/
git add config/
git add requirements.txt
git add app/

# Commit changes
git commit -m "Add Streamlit Cloud deployment support with sample data"

# Push to GitHub
git push origin main
```

### Step 6: Deploy to Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io/)
2. Click "New app"
3. Connect your GitHub repository
4. Configure:
   - **Repository**: Your forked/cloned repo
   - **Branch**: main
   - **Main file path**: `app/Home.py`
   - **Python version**: 3.10
5. Click "Deploy"
6. Wait 3-5 minutes for deployment

### Step 7: Verify Cloud Deployment

Once deployed, check:
- ✅ App loads successfully
- ✅ Cloud deployment banner visible
- ✅ Sample event count displayed correctly
- ✅ All pages functional
- ✅ No errors in Streamlit Cloud logs

---

## ⚡ Quick Commands Reference

```bash
# Full workflow
.venv\Scripts\activate
pip install -r requirements.txt
python scripts\create_sample_dataset.py
python scripts\create_cloud_database.py
streamlit run app/Home.py

# Verify database
.venv\Scripts\python.exe -c "import duckdb; con = duckdb.connect('data/sample/sample.duckdb', read_only=True); print('Events:', con.execute('SELECT COUNT(*) FROM events').fetchone()[0])"

# Check file sizes
Get-ChildItem data/sample/ | Select-Object Name, @{Name="SizeMB";Expression={[math]::Round($_.Length/1MB, 2)}}
```

---

## 🔧 Troubleshooting

### Issue: "Database not found"

Run the generation scripts first:
```bash
python scripts\create_sample_dataset.py
python scripts\create_cloud_database.py
```

### Issue: "Sample too large" (>100MB)

Reduce sample size in `scripts/create_sample_dataset.py`:
```python
SAMPLE_PERCENTAGE = 2.0  # Reduce from 3.0
```

### Issue: "pip install fails"

Ensure virtual environment is activated:
```bash
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

## 📚 Next Steps

After successful deployment:

1. **Update README.md** with your live URL
2. **Share your deployment** on LinkedIn/portfolio
3. **Monitor performance** in Streamlit Cloud dashboard
4. **Iterate and improve** based on feedback

---

**Need help?** Check:
- [LOCAL_SETUP.md](docs/LOCAL_SETUP.md) - Full local setup
- [DEPLOYMENT.md](docs/DEPLOYMENT.md) - Detailed deployment guide
- [README.md](README.md) - Project overview

---

**Ready to deploy! 🎉**
