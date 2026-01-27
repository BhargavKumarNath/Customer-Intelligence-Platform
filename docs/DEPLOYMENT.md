# Streamlit Cloud Deployment Guide

This guide explains how to deploy the Customer Intelligence Platform to Streamlit Cloud.

## Quick Deploy (Recommended)

The easiest way to deploy is to fork this repository and connect it to Streamlit Cloud:

1. **Fork the Repository**
   - Go to [github.com/BhargavKumarNath/Customer-Intelligence-Platform](https://github.com/BhargavKumarNath/Customer-Intelligence-Platform)
   - Click "Fork" in the top right
   - Clone your fork locally

2. **Generate Sample Data** (if not already included)
   
   Run locally to generate sample data for cloud deployment:
   ```bash
   python scripts/create_sample_dataset.py
   python scripts/create_cloud_database.py
   ```
   
   This creates:
   - `data/sample/sample_optimized.parquet` (~50-150MB)
   - `data/sample/sample.duckdb` (~50-150MB)

3. **Commit Sample Data**
   
   ```bash
   git add data/sample/
   git commit -m "Add sample data for cloud deployment"
   git push origin main
   ```

4. **Deploy to Streamlit Cloud**
   
   - Visit [share.streamlit.io](https://share.streamlit.io/)
   - Click "New app"
   - Select your forked repository
   - Set:
     - **Main file path**: `app/Home.py`
     - **Python version**: 3.10
   - Click "Deploy!"

5. **Wait for Deployment** (~3-5 minutes)
   
   Streamlit Cloud will:
   - Install dependencies from `requirements.txt`
   - Load the sample database
   - Launch the dashboard

---

## Environment Variables

The app automatically detects cloud vs local environment. No manual configuration needed!

**How it works:**
- Checks for `STREAMLIT_SHARING` environment variable
- Falls back to sample database if full database doesn't exist
- Adjusts memory limits and thread count for cloud

---

## Configuration Files

### `.streamlit/config.toml`

Controls Streamlit behavior on cloud:

```toml
[server]
maxUploadSize = 200
enableCORS = false
headless = true

[browser]
gatherUsageStats = false

[theme]
primaryColor = "#FF4B4B"
backgroundColor = "#FFFFFF"
```

**Already configured** - no changes needed!

### `requirements.txt`

Pinned versions ensure reproducible deployments:

```txt
streamlit==1.32.0
duckdb==0.10.0
polars==0.20.10
pandas==2.2.0
pyarrow==15.0.0
plotly==5.19.0
scikit-learn==1.4.1
```

**Important**: Don't use `>=` versions in production - pinned versions prevent breaking changes.

---

## Deployment Checklist

Before deploying, verify:

- [ ] Sample database exists in `data/sample/sample.duckdb`
- [ ] Sample parquet exists in `data/sample/sample_optimized.parquet`
- [ ] Both files are under 100MB each
- [ ] `.gitignore` excludes full database but includes sample
- [ ] `requirements.txt` has pinned versions
- [ ] `.streamlit/config.toml` exists
- [ ] All pages load locally with sample data
- [ ] No hardcoded absolute paths in code

### Quick Verification

```bash
# Test locally with sample data
python scripts/create_sample_dataset.py
python scripts/create_cloud_database.py

# Set environment variable to simulate cloud
$env:STREAMLIT_SHARING = "true"  # Windows PowerShell
export STREAMLIT_SHARING=true    # macOS/Linux

# Launch dashboard
streamlit run app/Home.py

# Verify:
# - Cloud deployment notice appears
# - Sample event count shown
# - All pages load
# - No errors in console
```

---

## Resource Limits

Streamlit Cloud free tier provides:
- **RAM**: ~1GB
- **CPU**: Shared
- **Storage**: Limited (GitHub repo)
- **Uptime**: App sleeps after inactivity

**Our optimizations:**
- ✅ Sample database: ~50-150MB
- ✅ Memory limit: 512MB
- ✅ 2 threads for stability
- ✅ All queries optimized for speed

---

## Monitoring Deployment

### Check Build Logs

In Streamlit Cloud dashboard:
1. Click on your app
2. View "logs" tab
3. Look for:
   - ✅ "Installing requirements"
   - ✅ "Database connection successful"
   - ✅ "Your app is live"

### Common Deployment Issues

#### Issue: "File too large"

**Cause**: Sample database exceeds 100MB

**Solution**: Reduce sample size in `scripts/create_sample_dataset.py`:
```python
SAMPLE_PERCENTAGE = 2.0  # Reduce from 3.0 to 2.0
```

#### Issue: "ModuleNotFoundError"

**Cause**: Missing dependency in requirements.txt

**Solution**: Add missing package with pinned version:
```txt
missing-package==1.2.3
```

#### Issue: "Database not found"

**Cause**: Sample database not in repository

**Solution**: 
```bash
# Generate locally
python scripts/create_sample_dataset.py
python scripts/create_cloud_database.py

# Commit and push
git add data/sample/
git commit -m "Add sample database"
git push
```

#### Issue: "App crashes with MemoryError"

**Cause**: Queries using too much RAM

**Solution**: Reduce memory limit in `db_utils.py`:
```python
con.execute("SET memory_limit='256MB';")  # Lower if needed
```

---

## Updating Deployment

To update your deployed app:

```bash
# Make changes locally
git add .
git commit -m "Your update message"
git push origin main
```

Streamlit Cloud automatically redeploys when you push to GitHub!

---

## Custom Domain (Optional)

Streamlit Cloud provides a default URL: `yourapp-name.streamlit.app`

To use a custom domain:
1. Go to app settings in Streamlit Cloud
2. Click "Custom domain"
3. Follow DNS configuration instructions

---

## Performance Optimization

### Caching Strategies

Already implemented in `db_utils.py`:

```python
@st.cache_resource
def get_connection():
    # Connection cached across all users
    return duckdb.connect(DB_PATH, read_only=True)
```

### Query Optimization

All queries are pre-optimized for cloud:
- Uses dimensional model (pre-aggregated)
- Sub-second query times
- Minimal memory footprint

---

## Security Considerations

### Secrets Management

If you need to add secrets (API keys, etc.):

1. In Streamlit Cloud dashboard:
   - Go to app settings
   - Click "Secrets"
   - Add in TOML format:
     ```toml
     [api_keys]
     my_key = "secret_value"
     ```

2. Access in code:
   ```python
   import streamlit as st
   api_key = st.secrets["api_keys"]["my_key"]
   ```

### Data Privacy

- ✅ Sample dataset contains anonymized IDs
- ✅ No personal information
- ✅ Read-only database access
- ✅ No write operations in cloud

---

## Cost Considerations

### Free Tier (Current Setup)
- ✅ **Cost**: $0/month
- ⚠️ **Limitations**: App sleeps after inactivity, shared resources

### Streamlit Cloud for Teams
- 💰 **Cost**: $250/month
- ✅ More resources, no sleep, private repos

### Alternative: Self-Hosted
- 💰 **Cost**: Variable (VPS ~$5-20/month)
- ✅ Full control, but requires DevOps knowledge

**Recommendation**: Free tier is perfect for portfolio/demo purposes!

---

## Troubleshooting

### App Won't Start

1. Check build logs for errors
2. Verify all files are committed to GitHub
3. Ensure sample database exists
4. Check requirements.txt syntax

### App is Slow

1. Verify using sample database (not full)
2. Check memory limits in `db_utils.py`
3. Add more caching with `@st.cache_data`
4. Optimize expensive queries

### Database Errors

1. Test locally first with: `streamlit run app/Home.py`
2. Verify sample database is valid:
   ```python
   import duckdb
   con = duckdb.connect('data/sample/sample.duckdb', read_only=True)
   print(con.execute("SELECT COUNT(*) FROM events").fetchone())
   ```
3. Regenerate if corrupted:
   ```bash
   python scripts/create_cloud_database.py
   ```

---

## Support

- **Streamlit Docs**: [docs.streamlit.io](https://docs.streamlit.io)
- **Community Forum**: [discuss.streamlit.io](https://discuss.streamlit.io)
- **GitHub Issues**: Report bugs in your repository

---

## Next Steps

After successful deployment:

1. ✅ Share your deployment URL
2. ✅ Add URL to README.md
3. ✅ Update badge: `[![Streamlit App](badge.svg)](YOUR_URL)`
4. ✅ Monitor usage in Streamlit Cloud dashboard
5. ✅ Gather feedback and iterate!

---

**Congratulations! Your Customer Intelligence Platform is now live! 🚀**
