# Fresh GitHub Repository Setup Guide

## 🚨 IMPORTANT: What NOT to Upload

**DO NOT upload these large files:**
- ❌ `data/db/behavior.duckdb` (5.4GB - full database)
- ❌ `data/raw/*.csv` (12GB - raw data files)
- ❌ Any files in `outputs/`, `logs/`, `__pycache__/`
- ❌ Virtual environment `.venv/`

**ONLY upload these:**
- ✅ `data/sample/sample.duckdb` (~98 MB)
- ✅ `data/sample/sample_optimized.parquet` (~36 MB)
- ✅ All code files (`.py`, `.md`, `.toml`, `.yaml`, `.txt`)
- ✅ Documentation

---

## Step-by-Step: Fresh Repository Setup

### Step 1: Clean Your Local Repository

```powershell
# Navigate to project
cd "c:\Project\Customer Intelligence Platform"

# Remove git tracking (but keep files)
Remove-Item -Recurse -Force .git

# Verify .gitignore is correct (see below for content)
```

### Step 2: Verify .gitignore Content

Make sure your `.gitignore` has exactly this:

```gitignore
# Virtual environment
.venv/
venv/
env/

# Large datasets (local only) - DO NOT UPLOAD
data/raw/
data/db/
data/processed/

# Keep sample data for cloud deployment
!data/sample/

# Models
src/models/*.pkl

# Logs and outputs
logs/
outputs/
__pycache__/
*.pyc
*.pyo
.Python

# IDE
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db

# Local config
.streamlit/secrets.toml
config/config.local.yaml

# Jupyter
.ipynb_checkpoints/
*.ipynb

# Analysis
analysis_subsets/

# Temporary files
*.tmp
*~
```

### Step 3: Initialize Fresh Git Repository

```powershell
# Initialize new git repo
git init

# Configure git (if needed)
git config user.email "your.email@example.com"
git config user.name "Your Name"

# Add remote (replace with YOUR repository URL)
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
```

### Step 4: Add ONLY What's Needed

```powershell
# Add files in specific order to verify
git add .gitignore
git add README.md
git add QUICKSTART.md
git add DEPLOYMENT_READY.md
git add requirements.txt

# Add configuration
git add .streamlit/
git add config/config.cloud.yaml

# Add code
git add app/
git add scripts/
git add src/
git add docs/

# Add ONLY sample data (not full data!)
git add data/sample/
```

### Step 5: Verify Before Committing

```powershell
# Check what will be committed
git status

# Make sure you see:
# ✅ data/sample/sample.duckdb
# ✅ data/sample/sample_optimized.parquet
# ❌ NO data/db/ files
# ❌ NO data/raw/ files
# ❌ NO .venv/ files

# Check total size of staged files (should be ~150MB total)
git ls-files -s | ForEach-Object { $_.Split()[3] } | ForEach-Object { Get-Item $_ -ErrorAction SilentlyContinue } | Measure-Object -Property Length -Sum | Select-Object @{Name="TotalMB";Expression={[math]::Round($_.Sum/1MB, 2)}}
```

### Step 6: Commit and Push

```powershell
# Commit
git commit -m "Initial commit: Streamlit Cloud deployment with sample data

- Customer Intelligence Platform for cloud deployment
- Sample dataset (1.6M events) with all features
- Complete documentation and setup guides
- Production-ready for Streamlit Cloud"

# Create main branch
git branch -M main

# Push to GitHub
git push -u origin main
```

---

## 🔍 Troubleshooting

### If push is too slow or large:

```powershell
# Check what's being pushed
git ls-files | ForEach-Object { Get-Item $_ -ErrorAction SilentlyContinue } | Where-Object { $_.Length -gt 10MB } | Select-Object Name, @{Name="SizeMB";Expression={[math]::Round($_.Length/1MB, 2)}}
```

Should only show:
- `data/sample/sample.duckdb` (~98 MB)
- `data/sample/sample_optimized.parquet` (~36 MB)

### If wrong files are tracked:

```powershell
# Remove from git but keep locally
git rm --cached -r data/db/
git rm --cached -r data/raw/
git rm --cached -r .venv/

# Commit the removal
git commit -m "Remove large files from tracking"
```

---

## ✅ Expected Repository Size

- **Total files**: ~100-150 files
- **Total size**: ~150-200 MB
- **Upload time**: 2-5 minutes (depending on internet)

---

## 📁 What Should Be in Your Repository

```
Customer-Intelligence-Platform/
├── .gitignore              ✅
├── .streamlit/
│   └── config.toml         ✅
├── app/
│   ├── Home.py             ✅
│   ├── db_utils.py         ✅
│   ├── pages/              ✅ (all 7 pages)
│   └── components/         ✅
├── config/
│   └── config.cloud.yaml   ✅
├── data/
│   └── sample/             ✅ (ONLY this folder!)
│       ├── sample.duckdb
│       └── sample_optimized.parquet
├── docs/                   ✅
├── scripts/                ✅
├── src/                    ✅
├── README.md               ✅
├── QUICKSTART.md           ✅
├── DEPLOYMENT_READY.md     ✅
└── requirements.txt        ✅
```

**NOT in repository:**
- ❌ `data/db/` - Full database
- ❌ `data/raw/` - Raw CSV files
- ❌ `.venv/` - Virtual environment
- ❌ `outputs/`, `logs/` - Generated files

---

## 🎯 Next Steps After Push

1. Go to [share.streamlit.io](https://share.streamlit.io/)
2. Connect your GitHub repository
3. Set main file: `app/Home.py`
4. Deploy!

---

**Need help?** Run the verification commands above to check what's being uploaded.
