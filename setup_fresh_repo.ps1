# CLEAN GITHUB UPLOAD SCRIPT
# Run this step-by-step to upload ONLY what's needed

# =============================================================================
# STEP 1: CLEAN UP GIT TRACKING
# =============================================================================

Write-Host "Step 1: Cleaning git tracking..." -ForegroundColor Cyan

# Remove .git folder (keeps your files, removes git tracking)
if (Test-Path ".git") {
    Remove-Item -Recurse -Force ".git"
    Write-Host "✅ Removed old git tracking" -ForegroundColor Green
}

# =============================================================================
# STEP 2: VERIFY .GITIGNORE
# =============================================================================

Write-Host "`nStep 2: Verifying .gitignore..." -ForegroundColor Cyan

$gitignoreContent = Get-Content ".gitignore" -Raw
if ($gitignoreContent -match "data/db/" -and $gitignoreContent -match "data/raw/") {
    Write-Host "✅ .gitignore looks good" -ForegroundColor Green
} else {
    Write-Host "⚠️  WARNING: .gitignore may not exclude large files properly!" -ForegroundColor Yellow
}

# =============================================================================
# STEP 3: CHECK FILE SIZES
# =============================================================================

Write-Host "`nStep 3: Checking what will be uploaded..." -ForegroundColor Cyan

$sampleDbSize = (Get-Item "data\sample\sample.duckdb").Length / 1MB
$sampleParquetSize = (Get-Item "data\sample\sample_optimized.parquet").Length / 1MB

Write-Host "Sample data sizes:" -ForegroundColor Yellow
Write-Host "  - sample.duckdb: $([math]::Round($sampleDbSize, 2)) MB"
Write-Host "  - sample_optimized.parquet: $([math]::Round($sampleParquetSize, 2)) MB"
Write-Host "  - Total sample data: $([math]::Round($sampleDbSize + $sampleParquetSize, 2)) MB"

if ($sampleDbSize + $sampleParquetSize -gt 150) {
    Write-Host "⚠️  Warning: Sample data is large (>150MB)" -ForegroundColor Yellow
} else {
    Write-Host "✅ Sample data size is acceptable" -ForegroundColor Green
}

# =============================================================================
# STEP 4: INITIALIZE GIT
# =============================================================================

Write-Host "`nStep 4: Initializing fresh git repository..." -ForegroundColor Cyan

git init
Write-Host "✅ Git initialized" -ForegroundColor Green

# Configure git (update with your info if needed)
# git config user.email "your.email@example.com"
# git config user.name "Your Name"

# =============================================================================
# STEP 5: ADD FILES (IN ORDER)
# =============================================================================

Write-Host "`nStep 5: Adding files to git..." -ForegroundColor Cyan

# Add core files first
git add .gitignore
git add README.md
git add QUICKSTART.md
git add DEPLOYMENT_READY.md
git add requirements.txt

Write-Host "  Added documentation and config" -ForegroundColor Gray

# Add configuration
git add .streamlit/config.toml
git add config/config.cloud.yaml

Write-Host "  Added configuration files" -ForegroundColor Gray

# Add code directories
git add app/
git add scripts/
git add src/
git add docs/

Write-Host "  Added code directories" -ForegroundColor Gray

# Add ONLY sample data (this is the critical part!)
git add data/sample/

Write-Host "  Added sample data" -ForegroundColor Gray
Write-Host "✅ Files staged for commit" -ForegroundColor Green

# =============================================================================
# STEP 6: VERIFY BEFORE COMMITTING
# =============================================================================

Write-Host "`nStep 6: Verification check..." -ForegroundColor Cyan

# Count staged files
$stagedFiles = git diff --cached --name-only
$fileCount = ($stagedFiles | Measure-Object).Count

Write-Host "Total files to commit: $fileCount" -ForegroundColor Yellow

# Check for wrong files
$wrongFiles = $stagedFiles | Where-Object { 
    $_ -like "data/db/*" -or 
    $_ -like "data/raw/*" -or 
    $_ -like ".venv/*" -or
    $_ -like "outputs/*" -or
    $_ -like "logs/*"
}

if ($wrongFiles) {
    Write-Host "❌ ERROR: These files should NOT be uploaded:" -ForegroundColor Red
    $wrongFiles | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host "`nRun: git reset" -ForegroundColor Yellow
    Write-Host "Then fix .gitignore and try again" -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "✅ No large/unwanted files detected" -ForegroundColor Green
}

# Estimate upload size
Write-Host "`nEstimating upload size..." -ForegroundColor Cyan
$totalSizeMB = 0
$stagedFiles | ForEach-Object {
    if (Test-Path $_) {
        $size = (Get-Item $_).Length
        $totalSizeMB += $size
    }
}
$totalSizeMB = $totalSizeMB / 1MB

Write-Host "Estimated upload: $([math]::Round($totalSizeMB, 2)) MB" -ForegroundColor Yellow

if ($totalSizeMB -gt 300) {
    Write-Host "⚠️  WARNING: Upload size is large (>300MB)!" -ForegroundColor Red
    Write-Host "Something may be wrong. Review staged files." -ForegroundColor Yellow
} else {
    Write-Host "✅ Upload size looks reasonable" -ForegroundColor Green
}

# =============================================================================
# STEP 7: COMMIT
# =============================================================================

Write-Host "`nStep 7: Creating commit..." -ForegroundColor Cyan

git commit -m "Initial commit: Streamlit Cloud deployment with sample data

- Customer Intelligence Platform for cloud deployment
- Sample dataset (1.6M events) with all features
- Complete documentation and setup guides
- Production-ready for Streamlit Cloud"

Write-Host "✅ Files committed" -ForegroundColor Green

# =============================================================================
# STEP 8: READY TO PUSH
# =============================================================================

Write-Host "`n" + "="*60 -ForegroundColor Green
Write-Host "READY TO PUSH TO GITHUB" -ForegroundColor Green
Write-Host "="*60 -ForegroundColor Green

Write-Host "`nNext steps:" -ForegroundColor Cyan
Write-Host "1. Add your remote repository:" -ForegroundColor White
Write-Host "   git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Create main branch:" -ForegroundColor White
Write-Host "   git branch -M main" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Push to GitHub:" -ForegroundColor White
Write-Host "   git push -u origin main" -ForegroundColor Gray
Write-Host ""
Write-Host "Expected upload time: 2-5 minutes" -ForegroundColor Yellow
Write-Host "If it takes longer, something may be wrong!" -ForegroundColor Yellow

Write-Host "`n✅ Script complete! Follow the steps above to push." -ForegroundColor Green
