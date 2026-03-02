# ============================================================================
# Deploy dbt Runner to Cloud Run - PowerShell Script
# Run this script to build and deploy your dbt automation
# ============================================================================

# Configuration
$PROJECT_ID = "github-analytics-486213-u7"
$REGION = "asia-south1"
$SERVICE_NAME = "dbt-runner"
$IMAGE_NAME = "gcr.io/$PROJECT_ID/dbt-runner"

# Snowflake credentials
$SNOWFLAKE_ACCOUNT = "VKYVOUN-JU13655"
$SNOWFLAKE_USER = "eeshaniwasa"
$SNOWFLAKE_PASSWORD = "Eeshwara@12345"  # REPLACE THIS

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  dbt Cloud Run Deployment Script" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Step 1: Verify prerequisites
Write-Host "[1/6] Verifying prerequisites..." -ForegroundColor Yellow

# Check if gcloud is installed
try {
    $gcloudVersion = gcloud version 2>&1
    Write-Host "✅ gcloud CLI found" -ForegroundColor Green
} catch {
    Write-Host "❌ gcloud CLI not found. Install from: https://cloud.google.com/sdk/docs/install" -ForegroundColor Red
    exit 1
}

# Check if Docker is running
try {
    $dockerVersion = docker version 2>&1
    Write-Host "✅ Docker found and running" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker not running. Start Docker Desktop." -ForegroundColor Red
    exit 1
}

# Check if dbt_project directory exists
if (!(Test-Path "./github_dbt")) {
    Write-Host "❌ github_dbt directory not found in current directory" -ForegroundColor Red
    Write-Host "   Please run this script from the project root (github-analytics-v2)" -ForegroundColor Yellow
    exit 1
}
Write-Host "✅ dbt project directory found" -ForegroundColor Green

# Step 2: Set GCP project
Write-Host ""
Write-Host "[2/6] Setting GCP project..." -ForegroundColor Yellow
gcloud config set project $PROJECT_ID
Write-Host "✅ Project set to: $PROJECT_ID" -ForegroundColor Green

# Step 3: Enable required APIs
Write-Host ""
Write-Host "[3/6] Enabling required GCP APIs..." -ForegroundColor Yellow
gcloud services enable run.googleapis.com
gcloud services enable containerregistry.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
Write-Host "✅ APIs enabled" -ForegroundColor Green

# Step 4: Build Docker image
Write-Host ""
Write-Host "[4/6] Building Docker image..." -ForegroundColor Yellow
Write-Host "This may take 5-10 minutes..." -ForegroundColor Gray

docker build -t $IMAGE_NAME .

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Docker build failed" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Docker image built successfully" -ForegroundColor Green

# Step 5: Push image to Container Registry
Write-Host ""
Write-Host "[5/6] Pushing image to Container Registry..." -ForegroundColor Yellow

# Configure Docker to use gcloud as credential helper
gcloud auth configure-docker --quiet

docker push $IMAGE_NAME

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Docker push failed" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Image pushed to: $IMAGE_NAME" -ForegroundColor Green

# Step 6: Deploy to Cloud Run
Write-Host ""
Write-Host "[6/6] Deploying to Cloud Run..." -ForegroundColor Yellow

gcloud run deploy $SERVICE_NAME `
    --image=$IMAGE_NAME `
    --region=$REGION `
    --platform=managed `
    --allow-unauthenticated `
    --memory=2Gi `
    --cpu=2 `
    --timeout=30m `
    --max-instances=1 `
    --min-instances=0 `
    --set-env-vars="SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT,SNOWFLAKE_USER=$SNOWFLAKE_USER,SNOWFLAKE_PASSWORD=$SNOWFLAKE_PASSWORD"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Cloud Run deployment failed" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  ✅ Deployment Successful!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""

# Get the service URL
$SERVICE_URL = gcloud run services describe $SERVICE_NAME `
    --region=$REGION `
    --format="value(status.url)"

Write-Host "Service URL: $SERVICE_URL" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Test the service:" -ForegroundColor White
Write-Host "   curl -X POST $SERVICE_URL/test" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Manually trigger dbt build:" -ForegroundColor White
Write-Host "   curl -X POST $SERVICE_URL/run" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Set up Cloud Scheduler (see setup_scheduler.ps1)" -ForegroundColor White
Write-Host ""
