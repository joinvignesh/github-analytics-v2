# ============================================================================
# Setup Cloud Scheduler for dbt Automation
# Creates a daily schedule to trigger dbt build
# ============================================================================

# Configuration
$PROJECT_ID = "github-analytics-486213-u7"
$REGION = "asia-south1"
$SERVICE_NAME = "dbt-runner"
$JOB_NAME = "dbt-daily-build"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  Cloud Scheduler Setup for dbt Automation" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Get Cloud Run service URL
Write-Host "[1/3] Getting Cloud Run service URL..." -ForegroundColor Yellow
$SERVICE_URL = gcloud run services describe $SERVICE_NAME `
    --region=$REGION `
    --project=$PROJECT_ID `
    --format="value(status.url)"

if ([string]::IsNullOrEmpty($SERVICE_URL)) {
    Write-Host "❌ Cloud Run service not found. Deploy the service first using deploy.ps1" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Service URL: $SERVICE_URL" -ForegroundColor Green

# Create Cloud Scheduler job
Write-Host ""
Write-Host "[2/3] Creating Cloud Scheduler job..." -ForegroundColor Yellow
Write-Host "Schedule: Daily at 2:00 AM UTC (7:30 AM IST)" -ForegroundColor Gray

gcloud scheduler jobs create http $JOB_NAME `
    --location=$REGION `
    --schedule="0 2 * * *" `
    --time-zone="UTC" `
    --uri="$SERVICE_URL/run" `
    --http-method=POST `
    --headers="Content-Type=application/json" `
    --message-body='{"command":"build"}' `
    --attempt-deadline=30m `
    --description="Daily dbt build automation"

if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠️  Job might already exist. Attempting to update..." -ForegroundColor Yellow
    
    gcloud scheduler jobs update http $JOB_NAME `
        --location=$REGION `
        --schedule="0 2 * * *" `
        --time-zone="UTC" `
        --uri="$SERVICE_URL/run" `
        --http-method=POST `
        --clear-headers `
        --headers="Content-Type=application/json" `
        --message-body='{"command":"build"}' `
        --attempt-deadline=30m
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to create or update scheduler job" -ForegroundColor Red
        exit 1
    }
}

Write-Host "✅ Scheduler job created/updated" -ForegroundColor Green

# Test the job
Write-Host ""
Write-Host "[3/3] Testing the scheduler job..." -ForegroundColor Yellow
Write-Host "Would you like to trigger a test run now? (Y/N)" -ForegroundColor Cyan
$response = Read-Host

if ($response -eq "Y" -or $response -eq "y") {
    Write-Host "Triggering test run..." -ForegroundColor Gray
    
    gcloud scheduler jobs run $JOB_NAME --location=$REGION
    
    Write-Host ""
    Write-Host "✅ Test run triggered" -ForegroundColor Green
    Write-Host ""
    Write-Host "Monitor logs with:" -ForegroundColor Yellow
    Write-Host "gcloud logging read 'resource.type=cloud_run_revision resource.labels.service_name=dbt-runner' --limit=50 --format=json" -ForegroundColor Gray
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  ✅ Cloud Scheduler Setup Complete!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Scheduler Details:" -ForegroundColor Cyan
Write-Host "  Job Name:  $JOB_NAME" -ForegroundColor White
Write-Host "  Schedule:  Daily at 2:00 AM IST (8:30 PM UTC)" -ForegroundColor White
Write-Host "  Region:    $REGION" -ForegroundColor White
Write-Host "  Target:    $SERVICE_URL/run" -ForegroundColor White
Write-Host ""
Write-Host "Manage your scheduler:" -ForegroundColor Yellow
Write-Host "  View jobs:   gcloud scheduler jobs list --location=$REGION" -ForegroundColor Gray
Write-Host "  Pause job:   gcloud scheduler jobs pause $JOB_NAME --location=$REGION" -ForegroundColor Gray
Write-Host "  Resume job:  gcloud scheduler jobs resume $JOB_NAME --location=$REGION" -ForegroundColor Gray
Write-Host "  Run now:     gcloud scheduler jobs run $JOB_NAME --location=$REGION" -ForegroundColor Gray
Write-Host "  Delete job:  gcloud scheduler jobs delete $JOB_NAME --location=$REGION" -ForegroundColor Gray
Write-Host ""
