
# GitHub Analytics Platform
Serverless data pipeline for analyzing open-source repository activity.
## Architecture
GitHub API → Cloud Run → GCS → Snowpipe → Snowflake → dbt → Looker
## Project Structure
```
├── ingestion/       # Data extraction (GitHub API → GCS)
├── tests/           # Unit tests
├── config/          # Configuration files
└── requirements.txt # Python dependencies
```
## Quick Start
*Documentation in progress - Day 3*
## Components
- **Ingestion**: Python script extracting GitHub data
- **Storage**: GCS bucket with date-partitioned JSON
- **Warehouse**: Snowflake with 4-layer architecture
- **Transformation**: dbt models (staging → marts)
- **Orchestration**: Cloud Workflows (serverless)
- **Visualization**: Looker dashboards
## Status
- [x] Day 1: GCP project setup
- [x] Day 2: Snowflake & integration
- [ ] Day 3: Extraction script (in progress)



# useful commands

# command to trigger cloud scheaduler -> which then triggers cloud run
gcloud scheduler jobs run github-daily-extraction --location="asia-south1" --project=$PROJECT_ID

# commadn to check the logs of cloud run
gcloud run services logs read github-extractor --region="asia-south1" --limit=20

# activate virtual environment
venv/scripts/activate


# snowflakes-credentials

SNOWFLAKE_ACCOUNT=FSMCQTJ-TN29225
SNOWFLAKE_REGION=me-central2
SNOWFLAKE_CLOUD=gcp
SNOWFLAKE_USER=vignesh2828
SNOWFLAKE_PASSWORD=mindActivation@282
snowflakes user created to use the snowflakes bucket:
	github_pipeline_user
	vigPipe@123!
snowflakes service account: k0s630000@gcpmecentral2-1-6ca6.iam.gserviceaccount.com






