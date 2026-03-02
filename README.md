
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

-- eeshaniwasa
-- Eeshwara@12345
-- VKYVOUN-JU13655.snowflakecomputing.com
-- https://app.snowflake.com/me-central2.gcp/dj61470/#/homepage






