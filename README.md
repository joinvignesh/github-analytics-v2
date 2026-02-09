
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
