# ============================================================================
# Dockerfile for dbt Cloud Run Service
# Purpose: Run dbt build on schedule via Cloud Scheduler
# ============================================================================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    dbt-core==1.11.4 \
    dbt-snowflake==1.11.2 \
    gunicorn==21.2.0 \
    flask==3.0.0

# Copy dbt project (will be copied from local machine during build)
COPY ./github_dbt /app/dbt_project

# Copy Flask application
COPY dbt_runner.py /app/dbt_runner.py

# Create .dbt directory for profiles
RUN mkdir -p /root/.dbt

# Copy profiles template (will be populated by env vars at runtime)
COPY profiles.yml /root/.dbt/profiles.yml

# Set environment variables
ENV PORT=8080
ENV DBT_PROJECT_DIR=/app/dbt_project
ENV DBT_PROFILES_DIR=/root/.dbt
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health').read()"

# Run Flask app with gunicorn
# --workers 1: Single worker (dbt is not concurrent-safe)
# --threads 2: Allow 2 threads for handling requests
# --timeout 1800: 30 minute timeout (dbt can take time)
CMD exec gunicorn --bind :$PORT \
    --workers 1 \
    --threads 2 \
    --timeout 1800 \
    --log-level info \
    dbt_runner:app
