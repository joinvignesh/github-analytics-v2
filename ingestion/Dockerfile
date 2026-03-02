# ============================================
# DOCKERFILE FOR GITHUB DATA EXTRACTOR
# ============================================
# This creates a container image that can run anywhere
# (Cloud Run, local Docker, Kubernetes, etc.)

# ============================================
# STAGE 1: BASE IMAGE
# ============================================
# Start with official Python image (slim version for smaller size)
FROM python:3.11-slim

# Why 3.11-slim?
# - python:3.11 = Python 3.11 installed
# - slim = Minimal OS (Debian), smaller image size (~150MB vs 1GB)
# - Alternative: python:3.11-alpine (even smaller but can have issues with some libraries)

# Set metadata (good practice for documentation)
LABEL maintainer="your-email@example.com"
LABEL description="GitHub data extraction service for analytics pipeline"
LABEL version="1.0"

# ============================================
# STAGE 2: WORKING DIRECTORY
# ============================================
# Create and set working directory inside container
WORKDIR /app

# All subsequent commands run from /app
# Think of this as: cd /app

# ============================================
# STAGE 3: INSTALL DEPENDENCIES
# ============================================
# Copy requirements file first (layer caching optimization)
# Why copy requirements.txt separately?
# - Docker caches layers
# - If requirements.txt doesn't change, Docker reuses cached layer
# - Faster builds when you only change code, not dependencies

COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Why --no-cache-dir?
# - pip normally caches downloaded packages
# - In containers, cache is useless (container is ephemeral)
# - Removing cache reduces image size by ~100MB

# ============================================
# STAGE 4: COPY APPLICATION CODE
# ============================================
# Copy the extraction script
COPY ingestion/github_extractor.py .

# Copy configuration file
COPY config/repositories.json /app/config/repositories.json

# Note: We DON'T copy:
# - .env (secrets should come from environment variables)
# - venv/ (already in .dockerignore)
# - __pycache__/ (already in .dockerignore)

# ============================================
# STAGE 5: CREATE NON-ROOT USER (Security)
# ============================================
# Running as root inside container is a security risk
# Create a non-privileged user

RUN groupadd -r appuser && \
    useradd -r -g appuser -u 1000 appuser && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Why this matters:
# - If container is compromised, attacker has limited permissions
# - Best practice for production containers
# - Cloud Run recommends this

# ============================================
# STAGE 6: ENVIRONMENT VARIABLES
# ============================================
# Set Python to run in unbuffered mode
ENV PYTHONUNBUFFERED=1

# Why unbuffered?
# - Ensures logs are immediately visible (not buffered)
# - Critical for debugging in Cloud Run
# - Otherwise logs might not appear until container stops

# Set Python path
ENV PYTHONPATH=/app

# ============================================
# STAGE 7: HEALTH CHECK (Optional but Recommended)
# ============================================
# Cloud Run doesn't use HEALTHCHECK, but good for local testing
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import sys; sys.exit(0)"

# ============================================
# STAGE 8: EXPOSE PORT
# ============================================
# Cloud Run expects containers to listen on PORT environment variable
# Default to 8080 if not specified
ENV PORT=8080
EXPOSE $PORT

# Note: Our script doesn't actually listen on HTTP yet
# We'll add a simple HTTP wrapper next

# ============================================
# STAGE 9: ENTRYPOINT
# ============================================
# Copy the HTTP wrapper
COPY ingestion/app.py .

# Use Gunicorn to run Flask app (production-grade)
# Why Gunicorn?
# - Flask's built-in server is for development only
# - Gunicorn is production-ready WSGI server
# - Handles multiple requests, better performance

CMD exec gunicorn \
    --bind :$PORT \
    --workers 1 \
    --threads 8 \
    --timeout 600 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    app:app

# Explanation of flags:
# --bind :$PORT          → Listen on Cloud Run's PORT variable
# --workers 1            → 1 process (Cloud Run handles scaling)
# --threads 8            → 8 threads per worker
# --timeout 600          → 10 min timeout (extraction can be slow)
# --access-logfile -     → Log to stdout (Cloud Logging captures this)
# --error-logfile -      → Errors to stdout
# --log-level info       → Info level logging
# app:app                → Module:app_object (app.py file, app variable)

