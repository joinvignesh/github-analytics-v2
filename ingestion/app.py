"""
HTTP Wrapper for Cloud Run
Provides HTTP endpoint to trigger github_extractor.py

Cloud Run requires containers to:
1. Listen on PORT environment variable
2. Respond to HTTP requests
3. Return 200 OK for health checks

This wrapper handles all of that.
"""

import os
import logging
from flask import Flask, request, jsonify
from datetime import datetime

# Import our extraction logic
from github_extractor import main as run_extraction

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)


@app.route("/", methods=["GET"])
def health_check():
    """
    Health check endpoint
    Cloud Run pings this to verify container is healthy
    """
    return (
        jsonify(
            {
                "status": "healthy",
                "service": "github-extractor",
                "timestamp": datetime.utcnow().isoformat(),
            }
        ),
        200,
    )


@app.route("/extract", methods=["POST"])
def trigger_extraction():
    """
    Main extraction endpoint

    Accepts POST requests with optional JSON body:
    {
        "extraction_date": "2026-02-09",  # Optional, defaults to yesterday
        "dry_run": false                   # Optional, defaults to false
    }

    Returns:
    {
        "status": "success" | "error",
        "message": "...",
        "extraction_date": "2026-02-09",
        "repositories_processed": 2,
        "files_written": 6
    }
    """
    try:
        # Get request data
        data = request.get_json() or {}

        # Extract parameters
        extraction_date = data.get("extraction_date")
        dry_run = data.get("dry_run", False)

        logger.info(f"Extraction triggered via HTTP")
        logger.info(f"  Date: {extraction_date or 'default (yesterday)'}")
        logger.info(f"  Dry run: {dry_run}")

        # Set environment variables for the extraction script
        if extraction_date:
            os.environ["EXTRACTION_DATE"] = extraction_date
        if dry_run:
            os.environ["DRY_RUN"] = "true"

        # Run the extraction
        exit_code = run_extraction()

        if exit_code == 0:
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "Extraction completed successfully",
                        "extraction_date": extraction_date
                        or os.environ.get("EXTRACTION_DATE"),
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                ),
                200,
            )
        else:
            return (
                jsonify(
                    {
                        "status": "partial_success",
                        "message": "Some repositories failed, check logs",
                        "extraction_date": extraction_date
                        or os.environ.get("EXTRACTION_DATE"),
                        "exit_code": exit_code,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                ),
                207,
            )  # 207 Multi-Status

    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        return (
            jsonify(
                {
                    "status": "error",
                    "message": str(e),
                    "timestamp": datetime.utcnow().isoformat(),
                }
            ),
            500,
        )


@app.route("/status", methods=["GET"])
def get_status():
    """
    Status endpoint
    Returns information about the service
    """
    return (
        jsonify(
            {
                "service": "github-extractor",
                "version": "1.0",
                "environment": {
                    "project_id": os.getenv("GCP_PROJECT_ID"),
                    "bucket": os.getenv("GCS_BUCKET_NAME"),
                    "dry_run": os.getenv("DRY_RUN", "false"),
                },
                "timestamp": datetime.utcnow().isoformat(),
            }
        ),
        200,
    )


if __name__ == "__main__":
    # Get port from environment (Cloud Run sets this)
    port = int(os.environ.get("PORT", 8080))

    logger.info(f"Starting Flask server on port {port}")

    # Run Flask development server
    # In production, Gunicorn will run this instead
    app.run(
        host="0.0.0.0",  # Listen on all interfaces
        port=port,
        debug=False,  # Never use debug=True in production
    )
