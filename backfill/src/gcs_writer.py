"""
GCS Writer with Batching and Error Handling
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Any
from google.cloud import storage
from google.api_core import retry

from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class GCSWriter:
    """
    Handles writing JSON data to GCS with:
    - Batching for efficiency
    - Retry on failure
    - Compression support
    - Organized folder structure
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.project_id = config["gcp"]["project_id"]
        self.bucket_name = config["gcp"]["bucket_name"]

        # Initialize GCS client
        self.client = storage.Client(project=self.project_id)
        self.bucket = self.client.bucket(self.bucket_name)

        logger.info(f"GCS Writer initialized for bucket: {self.bucket_name}")

    def write_issues(
        self, owner: str, repo: str, issues: List[Dict[str, Any]], date: str
    ) -> Optional[str]:  # Changed from -> str
        """
        Write issues to GCS

        Path format: issues/YYYY/MM/DD/{owner}_{repo}_issues_{date}.json
        """
        if not issues:
            logger.warning(f"No issues to write for {owner}/{repo} on {date}")
            return None

        # Parse date for folder structure
        date_obj = datetime.fromisoformat(date)
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")

        # Construct path
        filename = f"{owner}_{repo}_issues_{date}.json"
        blob_path = f"issues/{year}/{month}/{day}/{filename}"

        return self._write_json(blob_path, issues)

    def write_comments(
        self, owner: str, repo: str, comments: List[Dict[str, Any]], date: str
    ) -> Optional[str]:  # Changed from -> str
        """Write comments to GCS"""
        if not comments:
            return None

        date_obj = datetime.fromisoformat(date)
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")

        filename = f"{owner}_{repo}_comments_{date}.json"
        blob_path = f"comments/{year}/{month}/{day}/{filename}"

        return self._write_json(blob_path, comments)

    def write_repository(
        self, owner: str, repo: str, repo_data: Dict[str, Any], date: str
    ) -> Optional[str]:  # Changed from -> str
        """Write repository metadata to GCS"""
        date_obj = datetime.fromisoformat(date)
        year = date_obj.strftime("%Y")
        month = date_obj.strftime("%m")
        day = date_obj.strftime("%d")

        filename = f"{owner}_{repo}_repository_{date}.json"
        blob_path = f"repositories/{year}/{month}/{day}/{filename}"

        return self._write_json(blob_path, [repo_data])  # Wrap in list for consistency

    @retry.Retry(predicate=retry.if_exception_type(Exception))
    def _write_json(self, blob_path: str, data: Any) -> str:
        """
        Write JSON data to GCS with retry

        Returns:
            GCS URI of uploaded file
        """
        try:
            blob = self.bucket.blob(blob_path)

            # Convert to JSON string
            json_data = json.dumps(data, indent=None, ensure_ascii=False)

            # Upload
            blob.upload_from_string(
                json_data, content_type="application/json", timeout=300
            )

            gcs_uri = f"gs://{self.bucket_name}/{blob_path}"
            logger.debug(f"Uploaded {len(data)} records to {gcs_uri}")

            return gcs_uri

        except Exception as e:
            logger.error(f"Error writing to GCS: {blob_path}: {e}")
            raise

    def file_exists(self, blob_path: str) -> bool:
        """Check if a file exists in GCS"""
        blob = self.bucket.blob(blob_path)
        return blob.exists()
