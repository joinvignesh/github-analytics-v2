"""
GitHub Data Extractor
Fetches repository data from GitHub REST API and writes to GCS

Architecture:
- GitHubExtractor: Handles all GitHub API interactions
- GCSWriter: Handles all Google Cloud Storage writes
- main(): Orchestrates the extraction pipeline

Author: [Your Name]
Date: 2026-02-09
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import requests
from google.cloud import storage, secretmanager
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================
# GITHUB API EXTRACTOR CLASS
# ============================================


class GitHubExtractor:
    """
    Extracts data from GitHub REST API with:
    - Authentication
    - Pagination
    - Rate limit handling
    - Retry logic
    - Error handling
    """

    def __init__(self, token: str):
        """
        Initialize GitHub extractor

        Args:
            token: GitHub Personal Access Token
        """
        self.token = token
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        logger.info("GitHubExtractor initialized")

    def _make_request(
        self, url: str, params: Optional[Dict] = None, max_retries: int = 3
    ) -> requests.Response:
        """
        Make HTTP request with retry logic and rate limit handling

        Args:
            url: API endpoint URL
            params: Query parameters
            max_retries: Maximum number of retry attempts

        Returns:
            Response object

        Raises:
            requests.exceptions.RequestException: If all retries fail
        """
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url, headers=self.headers, params=params, timeout=30
                )

                # Check rate limit BEFORE processing response
                self._check_rate_limit(response)

                # Raise exception for HTTP errors
                response.raise_for_status()

                return response

            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code

                # Don't retry on authentication or not found errors
                if status_code in [401, 404]:
                    logger.error(f"HTTP {status_code} error: {url}")
                    raise

                # Retry on rate limit or server errors
                if status_code in [403, 500, 502, 503]:
                    if attempt < max_retries - 1:
                        wait_time = 2**attempt  # Exponential backoff: 1s, 2s, 4s
                        logger.warning(
                            f"HTTP {status_code} error, retry {attempt + 1}/{max_retries} "
                            f"after {wait_time}s: {url}"
                        )
                        time.sleep(wait_time)
                        continue

                # Re-raise on last attempt
                raise

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Request failed, retry {attempt + 1}/{max_retries} "
                        f"after {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                    continue
                raise

        # Should never reach here, but just in case
        raise requests.exceptions.RequestException("Max retries exceeded")

    def _check_rate_limit(self, response: requests.Response) -> None:
        """
        Check GitHub API rate limit and wait if necessary

        Args:
            response: Response object from GitHub API
        """
        remaining = int(response.headers.get("X-RateLimit-Remaining", 5000))
        limit = int(response.headers.get("X-RateLimit-Limit", 5000))
        reset_timestamp = int(response.headers.get("X-RateLimit-Reset", 0))

        # Log rate limit status
        logger.debug(f"Rate limit: {remaining}/{limit}")

        # If we're running low on requests, wait until reset
        if remaining < 10:
            reset_time = datetime.fromtimestamp(reset_timestamp)
            current_time = datetime.now()
            wait_seconds = (
                reset_time - current_time
            ).total_seconds() + 10  # Add 10s buffer

            if wait_seconds > 0:
                logger.warning(
                    f"Rate limit low ({remaining} remaining). "
                    f"Waiting {wait_seconds:.0f}s until reset at {reset_time}"
                )
                time.sleep(wait_seconds)

    def _paginate(self, url: str, params: Dict, max_pages: int = 10) -> List[Dict]:
        """
        Fetch all pages of results from GitHub API

        Args:
            url: API endpoint URL
            params: Query parameters
            max_pages: Maximum pages to fetch (safety limit)

        Returns:
            List of all items from all pages
        """
        all_items = []
        page = 1

        while page <= max_pages:
            # Add page number to params
            params["page"] = page

            logger.debug(f"Fetching page {page}: {url}")

            response = self._make_request(url, params)
            items = response.json()

            # If no items returned, we're done
            if not items:
                logger.debug(f"No more items on page {page}")
                break

            all_items.extend(items)
            logger.info(
                f"Fetched {len(items)} items from page {page} (total: {len(all_items)})"
            )

            # Check if there's a next page
            link_header = response.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                logger.debug("No next page, pagination complete")
                break

            page += 1

        if page > max_pages:
            logger.warning(f"Hit max_pages limit ({max_pages}), may have more data")

        return all_items

    def fetch_repository(self, owner: str, repo: str) -> Dict:
        """
        Fetch repository metadata

        Args:
            owner: Repository owner (e.g., 'apache')
            repo: Repository name (e.g., 'airflow')

        Returns:
            Repository metadata as dictionary
        """
        url = f"{self.base_url}/repos/{owner}/{repo}"

        logger.info(f"Fetching repository: {owner}/{repo}")

        response = self._make_request(url)
        repo_data = response.json()

        logger.info(
            f"Repository fetched: {repo_data['full_name']} "
            f"(stars: {repo_data['stargazers_count']}, "
            f"issues: {repo_data['open_issues_count']})"
        )

        return repo_data

    def fetch_issues(
        self, owner: str, repo: str, since: Optional[str] = None, max_pages: int = 10
    ) -> List[Dict]:
        """
        Fetch issues and pull requests (PRs are issues with pull_request field)

        Args:
            owner: Repository owner
            repo: Repository name
            since: ISO 8601 timestamp (e.g., '2026-02-02T00:00:00Z')
            max_pages: Maximum pages to fetch

        Returns:
            List of issue dictionaries
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/issues"

        params = {
            "state": "all",  # Get both open and closed
            "per_page": 100,  # Max allowed by GitHub
            "sort": "updated",
            "direction": "desc",
        }

        if since:
            params["since"] = since

        logger.info(
            f"Fetching issues for {owner}/{repo}" + (f" since {since}" if since else "")
        )

        issues = self._paginate(url, params, max_pages)

        # Count PRs vs regular issues
        pr_count = sum(1 for issue in issues if "pull_request" in issue)
        issue_count = len(issues) - pr_count

        logger.info(
            f"Fetched {len(issues)} total items: "
            f"{issue_count} issues, {pr_count} pull requests"
        )

        return issues

    def fetch_comments(
        self, owner: str, repo: str, since: Optional[str] = None, max_pages: int = 10
    ) -> List[Dict]:
        """
        Fetch issue comments

        Args:
            owner: Repository owner
            repo: Repository name
            since: ISO 8601 timestamp
            max_pages: Maximum pages to fetch

        Returns:
            List of comment dictionaries
        """
        url = f"{self.base_url}/repos/{owner}/{repo}/issues/comments"

        params = {"per_page": 100, "sort": "updated", "direction": "desc"}

        if since:
            params["since"] = since

        logger.info(
            f"Fetching comments for {owner}/{repo}"
            + (f" since {since}" if since else "")
        )

        comments = self._paginate(url, params, max_pages)

        logger.info(f"Fetched {len(comments)} comments")

        return comments


# ============================================
# GCS WRITER CLASS
# ============================================


class GCSWriter:
    """
    Writes JSON data to Google Cloud Storage with:
    - Date-based partitioning
    - Metadata injection
    - Atomic writes
    - Error handling
    """

    def __init__(self, bucket_name: str):
        """
        Initialize GCS writer

        Args:
            bucket_name: Name of GCS bucket (without gs:// prefix)
        """
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name

        logger.info(f"GCSWriter initialized for bucket: {bucket_name}")

    def write_json(
        self, data: List[Dict], entity_type: str, date: str, repo_name: str
    ) -> Optional[str]:
        """
        Write JSON data to GCS with partitioning and metadata

        Args:
            data: List of dictionaries to write
            entity_type: Type of entity ('issues', 'repositories', 'comments')
            date: Date string in YYYY-MM-DD format
            repo_name: Repository name in format 'owner_repo'

        Returns:
            GCS blob path where data was written

        Raises:
            Exception: If write fails
        """
        if not data:
            logger.warning(f"No data to write for {entity_type}/{repo_name}/{date}")
            return None

        # Create partitioned path: entity_type/YYYY/MM/DD/filename.json
        year, month, day = date.split("-")
        blob_name = (
            f"{entity_type}/{year}/{month}/{day}/"
            f"{repo_name}_{entity_type}_{date}.json"
        )

        # Add metadata to each item
        enriched_data = []
        for item in data:
            enriched_item = {
                **item,  # Original GitHub data
                "extracted_at": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "extraction_date": date,
                "source_file": blob_name,
                "source_repository": repo_name.replace("_", "/"),
            }
            enriched_data.append(enriched_item)

        # Convert to JSON
        json_content = json.dumps(enriched_data, indent=2, ensure_ascii=False)

        # Create blob and upload
        blob = self.bucket.blob(blob_name)

        try:
            blob.upload_from_string(json_content, content_type="application/json")

            logger.info(
                f"✓ Wrote {len(data)} records to gs://{self.bucket_name}/{blob_name} "
                f"({len(json_content)/1024:.1f} KB)"
            )

            return f"gs://{self.bucket_name}/{blob_name}"

        except Exception as e:
            logger.error(f"Failed to write to GCS: {blob_name} - {e}")
            raise


# ============================================
# HELPER FUNCTIONS
# ============================================


def get_secret(project_id: str, secret_id: str) -> str:
    """
    Retrieve secret from Google Secret Manager

    Args:
        project_id: GCP project ID
        secret_id: Secret name in Secret Manager

    Returns:
        Secret value as string
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        secret_value = response.payload.data.decode("UTF-8")

        # Clean the secret value
        # Remove: leading/trailing whitespace, quotes, -n flag
        secret_value = secret_value.strip()  # Remove \r\n, spaces
        secret_value = secret_value.strip('"')  # Remove quotes
        secret_value = secret_value.strip("'")  # Remove single quotes

        # Remove -n flag if present (from echo -n)
        if secret_value.startswith("-n "):
            secret_value = secret_value[3:].strip()

        logger.info(f"Retrieved secret: {secret_id}")
        return secret_value

    except Exception as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {e}")
        raise


def load_repository_config(config_path: str = "config/repositories.json") -> Dict:
    """
    Load repository configuration from JSON file

    Args:
        config_path: Path to repositories.json file

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, "r") as f:
            config = json.load(f)

        # Filter to only enabled repositories
        enabled_repos = [
            repo for repo in config["repositories"] if repo.get("enabled", True)
        ]

        logger.info(f"Loaded {len(enabled_repos)} enabled repositories from config")
        return {
            "repositories": enabled_repos,
            "extraction_config": config.get("extraction_config", {}),
        }

    except Exception as e:
        logger.error(f"Failed to load repository config: {e}")
        raise


# ============================================
# MAIN EXTRACTION PIPELINE
# ============================================


def main():
    """
    Main extraction pipeline orchestration

    Steps:
    1. Load configuration
    2. Get GitHub token (from env or Secret Manager)
    3. Initialize extractors
    4. For each repository:
       - Extract repository metadata
       - Extract issues
       - Extract comments
       - Write to GCS
    5. Log summary
    """

    logger.info("=" * 60)
    logger.info("GitHub Data Extraction Pipeline Starting")
    logger.info("=" * 60)

    # ----------------------------------------
    # 1. Load Configuration
    # ----------------------------------------

    project_id = os.getenv("GCP_PROJECT_ID")
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    extraction_date = os.getenv(
        "EXTRACTION_DATE",
        (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    debug = os.getenv("DEBUG", "false").lower() == "true"
    dry_run = os.getenv("DRY_RUN", "false").lower() == "true"

    if debug:
        logger.setLevel(logging.DEBUG)

    logger.info(f"Configuration:")
    logger.info(f"  Project ID: {project_id}")
    logger.info(f"  Bucket: {bucket_name}")
    logger.info(f"  Extraction Date: {extraction_date}")
    logger.info(f"  Dry Run: {dry_run}")

    # Load repository configuration
    config = load_repository_config()
    repositories = config["repositories"]
    extraction_config = config["extraction_config"]

    lookback_days = extraction_config.get("lookback_days", 7)
    max_issues_per_repo = extraction_config.get("max_issues_per_repo", 500)

    # Calculate 'since' timestamp (for incremental loading)
    since_date = datetime.strptime(extraction_date, "%Y-%m-%d") - timedelta(
        days=lookback_days
    )
    since_timestamp = since_date.isoformat() + "Z"

    logger.info(f"Extraction config:")
    logger.info(f"  Lookback days: {lookback_days}")
    logger.info(f"  Since: {since_timestamp}")
    logger.info(f"  Repositories to process: {len(repositories)}")

    # ----------------------------------------
    # 2. Get GitHub Token
    # ----------------------------------------

    # Try environment variable first (local dev), then Secret Manager (production)
    github_token = os.getenv("GITHUB_TOKEN")

    if not github_token and project_id:
        logger.info("GITHUB_TOKEN not in env, retrieving from Secret Manager")
        secret_name = os.getenv("SECRET_GITHUB_PAT", "github-pat")
        github_token = get_secret(project_id, secret_name)

    if not github_token:
        raise ValueError(
            "GitHub token not found. Set GITHUB_TOKEN in .env or "
            "ensure Secret Manager is configured."
        )

    logger.info("✓ GitHub token retrieved")

    # ----------------------------------------
    # 3. Initialize Extractors
    # ----------------------------------------

    extractor = GitHubExtractor(github_token)

    if not dry_run:
        # At this point bucket_name is guaranteed to be str (checked above)
        assert bucket_name is not None  # Type assertion for type checker
        writer = GCSWriter(bucket_name)
    else:
        logger.warning("DRY RUN MODE: No data will be written to GCS")
        writer = None

    # ----------------------------------------
    # 4. Extract Data for Each Repository
    # ----------------------------------------

    summary = {
        "total_repos": len(repositories),
        "successful_repos": 0,
        "failed_repos": 0,
        "total_issues": 0,
        "total_comments": 0,
        "files_written": [],
    }

    for repo_config in repositories:
        owner = repo_config["owner"]
        repo = repo_config["repo"]
        repo_full_name = f"{owner}/{repo}"
        repo_safe_name = f"{owner}_{repo}"

        logger.info("")
        logger.info("=" * 60)
        logger.info(f"Processing: {repo_full_name}")
        logger.info("=" * 60)

        try:
            # ---- Extract Repository Metadata ----
            repo_data = extractor.fetch_repository(owner, repo)

            if writer:
                blob_path = writer.write_json(
                    [repo_data],  # Wrap in list
                    "repositories",
                    extraction_date,
                    repo_safe_name,
                )
                summary["files_written"].append(blob_path)

            # ---- Extract Issues (includes PRs) ----
            issues = extractor.fetch_issues(
                owner,
                repo,
                since=since_timestamp,
                max_pages=max_issues_per_repo // 100,  # 100 items per page
            )

            summary["total_issues"] += len(issues)

            if issues and writer:
                blob_path = writer.write_json(
                    issues, "issues", extraction_date, repo_safe_name
                )
                summary["files_written"].append(blob_path)

            # ---- Extract Comments ----
            comments = extractor.fetch_comments(owner, repo, since=since_timestamp)

            summary["total_comments"] += len(comments)

            if comments and writer:
                blob_path = writer.write_json(
                    comments, "comments", extraction_date, repo_safe_name
                )
                summary["files_written"].append(blob_path)

            summary["successful_repos"] += 1
            logger.info(f"✓ Successfully processed {repo_full_name}")

        except Exception as e:
            summary["failed_repos"] += 1
            logger.error(f"✗ Failed to process {repo_full_name}: {e}", exc_info=True)
            # Continue to next repository
            continue

    # ----------------------------------------
    # 5. Log Summary
    # ----------------------------------------

    logger.info("")
    logger.info("=" * 60)
    logger.info("Extraction Complete - Summary")
    logger.info("=" * 60)
    logger.info(
        f"Repositories processed: {summary['successful_repos']}/{summary['total_repos']}"
    )
    logger.info(f"Failed repositories: {summary['failed_repos']}")
    logger.info(f"Total issues fetched: {summary['total_issues']}")
    logger.info(f"Total comments fetched: {summary['total_comments']}")
    logger.info(f"Files written to GCS: {len(summary['files_written'])}")

    if summary["files_written"]:
        logger.info("\nFiles written:")
        for file_path in summary["files_written"]:
            logger.info(f"  - {file_path}")

    if summary["failed_repos"] > 0:
        logger.warning(
            f"\n⚠️  {summary['failed_repos']} repositories failed. Check logs above."
        )
        return 1  # Exit code 1 indicates partial failure

    logger.info("\n✅ All repositories processed successfully!")
    return 0  # Exit code 0 indicates success


# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    import sys

    exit_code = main()
    sys.exit(exit_code)


### ---

## Part 2: Understanding the Code Structure

### 2.1 Code Architecture Overview

### Let me explain what we just built:

### **Class 1: GitHubExtractor**
### ```
### Purpose: Talk to GitHub API
### Methods:
### ├── __init__() ─────────── Store token, set up headers
### ├── _make_request() ────── HTTP calls with retry logic
### ├──  _check_rate_limit() ── Monitor and respect API limits
### ├── _paginate() ────────── Handle multi-page results
### ├── fetch_repository() ─── Get repo metadata
### ├── fetch_issues() ─────── Get issues (includes PRs)
### └── fetch_comments() ───── Get issue comments
### ```

### **Class 2: GCSWriter**
### ```
### Purpose: Write to Google Cloud Storage
### Methods:
### ├── __init__() ──────── Connect to GCS bucket
### └── write_json() ────── Write with partitioning + metadata
### ```

### **Functions:**
### ```
### Helper Functions:
### ├── get_secret() ────────── Retrieve from Secret Manager
### ├── load_repository_config() ── Read repositories.json
### └── main() ──────────────── Orchestrate entire pipeline
### ````
