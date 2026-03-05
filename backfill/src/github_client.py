"""
GitHub API Client with Advanced Retry Logic and Rate Limiting
"""

import time
import logging
import requests
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class GitHubAPIError(Exception):
    """Custom exception for GitHub API errors"""

    pass


class RateLimitExceeded(Exception):
    """Exception for rate limit exceeded"""

    pass


class GitHubClient:
    """
    Production-grade GitHub API client with:
    - Exponential backoff retry
    - Rate limit handling
    - Connection pooling
    - Request throttling
    """

    def __init__(self, token: str, config: Dict[str, Any]):
        self.token = token
        self.config = config
        self.base_url = config["github"]["api_base_url"]
        self.max_retries = config["github"]["max_retries"]
        self.retry_delay_base = config["github"]["retry_delay_base"]
        self.timeout = config["github"]["timeout"]

        # Rate limiting
        self.rate_limit_remaining = 5000
        self.rate_limit_reset = None
        self.request_count = 0

        # Setup session with connection pooling and retry
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy"""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],  # Changed from method_whitelist
            backoff_factor=self.retry_delay_base,
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=self.config["performance"]["connection_pool_size"],
            pool_maxsize=self.config["performance"]["connection_pool_size"],
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "GitHub-Analytics-Backfill/1.0",
            }
        )

        return session

    def _check_rate_limit(self) -> None:
        """Check and handle rate limiting"""
        self.request_count += 1

        # Check every N requests
        if self.request_count % self.config["rate_limit"]["check_interval"] == 0:
            rate_limit_info = self.get_rate_limit()
            self.rate_limit_remaining = rate_limit_info["remaining"]
            self.rate_limit_reset = rate_limit_info["reset"]

            logger.info(
                f"Rate Limit Status: {self.rate_limit_remaining} requests remaining. "
                f"Resets at {datetime.fromtimestamp(self.rate_limit_reset)}"
            )

            # Proactive backoff
            warning_threshold = self.config["rate_limit"]["warning_threshold"]
            critical_threshold = self.config["rate_limit"]["critical_threshold"]

            if self.rate_limit_remaining < critical_threshold:
                # Critical: Wait until reset
                wait_time = self.rate_limit_reset - time.time() + 10
                if wait_time > 0:
                    logger.warning(
                        f"Rate limit critical ({self.rate_limit_remaining} remaining). "
                        f"Waiting {wait_time:.0f} seconds until reset..."
                    )
                    time.sleep(wait_time)

            elif self.rate_limit_remaining < warning_threshold:
                # Warning: Slow down
                logger.warning(
                    f"Rate limit warning ({self.rate_limit_remaining} remaining). "
                    f"Slowing down requests..."
                )
                time.sleep(5)

    def get_rate_limit(self) -> Dict[str, int]:
        """Get current rate limit status"""
        try:
            response = self.session.get(
                f"{self.base_url}/rate_limit", timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            return {
                "limit": data["resources"]["core"]["limit"],
                "remaining": data["resources"]["core"]["remaining"],
                "reset": data["resources"]["core"]["reset"],
            }
        except Exception as e:
            logger.error(f"Error checking rate limit: {e}")
            # Return conservative estimate
            return {"limit": 5000, "remaining": 1000, "reset": int(time.time()) + 3600}

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request with error handling"""
        self._check_rate_limit()

        attempt = 0
        last_exception = None

        while attempt < self.max_retries:
            try:
                response = self.session.request(
                    method=method, url=url, timeout=self.timeout, **kwargs
                )

                # Handle rate limiting
                if response.status_code == 403:
                    if "rate limit" in response.text.lower():
                        logger.warning("Rate limit hit (403). Waiting for reset...")
                        self._handle_rate_limit_exceeded()
                        continue

                if response.status_code == 429:
                    logger.warning("Rate limit hit (429). Waiting for reset...")
                    self._handle_rate_limit_exceeded()
                    continue

                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                attempt += 1
                last_exception = e

                if attempt < self.max_retries:
                    delay = self.retry_delay_base**attempt
                    logger.warning(
                        f"Request failed (attempt {attempt}/{self.max_retries}): {e}. "
                        f"Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts")

        raise GitHubAPIError(f"Max retries exceeded: {last_exception}")

    def _handle_rate_limit_exceeded(self) -> None:
        """Handle rate limit exceeded"""
        rate_limit_info = self.get_rate_limit()
        reset_time = rate_limit_info["reset"]
        wait_time = reset_time - time.time() + 10  # Add 10s buffer

        if wait_time > 0:
            logger.warning(
                f"Rate limit exceeded. Waiting {wait_time:.0f} seconds until reset at "
                f"{datetime.fromtimestamp(reset_time)}"
            )
            time.sleep(wait_time)

        # Update rate limit info
        self.rate_limit_remaining = rate_limit_info["remaining"]
        self.rate_limit_reset = rate_limit_info["reset"]

    def get_repository(self, owner: str, repo: str) -> Dict[str, Any]:
        """Get repository metadata"""
        url = f"{self.base_url}/repos/{owner}/{repo}"
        response = self._make_request("GET", url)
        return response.json()

    def get_issues(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        since: Optional[str] = None,  # Make optional
        per_page: int = 100,
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        """Get issues for a repository"""
        url = f"{self.base_url}/repos/{owner}/{repo}/issues"

        params = {
            "state": state,
            "per_page": per_page,
            "page": page,
            "sort": "created",  # ← Sort by created_at
            "direction": "asc",  # ← Oldest first
        }

        # Only add 'since' if explicitly provided
        # (Don't use for historical backfill)
        if since:
            params["since"] = since

        response = self._make_request("GET", url, params=params)

        issues = response.json()

        # Filter out pull requests
        issues = [issue for issue in issues if "pull_request" not in issue]

        return issues

    def get_issue_comments(
        self,
        owner: str,
        repo: str,
        issue_number: int,
        per_page: int = 100,
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        """Get comments for a specific issue"""
        url = f"{self.base_url}/repos/{owner}/{repo}/issues/{issue_number}/comments"

        params = {
            "per_page": per_page,
            "page": page,
            "sort": "created",
            "direction": "asc",
        }

        response = self._make_request("GET", url, params=params)
        return response.json()

    def get_all_issue_comments(
        self,
        owner: str,
        repo: str,
        since: Optional[str] = None,
        per_page: int = 100,
        page: int = 1,
    ) -> List[Dict[str, Any]]:
        """Get all comments for a repository (more efficient than per-issue)"""
        url = f"{self.base_url}/repos/{owner}/{repo}/issues/comments"

        params = {
            "per_page": per_page,
            "page": page,
            "sort": "created",
            "direction": "asc",
        }

        if since:
            params["since"] = since

        response = self._make_request("GET", url, params=params)
        return response.json()

    def has_next_page(self, response: requests.Response) -> bool:
        """Check if there's a next page in pagination"""
        link_header = response.headers.get("Link", "")
        return 'rel="next"' in link_header

    def close(self) -> None:
        """Close the session"""
        self.session.close()
