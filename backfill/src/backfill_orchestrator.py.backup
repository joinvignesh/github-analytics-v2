"""
Main Backfill Orchestrator
Coordinates the entire historical data extraction process
"""

import logging
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from .github_client import GitHubClient
from .gcs_writer import GCSWriter
from .checkpoint_manager import CheckpointManager
from .validators import DataValidator

logger = logging.getLogger(__name__)


class BackfillOrchestrator:
    """
    Orchestrates historical data backfill with:
    - Monthly chunking for manageability
    - Parallel repository processing
    - Checkpoint-based resume
    - Comprehensive error handling
    """

    def __init__(
        self,
        config: Dict[str, Any],
        github_client: GitHubClient,
        gcs_writer: GCSWriter,
        checkpoint_manager: CheckpointManager,
        validator: DataValidator,
    ):
        self.config = config
        self.github_client = github_client
        self.gcs_writer = gcs_writer
        self.checkpoint = checkpoint_manager
        self.validator = validator

        self.backfill_config = config.get("backfill", {})
        self.error_config = config.get("error_handling", {})

    def run(self, repositories: List[Dict[str, Any]]) -> None:
        """
        Execute backfill for all repositories

        Args:
            repositories: List of repository configurations
        """
        logger.info(f"Starting backfill for {len(repositories)} repositories")

        batch_size = self.backfill_config.get("batch_size", 1)

        if batch_size == 1:
            # Sequential processing
            for repo_config in repositories:
                self._process_repository(repo_config)
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                futures = {
                    executor.submit(self._process_repository, repo): repo
                    for repo in repositories
                }

                for future in as_completed(futures):
                    repo = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Repository {repo['full_name']} failed: {e}")

                        if not self.error_config.get("continue_on_repo_failure", True):
                            raise

        self.checkpoint.mark_completed()
        logger.info("Backfill completed successfully")

    def _process_repository(self, repo_config: Dict[str, Any]) -> None:
        """Process a single repository"""
        owner = repo_config["owner"]
        repo = repo_config["name"]
        full_name = repo_config["full_name"]

        logger.info(f"Processing repository: {full_name}")

        try:
            # Get repository metadata
            repo_data = self.github_client.get_repository(owner, repo)
            created_at = repo_data["created_at"]

            # Determine date range
            start_date, end_date = self._get_date_range(repo_config, created_at)

            # Initialize checkpoint
            self.checkpoint.init_repository(
                full_name, start_date.isoformat(), end_date.isoformat()
            )
            self.checkpoint.mark_repository_started(full_name)

            # Generate monthly chunks
            chunks = self._generate_monthly_chunks(start_date, end_date)

            logger.info(
                f"{full_name}: Processing {len(chunks)} monthly chunks "
                f"from {start_date.date()} to {end_date.date()}"
            )

            # Process each chunk
            consecutive_failures = 0
            max_failures = self.error_config.get("max_consecutive_failures", 10)

            for chunk_start, chunk_end in chunks:
                chunk_id = chunk_start.strftime("%Y-%m")

                # Check if chunk already processed
                if not self.checkpoint.should_process_chunk(full_name, chunk_id):
                    logger.info(
                        f"{full_name}: Chunk {chunk_id} already completed, skipping"
                    )
                    continue

                try:
                    issues_count, comments_count = self._process_chunk(
                        owner, repo, chunk_start, chunk_end, chunk_id
                    )

                    self.checkpoint.mark_chunk_completed(
                        full_name, chunk_id, issues_count, comments_count
                    )

                    consecutive_failures = 0  # Reset on success

                except Exception as e:
                    consecutive_failures += 1
                    logger.error(
                        f"{full_name}: Chunk {chunk_id} failed "
                        f"(attempt {consecutive_failures}/{max_failures}): {e}"
                    )

                    self.checkpoint.mark_chunk_failed(full_name, chunk_id, str(e))

                    if consecutive_failures >= max_failures:
                        raise Exception(
                            f"Max consecutive failures ({max_failures}) reached"
                        )

            # Write repository metadata
            self._write_repository_metadata(owner, repo, repo_data, end_date)

            self.checkpoint.mark_repository_completed(full_name)

        except Exception as e:
            logger.error(f"Repository {full_name} failed: {e}")
            self.checkpoint.mark_repository_failed(full_name, str(e))
            raise

    def _process_chunk(
        self,
        owner: str,
        repo: str,
        start_date: datetime,
        end_date: datetime,
        chunk_id: str,
    ) -> Tuple[int, int]:
        """
        Process a time chunk (e.g., one month)

        Returns:
            Tuple of (issues_count, comments_count)
        """
        logger.info(
            f"{owner}/{repo}: Processing chunk {chunk_id} "
            f"({start_date.date()} to {end_date.date()})"
        )

        # Fetch issues for this time period
        issues = self._fetch_issues_for_period(owner, repo, start_date, end_date)

        if not issues:
            logger.info(f"{owner}/{repo}: No issues in chunk {chunk_id}")
            return 0, 0

        # Fetch comments for these issues
        comments = self._fetch_comments_for_issues(owner, repo, issues)

        # Validate data
        self.validator.validate_issues(issues)
        self.validator.validate_comments(comments)

        # Write to GCS
        date_str = end_date.strftime("%Y-%m-%d")

        if issues:
            self.gcs_writer.write_issues(owner, repo, issues, date_str)
            self.checkpoint.update_stats(gcs_uploads=1)

        if comments:
            self.gcs_writer.write_comments(owner, repo, comments, date_str)
            self.checkpoint.update_stats(gcs_uploads=1)

        logger.info(
            f"{owner}/{repo}: Chunk {chunk_id} completed - "
            f"{len(issues)} issues, {len(comments)} comments"
        )

        return len(issues), len(comments)

    def _fetch_issues_for_period(
        self, owner: str, repo: str, start_date: datetime, end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch all issues created in a date range"""
        all_issues = []
        page = 1
        per_page = self.backfill_config.get("per_page", 100)

        # DON'T use 'since' parameter for historical backfill
        # Filter by created_at after fetching

        while True:
            try:
                issues = self.github_client.get_issues(
                    owner=owner,
                    repo=repo,
                    state="all",
                    # since=since,  # ← REMOVE THIS
                    # until=until,  # ← GitHub API doesn't support 'until'
                    per_page=per_page,
                    page=page,
                )

                self.checkpoint.update_stats(api_calls=1)

                if not issues:
                    break

                # Filter by created_at in our code
                filtered = []
                for issue in issues:
                    created = datetime.fromisoformat(
                        issue["created_at"].replace("Z", "+00:00")
                    )

                    # Keep issues within our time window
                    if start_date <= created <= end_date:
                        filtered.append(issue)

                    # Stop if we've passed the end date
                    elif created > end_date:
                        break

                all_issues.extend(filtered)

                # Stop pagination if:
                # 1. Less than full page returned (last page)
                if len(issues) < per_page:
                    break

                # 2. All issues are beyond our date range
                last_created = datetime.fromisoformat(
                    issues[-1]["created_at"].replace("Z", "+00:00")
                )
                if last_created > end_date:
                    break

                page += 1

            except Exception as e:
                logger.error(f"Error fetching issues page {page}: {e}")
                raise

        logger.info(
            f"Fetched {len(all_issues)} issues created between "
            f"{start_date.date()} and {end_date.date()}"
        )

        return all_issues

    def _fetch_comments_for_issues(
        self, owner: str, repo: str, issues: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Fetch comments for a list of issues"""
        all_comments = []

        for issue in issues:
            issue_number = issue["number"]
            comments_count = issue.get("comments", 0)

            if comments_count == 0:
                continue

            try:
                page = 1
                per_page = self.backfill_config.get("per_page", 100)

                while True:
                    comments = self.github_client.get_issue_comments(
                        owner=owner,
                        repo=repo,
                        issue_number=issue_number,
                        per_page=per_page,
                        page=page,
                    )

                    self.checkpoint.update_stats(api_calls=1)

                    if not comments:
                        break

                    all_comments.extend(comments)

                    if len(comments) < per_page:
                        break

                    page += 1

            except Exception as e:
                logger.warning(
                    f"Error fetching comments for issue #{issue_number}: {e}"
                )
                # Continue with other issues
                continue

        return all_comments

    def _write_repository_metadata(
        self, owner: str, repo: str, repo_data: Dict[str, Any], date: datetime
    ) -> None:
        """Write repository metadata to GCS"""
        try:
            date_str = date.strftime("%Y-%m-%d")
            self.gcs_writer.write_repository(owner, repo, repo_data, date_str)
            self.checkpoint.update_stats(gcs_uploads=1)
        except Exception as e:
            logger.error(f"Error writing repository metadata: {e}")

    def _get_date_range(
        self, repo_config: Dict[str, Any], repo_created_at: str
    ) -> Tuple[datetime, datetime]:
        """Determine start and end dates for backfill"""
        # Check for custom range - ensure it's a dict, not None
        custom_range = repo_config.get("custom_date_range") or {}  # ← FIX

        if "start_date" in custom_range:
            start_date = datetime.fromisoformat(custom_range["start_date"])
        else:
            global_start = self.backfill_config.get("start_date")
            if global_start:
                start_date = datetime.fromisoformat(global_start)
            else:
                # Use repository creation date
                start_date = datetime.fromisoformat(
                    repo_created_at.replace("Z", "+00:00")
                )

        if "end_date" in custom_range:
            end_date = datetime.fromisoformat(custom_range["end_date"])
        else:
            global_end = self.backfill_config.get("end_date")
            if global_end:
                end_date = datetime.fromisoformat(global_end)
            else:
                end_date = datetime.now(timezone.utc)  # ← Also update this

        return start_date, end_date

    def _generate_monthly_chunks(
        self, start_date: datetime, end_date: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """Generate list of monthly date ranges"""
        chunks = []
        current = start_date

        while current < end_date:
            # Calculate end of current month
            next_month = current + relativedelta(months=1)
            chunk_end = min(next_month, end_date)

            chunks.append((current, chunk_end))
            current = next_month

        return chunks
