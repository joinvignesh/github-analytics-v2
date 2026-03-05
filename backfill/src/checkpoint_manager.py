"""
Checkpoint Manager for Backfill Resume Capability
Stores and retrieves processing state to enable fault-tolerant execution
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class CheckpointManager:
    """
    Manages checkpoints for resumable backfill processing

    Checkpoint structure:
    {
        "run_id": "backfill_20260227_123456",
        "started_at": "2026-02-27T12:34:56Z",
        "last_updated": "2026-02-27T14:22:10Z",
        "status": "in_progress",  # in_progress, completed, failed
        "repositories": {
            "apache/airflow": {
                "status": "in_progress",
                "last_processed_date": "2020-06-30",
                "issues_processed": 12450,
                "comments_processed": 35600,
                "last_issue_id": 98765,
                "chunks_completed": ["2015-04", "2015-05", ..., "2020-06"],
                "failed_chunks": []
            },
            "dbt-labs/dbt-core": {
                "status": "completed",
                ...
            }
        },
        "global_stats": {
            "total_issues": 25000,
            "total_comments": 70000,
            "total_api_calls": 5200,
            "total_gcs_uploads": 180
        }
    }
    """

    def __init__(self, config: Dict[str, Any], run_id: Optional[str] = None):
        self.config = config
        self.checkpoint_config = config.get("checkpoint", {})
        self.enabled = self.checkpoint_config.get("enabled", True)

        if not self.enabled:
            logger.info("Checkpointing is disabled")
            return

        # Generate or use provided run_id
        self.run_id = run_id or self._generate_run_id()

        # Setup checkpoint storage
        self.local_path = Path(
            self.checkpoint_config.get("local_path", "./checkpoints")
        )
        self.local_path.mkdir(parents=True, exist_ok=True)

        self.checkpoint_file = self.local_path / f"{self.run_id}.json"

        # Initialize or load checkpoint
        self.checkpoint_data = self._load_or_initialize()

        logger.info(f"Checkpoint Manager initialized. Run ID: {self.run_id}")

    def _generate_run_id(self) -> str:
        """Generate unique run ID"""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"backfill_{timestamp}"

    def _load_or_initialize(self) -> Dict[str, Any]:
        """Load existing checkpoint or initialize new one"""
        if self.checkpoint_file.exists():
            logger.info(f"Loading existing checkpoint: {self.checkpoint_file}")
            try:
                with open(self.checkpoint_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading checkpoint: {e}. Starting fresh.")

        logger.info("Initializing new checkpoint")
        return {
            "run_id": self.run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress",
            "repositories": {},
            "global_stats": {
                "total_issues": 0,
                "total_comments": 0,
                "total_repositories": 0,
                "total_api_calls": 0,
                "total_gcs_uploads": 0,
                "total_errors": 0,
            },
        }

    def save(self) -> None:
        """Save checkpoint to disk"""
        if not self.enabled:
            return

        self.checkpoint_data["last_updated"] = datetime.now(timezone.utc).isoformat()

        try:
            # Write to temp file first, then rename (atomic operation)
            temp_file = self.checkpoint_file.with_suffix(".tmp")
            with open(temp_file, "w") as f:
                json.dump(self.checkpoint_data, f, indent=2)

            temp_file.replace(self.checkpoint_file)
            logger.debug(f"Checkpoint saved: {self.checkpoint_file}")

        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    def init_repository(
        self, repo_full_name: str, start_date: str, end_date: str
    ) -> None:
        """Initialize checkpoint for a repository"""
        if not self.enabled:
            return

        if repo_full_name not in self.checkpoint_data["repositories"]:
            self.checkpoint_data["repositories"][repo_full_name] = {
                "status": "not_started",
                "start_date": start_date,
                "end_date": end_date,
                "last_processed_date": None,
                "issues_processed": 0,
                "comments_processed": 0,
                "repositories_processed": 0,
                "last_issue_id": None,
                "chunks_completed": [],
                "failed_chunks": [],
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": None,
            }
            self.save()

    def mark_repository_started(self, repo_full_name: str) -> None:
        """Mark repository processing as started"""
        if not self.enabled:
            return

        if repo_full_name in self.checkpoint_data["repositories"]:
            self.checkpoint_data["repositories"][repo_full_name][
                "status"
            ] = "in_progress"
            self.checkpoint_data["repositories"][repo_full_name]["started_at"] = (
                datetime.now(timezone.utc).isoformat()
            )
            self.save()

    def mark_chunk_completed(
        self,
        repo_full_name: str,
        chunk_id: str,
        issues_count: int = 0,
        comments_count: int = 0,
    ) -> None:
        """Mark a time chunk as completed"""
        if not self.enabled:
            return

        repo_data = self.checkpoint_data["repositories"].get(repo_full_name)
        if not repo_data:
            return

        # Add to completed chunks
        if chunk_id not in repo_data["chunks_completed"]:
            repo_data["chunks_completed"].append(chunk_id)

        # Update counters
        repo_data["issues_processed"] += issues_count
        repo_data["comments_processed"] += comments_count
        repo_data["last_processed_date"] = chunk_id

        # Update global stats
        self.checkpoint_data["global_stats"]["total_issues"] += issues_count
        self.checkpoint_data["global_stats"]["total_comments"] += comments_count

        # Save every N chunks
        save_interval = self.checkpoint_config.get("save_interval", 500)
        if repo_data["issues_processed"] % save_interval < issues_count:
            self.save()

    def mark_chunk_failed(self, repo_full_name: str, chunk_id: str, error: str) -> None:
        """Mark a time chunk as failed"""
        if not self.enabled:
            return

        repo_data = self.checkpoint_data["repositories"].get(repo_full_name)
        if not repo_data:
            return

        repo_data["failed_chunks"].append(
            {
                "chunk_id": chunk_id,
                "error": str(error),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

        self.checkpoint_data["global_stats"]["total_errors"] += 1
        self.save()

    def mark_repository_completed(self, repo_full_name: str) -> None:
        """Mark repository processing as completed"""
        if not self.enabled:
            return

        repo_data = self.checkpoint_data["repositories"].get(repo_full_name)
        if not repo_data:
            return

        repo_data["status"] = "completed"
        repo_data["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.save()

        logger.info(
            f"Repository {repo_full_name} completed: "
            f"{repo_data['issues_processed']} issues, "
            f"{repo_data['comments_processed']} comments"
        )

    def mark_repository_failed(self, repo_full_name: str, error: str) -> None:
        """Mark repository processing as failed"""
        if not self.enabled:
            return

        repo_data = self.checkpoint_data["repositories"].get(repo_full_name)
        if not repo_data:
            return

        repo_data["status"] = "failed"
        repo_data["error"] = str(error)
        repo_data["failed_at"] = datetime.now(timezone.utc).isoformat()
        self.save()

    def get_repository_status(self, repo_full_name: str) -> Optional[Dict[str, Any]]:
        """Get checkpoint status for a repository"""
        return self.checkpoint_data["repositories"].get(repo_full_name)

    def should_process_chunk(self, repo_full_name: str, chunk_id: str) -> bool:
        """Check if a chunk needs to be processed"""
        if not self.enabled:
            return True

        repo_data = self.checkpoint_data["repositories"].get(repo_full_name)
        if not repo_data:
            return True

        return chunk_id not in repo_data["chunks_completed"]

    def get_last_processed_date(self, repo_full_name: str) -> Optional[str]:
        """Get the last processed date for a repository"""
        repo_data = self.checkpoint_data["repositories"].get(repo_full_name)
        if not repo_data:
            return None

        return repo_data.get("last_processed_date")

    def update_stats(
        self, api_calls: int = 0, gcs_uploads: int = 0, errors: int = 0
    ) -> None:
        """Update global statistics"""
        if not self.enabled:
            return

        stats = self.checkpoint_data["global_stats"]
        stats["total_api_calls"] += api_calls
        stats["total_gcs_uploads"] += gcs_uploads
        stats["total_errors"] += errors

    def mark_completed(self) -> None:
        """Mark entire backfill run as completed"""
        if not self.enabled:
            return

        self.checkpoint_data["status"] = "completed"
        self.checkpoint_data["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.save()

        logger.info(f"Backfill run {self.run_id} completed successfully")
        self._print_summary()

    def mark_failed(self, error: str) -> None:
        """Mark entire backfill run as failed"""
        if not self.enabled:
            return

        self.checkpoint_data["status"] = "failed"
        self.checkpoint_data["error"] = str(error)
        self.checkpoint_data["failed_at"] = datetime.now(timezone.utc).isoformat()
        self.save()

    def _print_summary(self) -> None:
        """Print summary of backfill run"""
        stats = self.checkpoint_data["global_stats"]
        repos = self.checkpoint_data["repositories"]

        completed_repos = sum(1 for r in repos.values() if r["status"] == "completed")
        failed_repos = sum(1 for r in repos.values() if r["status"] == "failed")

        logger.info("=" * 60)
        logger.info("BACKFILL SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Run ID: {self.run_id}")
        logger.info(f"Started: {self.checkpoint_data['started_at']}")
        logger.info(f"Completed: {self.checkpoint_data.get('completed_at', 'N/A')}")
        logger.info(f"Status: {self.checkpoint_data['status']}")
        logger.info("")
        logger.info(f"Repositories:")
        logger.info(f"  Total: {len(repos)}")
        logger.info(f"  Completed: {completed_repos}")
        logger.info(f"  Failed: {failed_repos}")
        logger.info("")
        logger.info(f"Data Processed:")
        logger.info(f"  Issues: {stats['total_issues']:,}")
        logger.info(f"  Comments: {stats['total_comments']:,}")
        logger.info(f"  Repositories: {stats['total_repositories']:,}")
        logger.info("")
        logger.info(f"API Usage:")
        logger.info(f"  Total API calls: {stats['total_api_calls']:,}")
        logger.info(f"  GCS uploads: {stats['total_gcs_uploads']:,}")
        logger.info(f"  Errors: {stats['total_errors']:,}")
        logger.info("=" * 60)

    def get_resumable_repositories(self) -> list:
        """Get list of repositories that need to be processed or resumed"""
        resumable = []

        for repo_name, repo_data in self.checkpoint_data["repositories"].items():
            status = repo_data.get("status", "not_started")
            if status in ["not_started", "in_progress"]:
                resumable.append(repo_name)

        return resumable

    @classmethod
    def list_checkpoints(cls, checkpoint_dir: str = "./checkpoints") -> list:
        """List all available checkpoint files"""
        checkpoint_path = Path(checkpoint_dir)
        if not checkpoint_path.exists():
            return []

        checkpoints = []
        for file in checkpoint_path.glob("backfill_*.json"):
            try:
                with open(file, "r") as f:
                    data = json.load(f)
                    checkpoints.append(
                        {
                            "run_id": data["run_id"],
                            "file": str(file),
                            "started_at": data["started_at"],
                            "status": data["status"],
                            "repositories": len(data.get("repositories", {})),
                        }
                    )
            except Exception as e:
                logger.warning(f"Could not read checkpoint {file}: {e}")

        return sorted(checkpoints, key=lambda x: x["started_at"], reverse=True)
