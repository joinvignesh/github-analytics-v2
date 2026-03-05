"""
Main Entry Point for Historical Backfill
"""

import sys
import logging
import yaml
import argparse
from pathlib import Path
from datetime import datetime, timezone
from google.cloud import secretmanager

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.github_client import GitHubClient
from src.gcs_writer import GCSWriter
from src.checkpoint_manager import CheckpointManager
from src.validators import DataValidator
from src.backfill_orchestrator import BackfillOrchestrator


def setup_logging(config: dict) -> None:
    """Configure logging"""
    log_level = config.get("monitoring", {}).get("log_level", "INFO")
    log_file = config.get("monitoring", {}).get("log_file", "./logs/backfill.log")

    # Replace {timestamp} placeholder
    log_file = log_file.replace(
        "{timestamp}", datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    )

    # Create logs directory
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )


def get_github_token(config: dict) -> str:
    """Retrieve GitHub token from Secret Manager"""
    project_id = config["gcp"]["project_id"]
    secret_name = config["github"]["secret_name"]

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    response = client.access_secret_version(request={"name": name})
    token = response.payload.data.decode("UTF-8")

    # Strip whitespace (newlines, spaces, etc.)
    return token.strip()  # ← ADD THIS LINE


def load_config(config_path: str) -> dict:
    """Load configuration from YAML"""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_repositories(repos_path: str) -> list:
    """Load repository list from YAML"""
    with open(repos_path, "r") as f:
        data = yaml.safe_load(f)
        return data.get("repositories", [])


def main():
    parser = argparse.ArgumentParser(description="Run GitHub historical data backfill")
    parser.add_argument(
        "--config",
        default="./config/backfill_config.yaml",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--repositories",
        default="./config/repositories.yaml",
        help="Path to repositories file",
    )
    parser.add_argument("--resume", help="Resume from checkpoint (provide run_id)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Run in dry-run mode (no GCS writes)"
    )

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    repositories = load_repositories(args.repositories)

    # Override dry-run if specified
    if args.dry_run:
        config["testing"]["dry_run"] = True

    # Setup logging
    setup_logging(config)
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("GitHub Historical Backfill")
    logger.info("=" * 60)
    logger.info(f"Repositories: {len(repositories)}")
    logger.info(f"Dry run: {config.get('testing', {}).get('dry_run', False)}")
    logger.info("=" * 60)

    try:
        # Initialize components
        github_token = get_github_token(config)
        github_client = GitHubClient(github_token, config)
        gcs_writer = GCSWriter(config)
        checkpoint_manager = CheckpointManager(config, run_id=args.resume)
        validator = DataValidator(config)

        # Create orchestrator
        orchestrator = BackfillOrchestrator(
            config=config,
            github_client=github_client,
            gcs_writer=gcs_writer,
            checkpoint_manager=checkpoint_manager,
            validator=validator,
        )

        # Run backfill
        orchestrator.run(repositories)

        logger.info("Backfill completed successfully!")
        return 0

    except KeyboardInterrupt:
        logger.warning("Backfill interrupted by user")
        return 130

    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        return 1

    finally:
        if "github_client" in locals():
            github_client.close()


if __name__ == "__main__":
    sys.exit(main())
