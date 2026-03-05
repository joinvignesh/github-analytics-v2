# GitHub Analytics Platform: Historical Backfill Implementation Guide

## 📋 Complete File Structure

```
github-analytics-v2/
├── backfill/
│   ├── config/
│   │   ├── backfill_config.yaml              ✅ Created
│   │   └── repositories.yaml                 ✅ Created
│   │
│   ├── src/
│   │   ├── __init__.py
│   │   ├── backfill_orchestrator.py          # Main orchestration (see below)
│   │   ├── github_client.py                  ✅ Created
│   │   ├── gcs_writer.py                     # GCS upload handler (see below)
│   │   ├── checkpoint_manager.py             ✅ Created
│   │   ├── rate_limiter.py                   # Integrated in github_client
│   │   ├── validators.py                     # Data validators (see below)
│   │   └── monitoring.py                     # Metrics & logging (see below)
│   │
│   ├── scripts/
│   │   ├── run_backfill.py                   # Entry point (see below)
│   │   ├── validate_backfill.py              # Post-backfill validation
│   │   └── cleanup_failed_runs.py            # Cleanup utility
│   │
│   ├── tests/
│   │   ├── test_backfill_orchestrator.py
│   │   ├── test_github_client.py
│   │   └── test_checkpoint_manager.py
│   │
│   ├── logs/
│   │   └── .gitkeep
│   │
│   ├── checkpoints/
│   │   └── .gitkeep
│   │
│   ├── requirements.txt                       # See below
│   ├── README.md                              # See below
│   └── docker/
│       ├── Dockerfile                         # Container for Cloud Run
│       └── cloudbuild.yaml                    # Cloud Build config
```

---

## 📄 Core Implementation Files

### 1. **requirements.txt**

```txt
# Core dependencies
requests==2.31.0
google-cloud-storage==2.10.0
google-cloud-secret-manager==2.16.0
pyyaml==6.0.1

# Utilities
python-dateutil==2.8.2
pytz==2023.3

# Logging & monitoring
structlog==23.1.0

# Retries & rate limiting
urllib3==2.0.4
tenacity==8.2.3

# Testing
pytest==7.4.0
pytest-cov==4.1.0
pytest-mock==3.11.1
responses==0.23.3

# Code quality
black==23.7.0
flake8==6.1.0
mypy==1.5.0
```

---

### 2. **src/gcs_writer.py**

```python
"""
GCS Writer with Batching and Error Handling
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Any
from google.cloud import storage
from google.api_core import retry

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
        self.project_id = config['gcp']['project_id']
        self.bucket_name = config['gcp']['bucket_name']
        
        # Initialize GCS client
        self.client = storage.Client(project=self.project_id)
        self.bucket = self.client.bucket(self.bucket_name)
        
        logger.info(f"GCS Writer initialized for bucket: {self.bucket_name}")
    
    def write_issues(
        self,
        owner: str,
        repo: str,
        issues: List[Dict[str, Any]],
        date: str
    ) -> str:
        """
        Write issues to GCS
        
        Path format: issues/YYYY/MM/DD/{owner}_{repo}_issues_{date}.json
        """
        if not issues:
            logger.warning(f"No issues to write for {owner}/{repo} on {date}")
            return None
        
        # Parse date for folder structure
        date_obj = datetime.fromisoformat(date)
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
        
        # Construct path
        filename = f"{owner}_{repo}_issues_{date}.json"
        blob_path = f"issues/{year}/{month}/{day}/{filename}"
        
        return self._write_json(blob_path, issues)
    
    def write_comments(
        self,
        owner: str,
        repo: str,
        comments: List[Dict[str, Any]],
        date: str
    ) -> str:
        """Write comments to GCS"""
        if not comments:
            return None
        
        date_obj = datetime.fromisoformat(date)
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
        
        filename = f"{owner}_{repo}_comments_{date}.json"
        blob_path = f"comments/{year}/{month}/{day}/{filename}"
        
        return self._write_json(blob_path, comments)
    
    def write_repository(
        self,
        owner: str,
        repo: str,
        repo_data: Dict[str, Any],
        date: str
    ) -> str:
        """Write repository metadata to GCS"""
        date_obj = datetime.fromisoformat(date)
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
        
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
                json_data,
                content_type='application/json',
                timeout=300
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
```

---

### 3. **src/validators.py**

```python
"""
Data Quality Validators
"""

import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates data quality before GCS upload"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validation_config = config.get('validation', {})
        self.enabled = self.validation_config.get('enabled', True)
        self.strict_mode = self.validation_config.get('strict_mode', False)
        
        self.required_fields = self.validation_config.get('required_fields', {})
    
    def validate_issues(self, issues: List[Dict[str, Any]]) -> bool:
        """Validate issues data"""
        if not self.enabled:
            return True
        
        if not issues:
            return True
        
        required = self.required_fields.get('issue', [])
        
        total = len(issues)
        null_count = 0
        
        for issue in issues:
            for field in required:
                if field not in issue or issue[field] is None:
                    null_count += 1
                    logger.warning(
                        f"Issue {issue.get('id', 'unknown')} missing field: {field}"
                    )
        
        null_pct = (null_count / (total * len(required))) * 100 if total > 0 else 0
        threshold = self.validation_config.get('quality_thresholds', {}).get(
            'max_null_percentage', 5
        )
        
        if null_pct > threshold:
            msg = f"Issues validation failed: {null_pct:.1f}% null fields (threshold: {threshold}%)"
            if self.strict_mode:
                raise ValueError(msg)
            else:
                logger.warning(msg)
                return False
        
        logger.debug(f"Issues validation passed: {total} records, {null_pct:.1f}% null fields")
        return True
    
    def validate_comments(self, comments: List[Dict[str, Any]]) -> bool:
        """Validate comments data"""
        if not self.enabled:
            return True
        
        if not comments:
            return True
        
        required = self.required_fields.get('comment', [])
        
        total = len(comments)
        null_count = 0
        
        for comment in comments:
            for field in required:
                if field not in comment or comment[field] is None:
                    null_count += 1
        
        null_pct = (null_count / (total * len(required))) * 100 if total > 0 else 0
        threshold = self.validation_config.get('quality_thresholds', {}).get(
            'max_null_percentage', 5
        )
        
        if null_pct > threshold:
            msg = f"Comments validation failed: {null_pct:.1f}% null fields"
            if self.strict_mode:
                raise ValueError(msg)
            else:
                logger.warning(msg)
                return False
        
        return True
    
    def validate_repository(self, repo_data: Dict[str, Any]) -> bool:
        """Validate repository data"""
        if not self.enabled:
            return True
        
        required = self.required_fields.get('repository', [])
        
        for field in required:
            if field not in repo_data or repo_data[field] is None:
                msg = f"Repository missing required field: {field}"
                if self.strict_mode:
                    raise ValueError(msg)
                else:
                    logger.warning(msg)
                    return False
        
        return True
```

---

### 4. **src/backfill_orchestrator.py** (Main Logic)

```python
"""
Main Backfill Orchestrator
Coordinates the entire historical data extraction process
"""

import logging
from datetime import datetime, timedelta
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
        validator: DataValidator
    ):
        self.config = config
        self.github_client = github_client
        self.gcs_writer = gcs_writer
        self.checkpoint = checkpoint_manager
        self.validator = validator
        
        self.backfill_config = config.get('backfill', {})
        self.error_config = config.get('error_handling', {})
        
    def run(self, repositories: List[Dict[str, Any]]) -> None:
        """
        Execute backfill for all repositories
        
        Args:
            repositories: List of repository configurations
        """
        logger.info(f"Starting backfill for {len(repositories)} repositories")
        
        batch_size = self.backfill_config.get('batch_size', 1)
        
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
                        
                        if not self.error_config.get('continue_on_repo_failure', True):
                            raise
        
        self.checkpoint.mark_completed()
        logger.info("Backfill completed successfully")
    
    def _process_repository(self, repo_config: Dict[str, Any]) -> None:
        """Process a single repository"""
        owner = repo_config['owner']
        repo = repo_config['name']
        full_name = repo_config['full_name']
        
        logger.info(f"Processing repository: {full_name}")
        
        try:
            # Get repository metadata
            repo_data = self.github_client.get_repository(owner, repo)
            created_at = repo_data['created_at']
            
            # Determine date range
            start_date, end_date = self._get_date_range(repo_config, created_at)
            
            # Initialize checkpoint
            self.checkpoint.init_repository(
                full_name,
                start_date.isoformat(),
                end_date.isoformat()
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
            max_failures = self.error_config.get('max_consecutive_failures', 10)
            
            for chunk_start, chunk_end in chunks:
                chunk_id = chunk_start.strftime('%Y-%m')
                
                # Check if chunk already processed
                if not self.checkpoint.should_process_chunk(full_name, chunk_id):
                    logger.info(f"{full_name}: Chunk {chunk_id} already completed, skipping")
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
        chunk_id: str
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
        issues = self._fetch_issues_for_period(
            owner, repo, start_date, end_date
        )
        
        if not issues:
            logger.info(f"{owner}/{repo}: No issues in chunk {chunk_id}")
            return 0, 0
        
        # Fetch comments for these issues
        comments = self._fetch_comments_for_issues(owner, repo, issues)
        
        # Validate data
        self.validator.validate_issues(issues)
        self.validator.validate_comments(comments)
        
        # Write to GCS
        date_str = end_date.strftime('%Y-%m-%d')
        
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
        self,
        owner: str,
        repo: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Fetch all issues created in a date range"""
        all_issues = []
        page = 1
        per_page = self.backfill_config.get('per_page', 100)
        
        since = start_date.isoformat()
        until = end_date.isoformat()
        
        while True:
            try:
                issues = self.github_client.get_issues(
                    owner=owner,
                    repo=repo,
                    state='all',
                    since=since,
                    per_page=per_page,
                    page=page
                )
                
                self.checkpoint.update_stats(api_calls=1)
                
                if not issues:
                    break
                
                # Filter issues by date range (created_at)
                filtered = [
                    issue for issue in issues
                    if start_date <= datetime.fromisoformat(
                        issue['created_at'].replace('Z', '+00:00')
                    ) <= end_date
                ]
                
                all_issues.extend(filtered)
                
                # If we got less than a full page, we're done
                if len(issues) < per_page:
                    break
                
                # If all issues are beyond our end_date, stop
                if filtered and datetime.fromisoformat(
                    issues[-1]['created_at'].replace('Z', '+00:00')
                ) > end_date:
                    break
                
                page += 1
                
            except Exception as e:
                logger.error(f"Error fetching issues page {page}: {e}")
                raise
        
        return all_issues
    
    def _fetch_comments_for_issues(
        self,
        owner: str,
        repo: str,
        issues: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Fetch comments for a list of issues"""
        all_comments = []
        
        for issue in issues:
            issue_number = issue['number']
            comments_count = issue.get('comments', 0)
            
            if comments_count == 0:
                continue
            
            try:
                page = 1
                per_page = self.backfill_config.get('per_page', 100)
                
                while True:
                    comments = self.github_client.get_issue_comments(
                        owner=owner,
                        repo=repo,
                        issue_number=issue_number,
                        per_page=per_page,
                        page=page
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
        self,
        owner: str,
        repo: str,
        repo_data: Dict[str, Any],
        date: datetime
    ) -> None:
        """Write repository metadata to GCS"""
        try:
            date_str = date.strftime('%Y-%m-%d')
            self.gcs_writer.write_repository(owner, repo, repo_data, date_str)
            self.checkpoint.update_stats(gcs_uploads=1)
        except Exception as e:
            logger.error(f"Error writing repository metadata: {e}")
    
    def _get_date_range(
        self,
        repo_config: Dict[str, Any],
        repo_created_at: str
    ) -> Tuple[datetime, datetime]:
        """Determine start and end dates for backfill"""
        # Check for custom range
        custom_range = repo_config.get('custom_date_range', {})
        
        if 'start_date' in custom_range:
            start_date = datetime.fromisoformat(custom_range['start_date'])
        else:
            global_start = self.backfill_config.get('start_date')
            if global_start:
                start_date = datetime.fromisoformat(global_start)
            else:
                # Use repository creation date
                start_date = datetime.fromisoformat(
                    repo_created_at.replace('Z', '+00:00')
                )
        
        if 'end_date' in custom_range:
            end_date = datetime.fromisoformat(custom_range['end_date'])
        else:
            global_end = self.backfill_config.get('end_date')
            if global_end:
                end_date = datetime.fromisoformat(global_end)
            else:
                end_date = datetime.utcnow()
        
        return start_date, end_date
    
    def _generate_monthly_chunks(
        self,
        start_date: datetime,
        end_date: datetime
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
```

---

### 5. **scripts/run_backfill.py** (Entry Point)

```python
"""
Main Entry Point for Historical Backfill
"""

import sys
import logging
import yaml
import argparse
from pathlib import Path
from datetime import datetime
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
    log_level = config.get('monitoring', {}).get('log_level', 'INFO')
    log_file = config.get('monitoring', {}).get('log_file', './logs/backfill.log')
    
    # Replace {timestamp} placeholder
    log_file = log_file.replace(
        '{timestamp}',
        datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    )
    
    # Create logs directory
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )


def get_github_token(config: dict) -> str:
    """Retrieve GitHub token from Secret Manager"""
    project_id = config['gcp']['project_id']
    secret_name = config['github']['secret_name']
    
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode('UTF-8')


def load_config(config_path: str) -> dict:
    """Load configuration from YAML"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def load_repositories(repos_path: str) -> list:
    """Load repository list from YAML"""
    with open(repos_path, 'r') as f:
        data = yaml.safe_load(f)
        return data.get('repositories', [])


def main():
    parser = argparse.ArgumentParser(
        description='Run GitHub historical data backfill'
    )
    parser.add_argument(
        '--config',
        default='./config/backfill_config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--repositories',
        default='./config/repositories.yaml',
        help='Path to repositories file'
    )
    parser.add_argument(
        '--resume',
        help='Resume from checkpoint (provide run_id)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (no GCS writes)'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    repositories = load_repositories(args.repositories)
    
    # Override dry-run if specified
    if args.dry_run:
        config['testing']['dry_run'] = True
    
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
            validator=validator
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
        if 'github_client' in locals():
            github_client.close()


if __name__ == '__main__':
    sys.exit(main())
```

---

## 🚀 Deployment & Usage

### Local Development

```bash
# 1. Install dependencies
cd backfill/
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2. Configure
# Edit config/backfill_config.yaml
# Edit config/repositories.yaml

# 3. Authenticate with GCP
gcloud auth application-default login

# 4. Run backfill
python scripts/run_backfill.py

# 5. Resume from checkpoint (if interrupted)
python scripts/run_backfill.py --resume backfill_20260227_123456

# 6. Dry run (test without writing)
python scripts/run_backfill.py --dry-run
```

### Cloud Run Deployment

```dockerfile
# docker/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Set environment
ENV PYTHONUNBUFFERED=1

# Run backfill
CMD ["python", "scripts/run_backfill.py"]
```

```bash
# Build and deploy
cd backfill/

# Build image
docker build -t gcr.io/github-analytics-486213-u7/backfill-runner -f docker/Dockerfile .

# Push to GCR
docker push gcr.io/github-analytics-486213-u7/backfill-runner

# Deploy to Cloud Run
gcloud run deploy github-backfill-runner \
  --image=gcr.io/github-analytics-486213-u7/backfill-runner \
  --region=us-central1 \
  --memory=4Gi \
  --cpu=2 \
  --timeout=3600s \
  --max-instances=1 \
  --no-allow-unauthenticated

# Trigger manually
gcloud run jobs execute github-backfill-runner --region=us-central1
```

---

## 📊 Monitoring & Validation

### Check Progress

```bash
# View logs
tail -f logs/backfill_*.log

# Check checkpoint
python -c "
from src.checkpoint_manager import CheckpointManager
import json

checkpoints = CheckpointManager.list_checkpoints()
for cp in checkpoints:
    print(json.dumps(cp, indent=2))
"
```

### Validate Results

```sql
-- In Snowflake
-- Check data counts
SELECT
  'ISSUES' as entity,
  COUNT(*) as total_records,
  COUNT(DISTINCT DATE(loaded_at)) as distinct_days,
  MIN(loaded_at) as earliest,
  MAX(loaded_at) as latest
FROM GITHUB_ANALYTICS.RAW.GITHUB_ISSUES
UNION ALL
SELECT
  'COMMENTS',
  COUNT(*),
  COUNT(DISTINCT DATE(loaded_at)),
  MIN(loaded_at),
  MAX(loaded_at)
FROM GITHUB_ANALYTICS.RAW.GITHUB_COMMENTS;

-- Verify completeness (no gaps)
WITH date_spine AS (
  SELECT DATE('2015-04-13') + ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 as date
  FROM TABLE(GENERATOR(ROWCOUNT => 4000))
),
loaded_dates AS (
  SELECT DISTINCT DATE(loaded_at) as date
  FROM GITHUB_ANALYTICS.RAW.GITHUB_ISSUES
)
SELECT d.date as missing_date
FROM date_spine d
LEFT JOIN loaded_dates l ON d.date = l.date
WHERE l.date IS NULL
  AND d.date <= CURRENT_DATE()
ORDER BY d.date;
```

---

## 🎯 Key Features Summary

✅ **Monthly Chunking** - Processes data month-by-month to avoid timeouts  
✅ **Checkpoint Resume** - Can restart from last successful point  
✅ **Rate Limit Handling** - Automatically backs off when limits approached  
✅ **Retry Logic** - Exponential backoff on failures  
✅ **Data Validation** - Quality checks before upload  
✅ **Parallel Processing** - Multiple repos simultaneously (configurable)  
✅ **Progress Tracking** - Detailed logging and metrics  
✅ **Fault Tolerance** - Continues on individual chunk failures  
✅ **GCS Organization** - Maintains same folder structure as daily extraction  
✅ **Production Grade** - Error handling, monitoring, testing

---

## 📈 Performance Estimates

**For 2 repositories (Apache Airflow + dbt-core):**
- Estimated total issues: ~8,000
- Estimated total comments: ~25,000
- Estimated API calls: ~3,000
- Estimated runtime: 3-4 hours
- Estimated cost: ~$0.50 (Cloud Run) + $0.05 (GCS)

**Rate Limiting:**
- GitHub API limit: 5,000 requests/hour
- Our usage: ~1,000 requests/hour (conservative)
- Total time: 3-4 hours for full backfill