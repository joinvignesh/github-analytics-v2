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
        self.validation_config = config.get("validation", {})
        self.enabled = self.validation_config.get("enabled", True)
        self.strict_mode = self.validation_config.get("strict_mode", False)

        self.required_fields = self.validation_config.get("required_fields", {})

    def validate_issues(self, issues: List[Dict[str, Any]]) -> bool:
        """Validate issues data"""
        if not self.enabled:
            return True

        if not issues:
            return True

        required = self.required_fields.get("issue", [])

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
        threshold = self.validation_config.get("quality_thresholds", {}).get(
            "max_null_percentage", 5
        )

        if null_pct > threshold:
            msg = f"Issues validation failed: {null_pct:.1f}% null fields (threshold: {threshold}%)"
            if self.strict_mode:
                raise ValueError(msg)
            else:
                logger.warning(msg)
                return False

        logger.debug(
            f"Issues validation passed: {total} records, {null_pct:.1f}% null fields"
        )
        return True

    def validate_comments(self, comments: List[Dict[str, Any]]) -> bool:
        """Validate comments data"""
        if not self.enabled:
            return True

        if not comments:
            return True

        required = self.required_fields.get("comment", [])

        total = len(comments)
        null_count = 0

        for comment in comments:
            for field in required:
                if field not in comment or comment[field] is None:
                    null_count += 1

        null_pct = (null_count / (total * len(required))) * 100 if total > 0 else 0
        threshold = self.validation_config.get("quality_thresholds", {}).get(
            "max_null_percentage", 5
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

        required = self.required_fields.get("repository", [])

        for field in required:
            if field not in repo_data or repo_data[field] is None:
                msg = f"Repository missing required field: {field}"
                if self.strict_mode:
                    raise ValueError(msg)
                else:
                    logger.warning(msg)
                    return False

        return True
