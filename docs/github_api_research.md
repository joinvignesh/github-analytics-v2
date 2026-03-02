# GitHub REST API Research & Strategy

## API Authentication

### GitHub Personal Access Token (PAT)
- **Type**: Fine-grained token (preferred) or Classic token
- **Permissions needed**:
  - Repository → Metadata: Read-only
  - Repository → Issues: Read-only
  - Repository → Pull requests: Read-only
  - Repository → Contents: Read-only
- **Rate limits**:
  - Unauthenticated: 60 requests/hour
  - Authenticated: 5,000 requests/hour
  - GraphQL: 5,000 points/hour

### Authentication Header
```
Authorization: Bearer ghp_xxxxxxxxxxxxx
Accept: application/vnd.github+json
X-GitHub-Api-Version: 2022-11-28
```

---

## Endpoints We'll Use

### 1. Repository Metadata
**Endpoint:** `GET /repos/{owner}/{repo}`

**Example:** `https://api.github.com/repos/apache/airflow`

**Response (key fields):**
```json
{
  "id": 123456,
  "name": "airflow",
  "full_name": "apache/airflow",
  "description": "Apache Airflow...",
  "created_at": "2015-04-13T17:24:47Z",
  "updated_at": "2026-02-09T...",
  "pushed_at": "2026-02-09T...",
  "stargazers_count": 35000,
  "watchers_count": 35000,
  "forks_count": 14000,
  "open_issues_count": 850,
  "language": "Python",
  "license": { "name": "Apache License 2.0" }
}
```

**Why we need this:**
- Track repository growth over time
- Dimension table (repository attributes)
- Context for issues/PRs

---

### 2. Issues (includes Pull Requests!)
**Endpoint:** `GET /repos/{owner}/{repo}/issues`

**Important:** In GitHub's API, Pull Requests are a special type of Issue!
- Issues with a `pull_request` field → PRs
- Issues without → Regular issues

**Parameters:**
- `state`: `all` (get both open and closed)
- `since`: ISO 8601 timestamp (e.g., `2026-02-02T00:00:00Z`)
- `per_page`: 100 (max allowed)
- `page`: 1, 2, 3... (for pagination)
- `sort`: `updated` (most recently updated first)
- `direction`: `desc`

**Example:** 
```
GET /repos/apache/airflow/issues?state=all&since=2026-02-02T00:00:00Z&per_page=100&page=1
```

**Response (key fields):**
```json
[
  {
    "id": 987654,
    "number": 1234,
    "title": "Bug in scheduler",
    "state": "open",
    "user": {
      "login": "contributor123",
      "id": 55555
    },
    "created_at": "2026-02-05T10:30:00Z",
    "updated_at": "2026-02-09T14:22:00Z",
    "closed_at": null,
    "comments": 5,
    "pull_request": {  // ← This field indicates it's a PR!
      "url": "..."
    },
    "labels": [
      {"name": "bug"},
      {"name": "priority-high"}
    ]
  }
]
```

**Why we need this:**
- Core fact table (issue activity)
- Understand project health
- Track issue resolution time
- Separate PRs from issues in dbt

---

### 3. Issue Comments
**Endpoint:** `GET /repos/{owner}/{repo}/issues/comments`

**Parameters:**
- `since`: ISO 8601 timestamp
- `per_page`: 100
- `page`: 1, 2, 3...
- `sort`: `updated`
- `direction`: `desc`

**Example:**
```
GET /repos/apache/airflow/issues/comments?since=2026-02-02T00:00:00Z&per_page=100
```

**Response (key fields):**
```json
[
  {
    "id": 111222,
    "user": {
      "login": "reviewer456",
      "id": 66666
    },
    "created_at": "2026-02-09T09:15:00Z",
    "updated_at": "2026-02-09T09:15:00Z",
    "body": "This looks good, but...",
    "issue_url": "https://api.github.com/repos/apache/airflow/issues/1234"
  }
]
```

**Why we need this:**
- Community engagement metrics
- Contributor activity
- Discussion patterns

---

## Pagination Strategy

### The Problem
A repo with 500 issues can't return all in one request (max 100/page).

### The Solution: Link Header Pagination

GitHub returns a `Link` header:
```
Link: <https://api.github.com/repos/.../issues?page=2>; rel="next",
      <https://api.github.com/repos/.../issues?page=5>; rel="last"
```

**Our Strategy:**
1. Make request to page 1
2. Check if `Link` header contains `rel="next"`
3. If yes, extract next page URL and repeat
4. If no, we're done

**Safety limit:** Stop after 10 pages (1000 items) for initial testing

---

## Rate Limiting Strategy

### How GitHub Rate Limiting Works

**Response Headers:**
```
X-RateLimit-Limit: 5000        # Total requests allowed per hour
X-RateLimit-Remaining: 4850    # Requests left in this hour
X-RateLimit-Reset: 1707484800  # Unix timestamp when limit resets
```

### Our Strategy

**Check on every request:**
```python
if int(response.headers.get('X-RateLimit-Remaining', 0)) < 10:
    reset_time = int(response.headers.get('X-RateLimit-Reset'))
    wait_seconds = reset_time - time.time()
    logger.warning(f"Rate limit low, waiting {wait_seconds}s")
    time.sleep(wait_seconds + 10)  # Add 10s buffer
```

**Why < 10 not < 1?**
- Leave buffer for other processes
- Avoid hitting limit exactly
- Account for concurrent requests

---

## Error Handling Strategy

### Common GitHub API Errors

| Status Code | Meaning | Our Response |
|-------------|---------|--------------|
| **200** | Success | Continue |
| **304** | Not Modified (cached) | Safe to continue |
| **401** | Unauthorized | Invalid token → FAIL FAST |
| **403** | Rate limit exceeded | Wait and retry |
| **404** | Not found | Log warning, skip repo |
| **422** | Validation failed | Bad parameters → FAIL FAST |
| **500** | Server error | Retry with backoff |
| **502/503** | Service unavailable | Retry with backoff |

### Retry Strategy: Exponential Backoff
```python
for attempt in range(3):
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        if attempt == 2:  # Last attempt
            raise
        wait = 2 ** attempt  # 1s, 2s, 4s
        logger.warning(f"Attempt {attempt+1} failed, waiting {wait}s")
        time.sleep(wait)
```

---

## Data Structure Planning

### What We'll Store

**For each entity type:**

**1. Original GitHub data** (preserve everything)
```json
{
  "id": 123,
  "title": "...",
  ...  // All original fields
}
```

**2. Our metadata** (added by extraction script)
```json
{
  "extracted_at": "2026-02-09T15:30:00Z",
  "extraction_date": "2026-02-09",
  "source_file": "issues/2026/02/09/apache_airflow_issues_2026-02-09.json",
  "source_repository": "apache/airflow"
}
```

**Combined structure:**
```json
{
  // Original GitHub data
  "id": 123,
  "title": "Bug in scheduler",
  "state": "open",
  ...
  
  // Our metadata
  "extracted_at": "2026-02-09T15:30:00Z",
  "extraction_date": "2026-02-09",
  "source_file": "issues/2026/02/09/apache_airflow_issues_2026-02-09.json",
  "source_repository": "apache/airflow"
}
```

---

## File Naming Convention

### Pattern
```
{entity_type}/{YYYY}/{MM}/{DD}/{owner}_{repo}_{entity_type}_{YYYY-MM-DD}.json
```

### Examples
```
issues/2026/02/09/apache_airflow_issues_2026-02-09.json
repositories/2026/02/09/apache_airflow_repositories_2026-02-09.json
comments/2026/02/09/dbt-labs_dbt-core_comments_2026-02-09.json
```

### Why This Structure?

**Benefits:**
1. **Date partitioning**: Snowflake can partition by date
2. **Idempotent**: Re-running same date overwrites (no duplicates)
3. **Queryable**: Easy to find specific date's data
4. **Scalable**: Can handle millions of files
5. **Standard**: Follows Hive partitioning convention

---

## Incremental Loading Strategy

### Initial Load
- Extract last 7 days of activity
- Reasonable volume (~500-2000 issues total)
- Fast initial testing

### Daily Incremental
- Extract only `since=yesterday`
- Much smaller volume (10-100 issues/day)
- Cost-efficient

### Backfill (Future)
- Can backfill by changing `since` parameter
- Process month by month
- Rate limit aware

---

## Repository Selection Strategy

### Criteria for Good Test Repositories

✅ **apache/airflow**
- Active (100+ issues/month)
- Related to data engineering
- Medium size (~800 open issues)
- Good PR activity

✅ **dbt-labs/dbt-core**
- Directly related to our pipeline
- Active community
- Good mix of issues/PRs
- Moderate size (~200 open issues)

❌ **Avoid (for now):**
- **kubernetes/kubernetes**: Too large (3000+ open issues)
- **torvalds/linux**: Massive history
- **facebook/react**: Too much volume

### Start Small, Scale Later
- Begin with 2 repos
- Verify pipeline works
- Add more repos once stable

---

## Testing Strategy

### Phase 1: Single API Call Test
```python
# Test authentication
response = requests.get(
    'https://api.github.com/repos/apache/airflow',
    headers={'Authorization': f'Bearer {token}'}
)
print(response.status_code)  # Should be 200
```

### Phase 2: Pagination Test
```python
# Test pagination on issues endpoint
# Verify we get multiple pages
```

### Phase 3: Full Extraction Test
```python
# Extract 1 repo, 7 days
# Verify files in GCS
# Check file sizes reasonable
```

---

## Expected Data Volumes

### apache/airflow (7 days)

| Entity | Estimated Count | File Size |
|--------|----------------|-----------|
| Repository | 1 | ~5 KB |
| Issues | ~50-100 | ~200-400 KB |
| Comments | ~100-200 | ~100-200 KB |

### Total per repo per day: ~500 KB

**For 2 repos × 30 days = ~30 MB/month**

**GCS cost**: $0.02/GB = **$0.0006/month** (negligible)

---

## Rate Limit Budget

### Calculation

**Per repository per day:**
- 1 request: Repository metadata
- ~1-2 requests: Issues (with pagination)
- ~1-2 requests: Comments (with pagination)
- **Total: ~4-5 requests per repo**

**For 2 repos:**
- ~10 requests per extraction
- Daily: 10 requests
- Monthly: 300 requests

**Rate limit available**: 5,000/hour
**Our usage**: 300/month = **0.4 requests/hour**

✅ **We're well under the limit!**

---

## Next Steps

1. Create test script to validate GitHub token
2. Test single API endpoint
3. Build extraction classes
4. Implement full pipeline