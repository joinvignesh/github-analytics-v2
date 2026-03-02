# GitHub Data Extraction Plan

## Extraction Scope

### Repositories
- apache/airflow
- dbt-labs/dbt-core

### Time Range
- **Initial load**: Last 7 days
- **Daily incremental**: Yesterday only

### Entities
1. Repository metadata (1 per repo per day)
2. Issues (includes PRs)
3. Issue comments

---

## Data Flow
```
GitHub API
    ↓
GitHubExtractor class
    ↓
JSON files (in memory)
    ↓
Add metadata
    ↓
GCSWriter class
    ↓
GCS Bucket (partitioned by date)
```

---

## File Output Structure

### Repository Metadata
```
gs://bucket/repositories/YYYY/MM/DD/owner_repo_repositories_YYYY-MM-DD.json
```

**Example:**
```
gs://github-raw-data-PROJECT/repositories/2026/02/09/apache_airflow_repositories_2026-02-09.json
```

**Content structure:**
```json
{
  "id": 123456,
  "name": "airflow",
  "full_name": "apache/airflow",
  ...original GitHub fields...,
  "extracted_at": "2026-02-09T15:30:00Z",
  "extraction_date": "2026-02-09",
  "source_file": "repositories/2026/02/09/apache_airflow_repositories_2026-02-09.json",
  "source_repository": "apache/airflow"
}
```

### Issues
```
gs://bucket/issues/YYYY/MM/DD/owner_repo_issues_YYYY-MM-DD.json
```

**Content**: Array of issue objects with metadata

### Comments
```
gs://bucket/comments/YYYY/MM/DD/owner_repo_comments_YYYY-MM-DD.json
```

---

## API Call Estimates

### Per Repository Per Day

| Endpoint | Calls | Reason |
|----------|-------|--------|
| Repository metadata | 1 | Single object |
| Issues | 1-3 | ~50-150 issues (last 7 days) = 1-2 pages |
| Comments | 1-3 | ~100-200 comments = 1-2 pages |
| **Total** | **3-7** | |

### For 2 Repositories
- **Daily**: 6-14 API calls
- **Monthly**: 180-420 API calls
- **Rate limit**: 5,000/hour

✅ **Well under limit!**

---

## Error Handling Requirements

1. **API failures**: Retry 3 times with exponential backoff
2. **Rate limits**: Check headers, wait if needed
3. **Invalid repos**: Log warning, continue to next
4. **Network issues**: Retry with timeout
5. **GCS write failures**: Log error, don't lose data

---

## Success Criteria

✅ All API endpoints return 200  
✅ Files written to correct GCS paths  
✅ Metadata fields present  
✅ No rate limit violations  
✅ Errors logged properly  
✅ Idempotent (re-running same date overwrites)  

---

## Next: Implementation

Now we build:
1. `GitHubExtractor` class
2. `GCSWriter` class
3. `main()` orchestration
4. Error handling
5. Logging