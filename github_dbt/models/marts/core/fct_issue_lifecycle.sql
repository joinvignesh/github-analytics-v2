{{
    config(
        materialized='table',
        tags=['marts', 'core', 'fact', 'issue_lifecycle']
    )
}}

WITH issues AS (
    SELECT
        issue_id,
        issue_number,
        title,
        state,
        created_at,
        updated_at,
        closed_at,
        repository,
        is_pull_request,
        author_username,
        author_id,
        comment_count,
        labels_json,
        html_url
    FROM {{ ref('int_issue_metrics') }}
),

repos AS (
    SELECT
        repository_id,
        full_name AS repository_full_name,
        owner_username
    FROM {{ ref('int_repository_metrics') }}
    -- REMOVED: WHERE row_num = 1  ← This line was the problem
),

joined AS (
    SELECT
        -- Issue identifiers
        i.issue_id,
        i.issue_number,
        i.title,
        r.repository_id,
        i.repository AS repository_full_name,
        r.owner_username,
        
        -- Lifecycle timestamps
        i.created_at,
        i.updated_at,
        i.closed_at,
        
        -- Dates (for filtering)
        DATE(i.created_at) AS created_date,
        DATE(i.closed_at) AS closed_date,
        DATE(i.updated_at) AS updated_date,
        
        -- Status
        i.state,
        CASE 
            WHEN i.state = 'closed' THEN 'Closed'
            WHEN i.state = 'open' THEN 'Open'
            ELSE 'Unknown'
        END AS status_label,
        
        -- Lifecycle metrics
        CASE 
            WHEN i.closed_at IS NOT NULL 
            THEN DATEDIFF('day', i.created_at, i.closed_at)
            ELSE DATEDIFF('day', i.created_at, CURRENT_TIMESTAMP())
        END AS duration_days,
        
        CASE 
            WHEN i.closed_at IS NOT NULL 
            THEN DATEDIFF('hour', i.created_at, i.closed_at)
            ELSE DATEDIFF('hour', i.created_at, CURRENT_TIMESTAMP())
        END AS duration_hours,
        
        CASE 
            WHEN i.state = 'closed' THEN 'Completed'
            WHEN DATEDIFF('day', i.created_at, CURRENT_TIMESTAMP()) > 90 THEN 'Stale'
            WHEN DATEDIFF('day', i.created_at, CURRENT_TIMESTAMP()) > 30 THEN 'Long-running'
            ELSE 'Active'
        END AS lifecycle_stage,
        
        -- Metadata
        i.is_pull_request,
        i.author_username,
        i.author_id,
        i.comment_count,
        i.labels_json AS labels,  -- ← FIX: Change from labels to labels_json
        i.html_url,
        
        -- Generate surrogate key for uniqueness
        {{ dbt_utils.generate_surrogate_key(['i.issue_id']) }} AS issue_key
        
    FROM issues i
    LEFT JOIN repos r 
        ON i.repository = r.repository_full_name
)

SELECT * FROM joined