{{
    config(
        materialized='table',
        tags=['intermediate', 'metrics']
    )
}}

WITH issues AS (
    SELECT * FROM {{ ref('stg_github_issues') }}
),

calculated AS (
    SELECT
        *,
        
        -- Time to close calculation
        CASE 
            WHEN closed_at IS NOT NULL 
            THEN DATEDIFF('hour', created_at, closed_at)
            ELSE NULL
        END AS hours_to_close,
        
        CASE 
            WHEN closed_at IS NOT NULL 
            THEN DATEDIFF('day', created_at, closed_at)
            ELSE NULL
        END AS days_to_close,
        
        -- Current age
        DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) AS age_in_days,
        
        -- Categorize by resolution time
        CASE 
            WHEN closed_at IS NULL THEN 'Open'
            WHEN DATEDIFF('hour', created_at, closed_at) < 1 THEN 'Closed <1 hour'
            WHEN DATEDIFF('hour', created_at, closed_at) < 24 THEN 'Closed <1 day'
            WHEN DATEDIFF('day', created_at, closed_at) < 7 THEN 'Closed <1 week'
            WHEN DATEDIFF('day', created_at, closed_at) < 30 THEN 'Closed <1 month'
            ELSE 'Closed >1 month'
        END AS resolution_time_bucket,
        
        -- Activity level
        CASE
            WHEN comment_count = 0 THEN 'No comments'
            WHEN comment_count BETWEEN 1 AND 5 THEN 'Low activity'
            WHEN comment_count BETWEEN 6 AND 20 THEN 'Medium activity'
            ELSE 'High activity'
        END AS activity_level,
        
        -- Has assignee flag
        CASE 
            WHEN assignee_username IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_assignee,
        
        -- Has labels flag
        CASE 
            WHEN label_count > 0 THEN TRUE
            ELSE FALSE
        END AS has_labels,
        
        -- Has milestone flag
        CASE 
            WHEN milestone_title IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS has_milestone,
        
        -- Issue type
        CASE 
            WHEN is_pull_request THEN 'Pull Request'
            ELSE 'Issue'
        END AS issue_type,
        
        -- Title length category
        CASE
            WHEN LENGTH(title) < 30 THEN 'Short'
            WHEN LENGTH(title) < 80 THEN 'Medium'
            ELSE 'Long'
        END AS title_length_category
        
    FROM issues
)

SELECT * FROM calculated