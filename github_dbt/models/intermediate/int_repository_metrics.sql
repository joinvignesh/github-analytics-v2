{{
    config(
        materialized='table',
        tags=['intermediate', 'metrics']
    )
}}

WITH repos AS (
    SELECT * FROM {{ ref('stg_github_repositories') }}
),

calculated AS (
    SELECT
        *,
        
        -- Age calculations
        DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) AS age_in_days,
        DATEDIFF('month', created_at, CURRENT_TIMESTAMP()) AS age_in_months,
        DATEDIFF('year', created_at, CURRENT_TIMESTAMP()) AS age_in_years,
        
        -- Days since last push
        DATEDIFF('day', pushed_at, CURRENT_TIMESTAMP()) AS days_since_last_push,
        
        -- Activity status
        CASE
            WHEN DATEDIFF('day', pushed_at, CURRENT_TIMESTAMP()) < 7 THEN 'Very Active'
            WHEN DATEDIFF('day', pushed_at, CURRENT_TIMESTAMP()) < 30 THEN 'Active'
            WHEN DATEDIFF('day', pushed_at, CURRENT_TIMESTAMP()) < 90 THEN 'Moderate'
            WHEN DATEDIFF('day', pushed_at, CURRENT_TIMESTAMP()) < 365 THEN 'Low Activity'
            ELSE 'Inactive'
        END AS activity_status,
        
        -- Popularity tier
        CASE
            WHEN stars_count >= 10000 THEN 'Very Popular (10K+ stars)'
            WHEN stars_count >= 1000 THEN 'Popular (1K-10K stars)'
            WHEN stars_count >= 100 THEN 'Growing (100-1K stars)'
            ELSE 'Emerging (<100 stars)'
        END AS popularity_tier,
        
        -- Engagement rate (watchers / stars)
        CASE 
            WHEN stars_count > 0 
            THEN ROUND(watchers_count::FLOAT / stars_count::FLOAT, 4)
            ELSE 0
        END AS engagement_rate,
        
        -- Fork rate
        CASE 
            WHEN stars_count > 0 
            THEN ROUND(forks_count::FLOAT / stars_count::FLOAT, 4)
            ELSE 0
        END AS fork_rate,
        
        -- Issue density (open issues per star)
        CASE 
            WHEN stars_count > 0 
            THEN ROUND(open_issues_count::FLOAT / stars_count::FLOAT, 4)
            ELSE 0
        END AS issue_density
        
    FROM repos
)

SELECT * FROM calculated