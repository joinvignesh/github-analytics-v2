{{
    config(
        materialized='table',
        tags=['marts', 'core', 'dimension']
    )
}}

WITH repos AS (
    SELECT * FROM {{ ref('int_repository_metrics') }}
),

latest_snapshot AS (
    SELECT
        -- Primary key
        repository_id,
        
        -- Repository identification
        repository_name,
        full_name,
        owner_username,
        owner_type,
        
        -- Description
        description,
        
        -- Classification
        primary_language,
        topics_count,
        license_name,
        
        -- Flags
        is_private,
        is_fork,
        is_archived,
        is_disabled,
        
        -- Features enabled
        has_issues_enabled,
        has_projects_enabled,
        has_wiki_enabled,
        has_pages_enabled,
        has_downloads_enabled,
        
        -- Current metrics
        stars_count,
        forks_count,
        watchers_count,
        open_issues_count,
        size_kb,
        
        -- Calculated metrics
        popularity_tier,
        engagement_rate,
        fork_rate,
        issue_density,
        activity_status,
        
        -- Age metrics
        age_in_days,
        age_in_months,
        age_in_years,
        days_since_last_push,
        
        -- Important dates
        created_at,
        updated_at,
        pushed_at,
        
        -- URLs
        html_url,
        homepage_url,
        
        -- Technical
        default_branch,
        
        -- Metadata
        extraction_date,
        loaded_at,
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        
        -- SCD Type 2 preparation (if needed later)
        ROW_NUMBER() OVER (PARTITION BY repository_id ORDER BY loaded_at DESC) AS row_num
        
    FROM repos
)

SELECT * FROM latest_snapshot
WHERE row_num = 1  -- Get only most recent snapshot