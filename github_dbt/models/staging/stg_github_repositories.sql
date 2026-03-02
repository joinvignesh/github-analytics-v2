{{
    config(
        materialized='view',
        tags=['staging', 'github']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('github_raw', 'github_repositories') }}
),

parsed AS (
    SELECT
        -- Primary keys
        raw_data:id::NUMBER AS repository_id,
        raw_data:node_id::STRING AS node_id,
        raw_data:name::STRING AS repository_name,
        raw_data:full_name::STRING AS full_name,
        
        -- Owner information
        raw_data:owner.login::STRING AS owner_username,
        raw_data:owner.id::NUMBER AS owner_id,
        raw_data:owner.type::STRING AS owner_type,
        
        -- Repository details
        raw_data:description::STRING AS description,
        raw_data:private::BOOLEAN AS is_private,
        raw_data:fork::BOOLEAN AS is_fork,
        raw_data:archived::BOOLEAN AS is_archived,
        raw_data:disabled::BOOLEAN AS is_disabled,
        
        -- Timestamps
        raw_data:created_at::TIMESTAMP_NTZ AS created_at,
        raw_data:updated_at::TIMESTAMP_NTZ AS updated_at,
        raw_data:pushed_at::TIMESTAMP_NTZ AS pushed_at,
        
        -- Metrics
        raw_data:stargazers_count::NUMBER AS stars_count,
        raw_data:watchers_count::NUMBER AS watchers_count,
        raw_data:forks_count::NUMBER AS forks_count,
        raw_data:open_issues_count::NUMBER AS open_issues_count,
        raw_data:size::NUMBER AS size_kb,
        
        -- Language and topics
        raw_data:language::STRING AS primary_language,
        raw_data:topics AS topics_json,
        ARRAY_SIZE(raw_data:topics) AS topics_count,
        
        -- License
        raw_data:license.name::STRING AS license_name,
        raw_data:license.key::STRING AS license_key,
        
        -- Settings
        raw_data:has_issues::BOOLEAN AS has_issues_enabled,
        raw_data:has_projects::BOOLEAN AS has_projects_enabled,
        raw_data:has_wiki::BOOLEAN AS has_wiki_enabled,
        raw_data:has_pages::BOOLEAN AS has_pages_enabled,
        raw_data:has_downloads::BOOLEAN AS has_downloads_enabled,
        
        -- URLs
        raw_data:html_url::STRING AS html_url,
        raw_data:homepage::STRING AS homepage_url,
        
        -- Default branch
        raw_data:default_branch::STRING AS default_branch,
        
        -- Metadata
        raw_data:extraction_date::DATE AS extraction_date,
        source_file,
        loaded_at
        
    FROM source
),

-- Add at the end, replacing the final SELECT
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY repository_id 
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM parsed
)

SELECT * EXCLUDE (row_num)
FROM deduped
WHERE row_num = 1