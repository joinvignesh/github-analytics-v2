{{
    config(
        materialized='view',
        tags=['staging', 'github', 'comments']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('github_raw', 'github_comments') }}
),

parsed AS (
    SELECT
        -- Primary keys
        raw_data:id::NUMBER AS comment_id,
        raw_data:node_id::STRING AS node_id,
        
        -- Comment details
        raw_data:body::STRING AS body,
        LENGTH(raw_data:body::STRING) AS body_length,
        
        -- Author information
        raw_data:user.login::STRING AS author_username,
        raw_data:user.id::NUMBER AS author_id,
        raw_data:user.type::STRING AS author_type,

        -- Associated issue & repository
        raw_data:issue_url::STRING AS issue_url,
        
        -- Associated issue
        -- Extract issue number from URL like .../issues/1234
        REGEXP_SUBSTR(
            raw_data:issue_url::STRING, 
            '/issues/([0-9]+)', 
            1, 1, 'e', 1
        )::NUMBER AS issue_number,

        -- Extract repository from issue_url
        -- URL format: https://api.github.com/repos/{owner}/{repo}/issues/{issue_num}
        REGEXP_SUBSTR(
            raw_data:issue_url::STRING,
            'repos/([^/]+/[^/]+)/',
            1, 1, 'e', 1
        )::STRING AS repository_full_name,
        
        -- Timestamps
        raw_data:created_at::TIMESTAMP_NTZ AS created_at,
        raw_data:updated_at::TIMESTAMP_NTZ AS updated_at,
        
        -- URLs
        raw_data:html_url::STRING AS html_url,
        raw_data:url::STRING AS api_url,
        
        -- Reactions
        raw_data:reactions.total_count::NUMBER AS reaction_count,
        raw_data:reactions.plus1::NUMBER AS thumbs_up_count,
        raw_data:reactions.minus1::NUMBER AS thumbs_down_count,
        raw_data:reactions.laugh::NUMBER AS laugh_count,
        raw_data:reactions.hooray::NUMBER AS hooray_count,
        raw_data:reactions.confused::NUMBER AS confused_count,
        raw_data:reactions.heart::NUMBER AS heart_count,
        raw_data:reactions.rocket::NUMBER AS rocket_count,
        raw_data:reactions.eyes::NUMBER AS eyes_count,
        
        -- Metadata
        raw_data:source_repository::STRING AS repository,
        raw_data:extraction_date::DATE AS extraction_date,
        raw_data:author_association::STRING AS author_association,
        source_file,
        loaded_at
        
    FROM source
),

-- Add at the end, replacing the final SELECT
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY comment_id 
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM parsed
)

SELECT * EXCLUDE (row_num)
FROM deduped
WHERE row_num = 1

