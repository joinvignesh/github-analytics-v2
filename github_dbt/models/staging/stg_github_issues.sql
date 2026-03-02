{{
    config(
        materialized='view',
        tags=['staging', 'github']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('github_raw', 'github_issues') }}
),

parsed AS (
    SELECT
        raw_data:id::NUMBER AS issue_id,
        raw_data:number::NUMBER AS issue_number,
        raw_data:node_id::STRING AS node_id,
        raw_data:title::STRING AS title,
        raw_data:state::STRING AS state,
        raw_data:locked::BOOLEAN AS is_locked,
        raw_data:user.login::STRING AS author_username,
        raw_data:user.id::NUMBER AS author_id,
        raw_data:user.type::STRING AS author_type,
        raw_data:assignee.login::STRING AS assignee_username,
        raw_data:assignees AS assignees_json,
        ARRAY_SIZE(raw_data:assignees) AS assignee_count,
        raw_data:labels AS labels_json,
        ARRAY_SIZE(raw_data:labels) AS label_count,
        raw_data:milestone.title::STRING AS milestone_title,
        raw_data:milestone.number::NUMBER AS milestone_number,
        raw_data:created_at::TIMESTAMP_NTZ AS created_at,
        raw_data:updated_at::TIMESTAMP_NTZ AS updated_at,
        raw_data:closed_at::TIMESTAMP_NTZ AS closed_at,
        raw_data:comments::NUMBER AS comment_count,
        CASE 
            WHEN raw_data:pull_request IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS is_pull_request,
        raw_data:html_url::STRING AS html_url,
        raw_data:url::STRING AS api_url,
        raw_data:body::STRING AS body,
        LENGTH(raw_data:body::STRING) AS body_length,
        raw_data:source_repository::STRING AS repository,
        raw_data:extraction_date::DATE AS extraction_date,
        source_file,
        loaded_at
    FROM source
),

-- Keep only the most recent snapshot of each issue
-- This gives us current state while raw table retains full history
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY issue_id 
            ORDER BY loaded_at DESC  -- most recent load wins
        ) AS row_num
    FROM parsed
)

SELECT * EXCLUDE (row_num)
FROM deduped
WHERE row_num = 1