{{ config(materialized='table', tags=['audit']) }}

WITH source AS (
    SELECT * FROM {{ source('github_raw', 'github_comments') }}
),
parsed AS (
    SELECT
        raw_data:id::NUMBER AS comment_id,
        raw_data:created_at::TIMESTAMP_NTZ AS created_at,
        raw_data,
        source_file,
        loaded_at
    FROM source
)
SELECT 
    *,
    'Missing Comment ID' AS rejection_reason,
    CURRENT_TIMESTAMP() AS rejected_at
FROM parsed
WHERE comment_id IS NULL