{{ config(materialized='table', tags=['audit']) }}

WITH source AS (
    SELECT * FROM {{ source('github_raw', 'github_repositories') }}
),
parsed AS (
    SELECT
        raw_data:id::NUMBER AS repository_id,
        raw_data:name::STRING AS repository_name,
        raw_data,
        source_file,
        loaded_at
    FROM source
)
SELECT 
    *,
    'Missing Repository ID' AS rejection_reason,
    CURRENT_TIMESTAMP() AS rejected_at
FROM parsed
WHERE repository_id IS NULL