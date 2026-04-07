{{
    config(
        materialized='table',
        tags=['audit', 'quarantine']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('github_raw', 'github_issues') }}
),

parsed AS (
    -- We use the same parsing logic as your staging model
    SELECT
        raw_data:id::NUMBER AS issue_id,
        raw_data:created_at::TIMESTAMP_NTZ AS created_at,
        raw_data,
        source_file,
        loaded_at
    FROM source
)

SELECT 
    *,
    CASE 
        WHEN issue_id IS NULL THEN 'Missing Issue ID'
        WHEN created_at IS NULL THEN 'Missing Created At'
        ELSE 'Unknown/Other'
    END AS rejection_reason,
    CURRENT_TIMESTAMP() AS rejected_at
FROM parsed
WHERE issue_id IS NULL OR created_at IS NULL