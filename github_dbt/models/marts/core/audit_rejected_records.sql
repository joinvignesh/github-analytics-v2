{{ config(
    materialized='table'
) }}

SELECT 'issue' as record_type, issue_id as original_id, rejection_reason, rejected_at 
FROM {{ ref('audit_stg_github_issues_rejected') }}

UNION ALL

-- Repeat for comments and repos once you build their quarantine models
SELECT 'comment' as record_type, comment_id as original_id, rejection_reason, rejected_at 
FROM {{ ref('audit_stg_github_comments_rejected') }}