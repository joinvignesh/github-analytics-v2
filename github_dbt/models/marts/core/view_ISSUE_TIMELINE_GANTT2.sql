{{
    config(
        materialized='view',
        tags=['marts', 'analytics']
    )
}}

WITH issue_base AS (
    -- Get the core issue metadata to join against comments
    SELECT 
        issue_id,
        issue_number,
        title AS issue_title,
        status_label AS state,
        lifecycle_stage,
        repository_full_name,
        created_at AS issue_opened_at,
        COALESCE(closed_at, CURRENT_TIMESTAMP()) AS issue_closed_at,
        html_url AS issue_url
    FROM {{ ref('fct_issue_lifecycle') }}
),

issue_events AS (
    -- 1. THE BAR: Define the duration of the issue itself
    SELECT 
        issue_id,
        issue_title,
        state,
        lifecycle_stage,
        repository_full_name,
        issue_opened_at AS event_date,
        'ISSUE_DURATION' AS event_type,
        issue_opened_at,
        issue_closed_at,
        NULL AS comment_body,
        issue_url AS event_url
    FROM issue_base

    UNION ALL

    -- 2. THE MILESTONES: The comments mapped to the issue timeline
    SELECT 
        i.issue_id,
        i.issue_title,
        i.state,
        i.lifecycle_stage,
        i.repository_full_name,
        c.comment_date AS event_date,
        'COMMENT' AS event_type,
        i.issue_opened_at,
        i.issue_closed_at,
        c.comment_body,
        c.html_url AS event_url
    FROM {{ ref('fct_issue_comments') }} c
    INNER JOIN issue_base i ON c.issue_id = i.issue_id
)

SELECT 
    *,
    -- Helpful for BI tools to calculate progress/length
    DATEDIFF('day', issue_opened_at, issue_closed_at) AS total_issue_age_days,
    DATEDIFF('day', issue_opened_at, event_date) AS days_since_opened
FROM issue_events