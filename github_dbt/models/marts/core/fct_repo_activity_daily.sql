{{
    config(
        materialized='incremental',
        unique_key='fct_key',
        tags=['marts', 'core', 'fact']
    )
}}

WITH issues AS (
    SELECT
        issue_id,
        created_at,
        closed_at,
        is_pull_request,
        comment_count,
        author_username,
        has_assignee,
        has_labels,
        hours_to_close,
        repository
    FROM {{ ref('int_issue_metrics') }}
),

repos AS (
    SELECT
        repository_id,
        repository_name,
        full_name,
        owner_username,
        stars_count,
        forks_count,
        open_issues_count
    FROM {{ ref('int_repository_metrics') }}
),

daily_activity AS (
    SELECT
        -- Surrogate key (single column, no ambiguity in MERGE)
        DATE_TRUNC('day', i.created_at)::DATE::VARCHAR
            || '_' || COALESCE(r.repository_id::VARCHAR, 'unknown')  AS fct_key,

        -- Date dimension
        DATE_TRUNC('day', i.created_at)                             AS activity_date,

        -- Repository dimension
        r.repository_id,
        r.repository_name,
        r.full_name                                                  AS repository_full_name,
        r.owner_username,

        -- Issue counts
        COUNT(DISTINCT i.issue_id)                                   AS total_issues_created,
        COUNT(DISTINCT CASE WHEN i.is_pull_request
                            THEN i.issue_id END)                     AS pull_requests_created,
        COUNT(DISTINCT CASE WHEN NOT i.is_pull_request
                            THEN i.issue_id END)                     AS issues_created,

        -- Closed counts
        COUNT(DISTINCT CASE WHEN i.closed_at IS NOT NULL
                            THEN i.issue_id END)                     AS issues_closed,
        COUNT(DISTINCT CASE WHEN i.closed_at IS NOT NULL
                             AND i.is_pull_request
                            THEN i.issue_id END)                     AS pull_requests_closed,

        -- Comment activity
        SUM(i.comment_count)                                         AS total_comments,
        AVG(i.comment_count)                                         AS avg_comments_per_issue,

        -- Engagement metrics
        COUNT(DISTINCT i.author_username)                            AS unique_contributors,
        COUNT(DISTINCT CASE WHEN i.has_assignee
                            THEN i.issue_id END)                     AS assigned_issues,
        COUNT(DISTINCT CASE WHEN i.has_labels
                            THEN i.issue_id END)                     AS labeled_issues,

        -- Resolution time metrics
        AVG(CASE WHEN i.closed_at IS NOT NULL
                 THEN i.hours_to_close END)                          AS avg_hours_to_close,
        MEDIAN(CASE WHEN i.closed_at IS NOT NULL
                    THEN i.hours_to_close END)                       AS median_hours_to_close,

        -- Repository context
        MAX(r.stars_count)                                           AS current_stars,
        MAX(r.forks_count)                                           AS current_forks,
        MAX(r.open_issues_count)                                     AS current_open_issues,

        -- Metadata
        CURRENT_TIMESTAMP()                                          AS dbt_updated_at

    FROM issues i
    LEFT JOIN repos r
        ON i.repository = r.full_name

    {% if is_incremental() %}
    WHERE DATE_TRUNC('day', i.created_at) > (
        SELECT MAX(activity_date) FROM {{ this }}
    )
    {% endif %}

    GROUP BY
        DATE_TRUNC('day', i.created_at)::DATE::VARCHAR
            || '_' || COALESCE(r.repository_id::VARCHAR, 'unknown'),
        DATE_TRUNC('day', i.created_at),
        r.repository_id,
        r.repository_name,
        r.full_name,
        r.owner_username
)

SELECT * FROM daily_activity