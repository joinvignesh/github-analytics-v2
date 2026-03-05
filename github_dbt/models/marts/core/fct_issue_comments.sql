{{
    config(
        materialized='incremental',
        unique_key='comment_key',
        tags=['marts', 'core', 'fact', 'comments']
    )
}}

WITH comments AS (
    SELECT
        comment_id,
        issue_number,
        repository_full_name,
        author_username,
        author_id,
        created_at,
        updated_at,
        body,
        html_url
    FROM {{ ref('stg_github_comments') }}
    
    {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

issues AS (
    SELECT
        issue_id,
        issue_number,
        repository_full_name
    FROM {{ ref('fct_issue_lifecycle') }}
),

joined AS (
    SELECT
        -- Comment identifiers
        c.comment_id,
        i.issue_id,
        c.issue_number,
        c.repository_full_name,
        
        -- Timestamps
        c.created_at AS comment_date,
        c.updated_at AS comment_updated_at,
        
        -- Date (for filtering)
        DATE(c.created_at) AS comment_date_key,
        
        -- Author
        c.author_username,
        c.author_id,
        
        -- Content
        c.body AS comment_body,
        LENGTH(c.body) AS comment_length,
        
        -- Links
        c.html_url,
        
        -- Sequence (order of comments on the issue)
        ROW_NUMBER() OVER (
            PARTITION BY i.issue_id 
            ORDER BY c.created_at
        ) AS comment_sequence,
        
        -- Generate surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.comment_id']) }} AS comment_key
        
    FROM comments c
    LEFT JOIN issues i 
        ON c.issue_number = i.issue_number 
        AND c.repository_full_name = i.repository_full_name
)

SELECT * FROM joined
WHERE issue_id IS NOT NULL  -- Only keep comments that match to known issues