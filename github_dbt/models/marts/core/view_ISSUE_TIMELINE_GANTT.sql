-- Union Issue Lifecycle and Comments into a single activity grain
SELECT 
    ISSUE_ID,
    TITLE AS ISSUE_TITLE,
    STATUS_LABEL AS STATE,
    LIFECYCLE_STAGE,
    CREATED_AT AS EVENT_DATE,
    'ISSUE_START' AS EVENT_TYPE,
    NULL AS COMMENT_BODY
FROM {{ ref('fct_issue_lifecycle') }}

UNION ALL

SELECT 
    ISSUE_ID,
    NULL AS ISSUE_TITLE,
    NULL AS STATE,
    NULL AS LIFECYCLE_STAGE,
    COMMENT_DATE AS EVENT_DATE,
    'COMMENT' AS EVENT_TYPE,
    COMMENT_BODY
FROM {{ ref('fct_issue_comments') }}

-- Add an 'ISSUE_END' row for closed issues to define the bar length
UNION ALL

SELECT 
    ISSUE_ID,
    TITLE AS ISSUE_TITLE,      -- Fixed: Changed from ISSUE_TITLE to TITLE AS ISSUE_TITLE
    STATUS_LABEL AS STATE,     -- Fixed: Changed from STATE to STATUS_LABEL AS STATE
    LIFECYCLE_STAGE,
    COALESCE(CLOSED_AT, CURRENT_DATE()) AS EVENT_DATE,
    'ISSUE_END' AS EVENT_TYPE,
    NULL AS COMMENT_BODY
FROM {{ ref('fct_issue_lifecycle') }}