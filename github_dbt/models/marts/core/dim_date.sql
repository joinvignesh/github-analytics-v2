{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'date']
    )
}}

WITH date_spine AS (
    -- Generate dates from earliest issue to today + 7 days
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="dateadd(day, 7, current_date())"
    ) }}
),

dates AS (
    SELECT
        date_day AS date_key,
        date_day AS date,
        
        -- Date components
        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        DAY(date_day) AS day,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYOFYEAR(date_day) AS day_of_year,
        WEEKOFYEAR(date_day) AS week_of_year,
        
        -- Labels
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        TO_CHAR(date_day, 'YYYY-Qq') AS year_quarter,
        TO_CHAR(date_day, 'Mon YYYY') AS month_name,
        DAYNAME(date_day) AS day_name,
        
        -- Flags
        CASE WHEN DAYOFWEEK(date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN date_day = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN YEAR(date_day) = YEAR(CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_year
        
    FROM date_spine
)

SELECT * FROM dates