{{ 
    config(
        enabled=true
        )
}}

WITH date_range AS (
SELECT 
  UNNEST(RANGE('2024-01-01'::DATE, '2024-12-31'::DATE, '1 day'::interval))::DATE AS date_id
),

final AS (
    SELECT
        CONCAT(datepart('year', date_id)::STRING,
            lpad(datepart('month', date_id)::STRING, 2, '0'), 
            lpad(datepart('day', date_id)::STRING, 2, '0'))::INT AS _date_hk,
        date_id,
        datepart('day', date_id)::INT AS day_number,
        strftime(date_id, '%A, %-d %B %Y') AS formatted_date,
        CONCAT(datepart('year', date_id)::STRING, 
            lpad(datepart('month', date_id)::STRING, 2, '0'))::INT AS _year_month_sort,
        strftime(date_id, '%B %Y') AS month_year,
        datepart('year', date_id)::INT AS year_number,
        datepart('quarter', date_id)::INT AS quarter_number
    FROM 
        date_range
    UNION
    SELECT
        19000101 AS _date_hk,
        '1900-01-01'::DATE AS date_id,
        -1 AS day_number,
        'Not Applicable' AS formatted_date,
        -1 AS _year_month_sort,
        'Not Applicable' AS month_year,
        -1 AS year_number,
        -1 AS quarter_number
    UNION
    SELECT
        19000102 AS _date_hk,
        '1900-01-02'::DATE AS date_id,
        -2 AS day_number,
        'Unknown' AS formatted_date,
        -2 AS _year_month_sort,
        'Unknown' AS month_year,
        -2 AS year_number,
        -2 AS quarter_number
)

SELECT
    *
FROM
    final
