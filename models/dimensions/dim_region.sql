{{ 
    config(
        materialized='table',
        enabled=true
        )
}}

WITH
region AS (
    SELECT DISTINCT
        MD5(region) AS region_hk,
        region AS region_name
    FROM
        {{ ref('sale')}}
    WHERE
        1 = 1
        AND region IS NOT NULL
),

final AS (
    SELECT
        region_hk,
        region_name
    FROM
        region
    UNION
    SELECT
        '-1' AS region_hk,
        'Not Applicable' AS region_name
    UNION
    SELECT
        '-2' AS region_hk,
        'Unknown' AS region_name
)

SELECT
    *
FROM    
    final
