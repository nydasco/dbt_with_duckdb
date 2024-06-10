{{ 
    config(
        enabled=true
        )
}}

WITH
first_record AS (
    SELECT
        region AS region_name,
        min(dbt_valid_from)::TIMESTAMP AS _created_datetime
    FROM
        {{ ref('snp_sale')}}
    GROUP BY
        region
),

region AS (
    SELECT DISTINCT
        MD5(region) AS _region_hk,
        region AS region_name
    FROM
        {{ ref('snp_sale')}}
    WHERE
        region IS NOT NULL
),

final AS (
    SELECT
        region._region_hk,
        first_record._created_datetime,
        region.region_name
    FROM
        region
        INNER JOIN first_record
            ON region.region_name = first_record.region_name
    UNION
    SELECT
        '-1' AS _region_hk,
        '1900-01-01 00:00:00'::TIMESTAMP AS _created_datetime,
        'Not Applicable' AS region_name
    UNION
    SELECT
        '-2' AS _region_hk,
        '1900-01-01 00:00:00'::TIMESTAMP AS _created_datetime,
        'Unknown' AS region_name
)

SELECT
    *
FROM    
    final
