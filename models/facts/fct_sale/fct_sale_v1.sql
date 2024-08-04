{{ 
    config(
        enabled=true
        )
}}

WITH
d_client AS (
    SELECT
        _client_hk,
        _client_bk
    FROM
        {{ ref('dim_client', v='1') }}
),

d_employee AS (
    SELECT
        _employee_hk,
        _employee_bk,
        _valid_from_datetime,
        _valid_to_datetime
    FROM
        {{ ref('dim_employee', v='1') }}
),

d_region AS (
    SELECT
        _region_hk,
        region_name
    FROM
        {{ ref('dim_region', v='1') }}
),

sale AS (
    SELECT
        COALESCE(d_client._client_hk, '-2') AS _client_hk,
        COALESCE(d_employee._employee_hk, '-2') AS _employee_hk,
        COALESCE(d_region._region_hk, '-2') AS _region_hk,
        sale.date::DATE AS sale_date,
        SUM(sale.sale) AS sale_amount
    FROM
        {{ ref('stg_sale') }} AS sale
        LEFT JOIN d_client
            ON COALESCE(sale.client_id, '-1') = d_client._client_bk
        LEFT JOIN d_employee
            ON COALESCE(sale.employee_id, '-1') = d_employee._employee_bk
            AND sale.date::DATE >= d_employee._valid_from_datetime
            AND sale.date::DATE < d_employee._valid_to_datetime
        LEFT JOIN d_region
            ON COALESCE(sale.region, 'Unknown') = d_region.region_name
    WHERE
        1 = 1
        AND sale.dbt_valid_to IS NULL
    GROUP BY
        _client_hk,
        _employee_hk,
        _region_hk,
        sale_date
)

SELECT
    *
FROM    
    sale
