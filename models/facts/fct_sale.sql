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
        {{ ref('dim_client') }}
),

d_employee AS (
    SELECT
        _employee_hk,
        _employee_bk,
        _valid_from_datetime,
        _valid_to_datetime
    FROM
        {{ ref('dim_employee') }}
),

d_region AS (
    SELECT
        _region_hk,
        region_name
    FROM
        {{ ref('dim_region') }}
),

d_date AS (
    SELECT
        _date_hk,
        date_id
    FROM
        {{ ref('dim_date') }}
),

sale AS (
    SELECT
        COALESCE(d_client._client_hk, '-2') AS _client_hk,
        COALESCE(d_employee._employee_hk, '-2') AS _employee_hk,
        COALESCE(d_region._region_hk, '-2') AS _region_hk,
        COALESCE(d_date._date_hk, -2) AS _date_hk,
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
        LEFT JOIN d_date
            ON COALESCE(sale.date, 'Unknown') = d_date.date_id
    WHERE
        1 = 1
        AND sale.dbt_valid_to IS NULL
    GROUP BY
        _client_hk,
        _employee_hk,
        _region_hk,
        _date_hk
)

SELECT
    *
FROM    
    sale
