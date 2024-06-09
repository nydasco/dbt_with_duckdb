{{ 
    config(
        enabled=true
        )
}}

WITH
d_client AS (
    SELECT
        client_hk,
        client_bk
    FROM
        {{ ref('dim_client')}}
    WHERE
        1 = 1
),

d_employee AS (
    SELECT
        employee_hk,
        employee_bk
    FROM
        {{ ref('dim_employee')}}
    WHERE
        1 = 1
),

d_region AS (
    SELECT
        region_hk,
        region_name
    FROM
        {{ ref('dim_region')}}
    WHERE
        1 = 1
),

sale AS (
    SELECT
        COALESCE(d_client.client_hk, '-2') AS client_hk,
        COALESCE(d_employee.employee_hk, '-2') AS employee_hk,
        COALESCE(d_region.region_hk, '-2') AS region_hk,
        sale.date::DATE AS sale_date,
        SUM(sale.sale) AS sale_amount
    FROM
        {{ source('external_source', 'sale')}}
        LEFT JOIN d_client
            ON COALESCE(sale.client_id, '-1') = d_client.client_bk
        LEFT JOIN d_employee
            ON COALESCE(sale.employee_id, '-1') = d_employee.employee_bk
        LEFT JOIN d_region
            ON COALESCE(sale.region, 'Unknown') = d_region.region_name
    WHERE
        1 = 1
    GROUP BY
        client_hk,
        employee_hk,
        region_hk,
        sale_date
)

SELECT
    *
FROM    
    sale
