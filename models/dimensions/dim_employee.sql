{{ 
    config(
        materialized='table',
        enabled=true
        )
}}

WITH
department AS (
    SELECT
        id AS department_bk,
        department AS department_name
    FROM
        {{ ref('department')}}
    WHERE
        1 = 1
),

employee AS (
    SELECT
        MD5(id::VARCHAR) AS employee_hk,
        id AS employee_bk,
        CONCAT(last_name, ', ', first_name) AS employee_name,
        department_id AS department_bk
    FROM
        {{ ref('employee')}}
    WHERE
        1 = 1
),

final AS (
    SELECT
        employee.employee_hk,
        employee.employee_bk,
        employee.employee_name,
        COALESCE(department.department_name, '') AS department_name
    FROM
        employee
    LEFT JOIN department
        ON employee.department_bk = department.department_bk
    UNION
    SELECT
        '-1' AS employee_hk,
        -1 AS employee_bk,
        'Not Applicable' AS employee_name,
        'Not Applicable' AS department_name
    UNION
    SELECT
        '-2' AS employee_hk,
        -2 AS employee_bk,
        'Unknown' AS employee_name,
        'Unknown' AS department_name
)

SELECT
    *
FROM    
    final
