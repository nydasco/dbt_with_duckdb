{{ 
    config(
        enabled=true
        )
}}

WITH
client AS (
    SELECT
        MD5(id::VARCHAR) AS client_hk,
        id AS client_bk,
        CONCAT(last_name, ', ', first_name) AS client_name
    FROM
        {{ ref('snp_client')}}
    WHERE
        1 = 1
        AND dbt_valid_to IS NULL
),

final AS (
    SELECT
        client_hk,
        client_bk,
        client_name
    FROM
        client
    UNION
    SELECT
        '-1' AS client_hk,
        -1 AS client_bk,
        'Not Applicable' AS client_name
    UNION
    SELECT
        '-2' AS client_hk,
        -2 AS client_bk,
        'Unknown' AS client_name
)

SELECT
    *
FROM    
    final
