{{ 
    config(
        enabled=true
        )
}}

WITH
first_record AS (
    SELECT
        id AS _client_bk,
        min(dbt_valid_from)::TIMESTAMP AS _created_datetime
    FROM
        {{ ref('snp_client')}}
    GROUP BY
        id
),

client AS (
    SELECT
        MD5(id::VARCHAR) AS _client_hk,
        id AS _client_bk,
        dbt_valid_from::TIMESTAMP AS _modified_datetime,
        CASE 
            WHEN dbt_valid_to IS NULL THEN False
            ELSE True
        END AS _is_deleted,
        CONCAT(last_name, ', ', first_name) AS client_name
    FROM
        {{ ref('snp_client')}}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY dbt_valid_from DESC) = 1
),

final AS (
    SELECT
        client._client_hk,
        client._client_bk,
        first_record._created_datetime,
        client._modified_datetime,
        client._is_deleted,
        client.client_name
    FROM
        client
        INNER JOIN first_record
            ON client._client_bk = first_record._client_bk
    UNION
    SELECT
        '-1' AS _client_hk,
        -1 AS _client_bk,
        '1900-01-01 00:00:00'::TIMESTAMP AS _created_datetime,
        '1900-01-01 00:00:00'::TIMESTAMP AS _modified_datetime,
        False AS _is_deleted,
        'Not Applicable' AS client_name
    UNION
    SELECT
        '-2' AS _client_hk,
        -2 AS _client_bk,
        '1900-01-01 00:00:00'::TIMESTAMP AS _created_datetime,
        '1900-01-01 00:00:00'::TIMESTAMP AS _modified_datetime,
        False AS _is_deleted,
        'Unknown' AS client_name
)

SELECT
    *
FROM    
    final
