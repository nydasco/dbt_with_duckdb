{{ 
    config(
        enabled=true
        )
}}

SELECT
	id,
	employee_id,
	client_id,
	date,
	LEFT(region, 10)::VARCHAR(10) AS region,
	sale,
	LEFT(dbt_scd_id, 32)::VARCHAR(32) AS dbt_scd_id,
	dbt_updated_at,
	dbt_valid_from,
	dbt_valid_to
FROM
    {{ ref('snp_sale')}}
