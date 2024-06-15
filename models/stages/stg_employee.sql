{{ 
    config(
        enabled=true
        )
}}

SELECT
	id,
	LEFT(first_name, 50)::VARCHAR(50) AS first_name,
	LEFT(last_name, 50)::VARCHAR(50) AS last_name,
	department_id,
	LEFT(dbt_scd_id, 32)::VARCHAR(32) AS dbt_scd_id,
	dbt_updated_at,
	dbt_valid_from,
	dbt_valid_to
FROM
    {{ ref('snp_employee')}}
