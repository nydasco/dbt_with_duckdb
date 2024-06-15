{{ 
    config(
        enabled=true
        )
}}

SELECT
	id,
	LEFT(department, 50)::VARCHAR(50) AS department,
	LEFT(dbt_scd_id, 32)::VARCHAR(32) AS dbt_scd_id,
	dbt_updated_at,
	dbt_valid_from,
	dbt_valid_to
FROM
    {{ ref('snp_department')}}
