{{ 
    config(
        enabled=true
        )
}}

WITH 
se AS (
	SELECT
		id,
		department_id,
		CONCAT(last_name, ', ', first_name) AS employee_name,
		dbt_valid_from,
		COALESCE(dbt_valid_to, '2999-12-31') AS dbt_valid_to
	FROM
		{{ ref('snp_employee') }}
),
sd AS (
	SELECT 
		id,
		department,
		dbt_valid_from,
		COALESCE(dbt_valid_to, '2999-12-31') AS dbt_valid_to
	FROM
		{{ ref('snp_department') }}
),
final AS (
	SELECT
		se.id AS _employee_bk,
		GREATEST(
			COALESCE(se.dbt_valid_from, '1900-01-01'),
			COALESCE(sd.dbt_valid_from, '1900-01-01')
		) AS _valid_from_datetime,
		LEAST(
			COALESCE(se.dbt_valid_to, '2999-12-31'),
			COALESCE(sd.dbt_valid_to, '2999-12-31')
		) AS _valid_to_datetime,
		se.employee_name,
		sd.department
	FROM
		se
		LEFT JOIN sd
		  ON se.department_id = sd.id
		  AND se.dbt_valid_from <= sd.dbt_valid_to
		  AND se.dbt_valid_to > sd.dbt_valid_from
)
SELECT 
	MD5(CONCAT(_employee_bk::VARCHAR, _valid_from_datetime::VARCHAR)) AS employee_hk,
	_employee_bk,
	_valid_from_datetime AS _load_datetime,
	CASE 
		WHEN LAG(_valid_from_datetime) OVER (PARTITION BY _employee_bk ORDER BY _valid_from_datetime) IS NULL THEN '1900-01-01'
		ELSE _valid_from_datetime
	END AS _valid_from_datetime,
	_valid_to_datetime,
	employee_name,
	department
FROM
	final
