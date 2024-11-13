SELECT
	company_id,
	complaint_year,
	SUM(complaint_count) AS total_complaints,
	CASE
		WHEN complaint_year = MAX(complaint_year) OVER()
			THEN TRUE
		ELSE
			FALSE
	END AS is_latest_year
FROM {{ ref('fmcsa_complaints_final') }}
GROUP BY company_id, complaint_year