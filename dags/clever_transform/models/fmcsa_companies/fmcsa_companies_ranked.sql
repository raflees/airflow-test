SELECT
	company_id,
	complaint_year,
	SUM(complaint_count) AS total_complaints,
	RANK() OVER (
		PARTITION BY complaint_year
		ORDER BY SUM(complaint_count) ASC
	) AS total_complaints_ranking_in_year.
	CASE
		WHEN complaint_year = MAX(complaint_year) OVER()
			THEN TRUE
		ELSE
			FALSE
	END AS is_latest_ranking
FROM {{ ref('fmcsa_companies_typed') }}
GROUP BY company_id, complaint_year