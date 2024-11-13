{{ config(
	tags=["ranking"]
) }}

SELECT
	company_id,
	complaint_year,
	RANK() OVER (
		PARTITION BY complaint_year
		ORDER BY total_complaints ASC
	) AS total_complaints_in_year_ranking,
	is_latest_year
FROM {{ ref('fmcsa_companies_metrics') }}