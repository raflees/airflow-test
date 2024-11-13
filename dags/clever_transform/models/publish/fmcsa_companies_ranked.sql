SELECT
	company_id,
	complaint_year,
	total_complaints_in_year_ranking
FROM {{ ref('fmcsa_companies_ranked_final') }}