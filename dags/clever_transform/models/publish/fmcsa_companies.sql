SELECT
    city,
    company_type,
    company_url,
    created_at,
    created_by,
    id,
    location,
    name,
    state,
    total_complaints_latest_year,
    updated_at,
    updated_by
FROM {{ ref('fmcsa_companies_final') }}