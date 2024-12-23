SELECT
    city::TEXT,
    company_type::NUMERIC,
    company_url::TEXT,
    date_created::TIMESTAMP AS created_at,
    date_updated::TIMESTAMP AS updated_at,
    location::TEXT,
    company_name::TEXT AS name,
    state::TEXT,
    usdot_num::NUMERIC AS id,
    user_created::TEXT AS created_by,
    user_updated::TEXT AS updated_by
FROM {{ source('clever_raw', 'fmcsa_companies') }}