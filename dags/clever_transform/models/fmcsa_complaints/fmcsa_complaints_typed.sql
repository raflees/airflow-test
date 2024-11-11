SELECT
    usdot_num::NUMERIC AS company_id,
    id::TEXT,
    user_created::TEXT AS created_by,
    date_created::TIMESTAMP AS created_at,
    -- user_updated  -- ALL NULL
    -- date_updated  -- ALL NULL
    complaint_category::TEXT,
    complaint_year::NUMERIC,
    complaint_count::NUMERIC
FROM {{ source('clever_raw', 'fmcsa_complaints') }}