{{ config(
    alias='fmcsa_companies',
    indexes=[
        {'columns': ['id'], 'type': 'btree'},
    ]
) }}

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
    total_complaints AS total_complaints_latest_year,
    updated_at,
    updated_by
FROM {{ ref('fmcsa_companies_typed') }} comps
JOIN {{ ref('fmcsa_companies_metrics') }} rnk
    ON rnk.company_id = comps.id AND rnk.is_latest_year