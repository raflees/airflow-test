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
    total_complaints_2021,
    total_complaints_2022,
    total_complaints_2023,
    updated_at,
    updated_by
FROM {{ ref('fmcsa_companies_typed') }}