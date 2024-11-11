{{ config(
    alias='fmcsa_complaints',
    materialized='table',
    indexes=[
        {'columns': ['id', 'company_id'], 'type': 'btree'},
    ]
) }}

SELECT
    company_id,
    complaint_category,
    complaint_count,
    complaint_year,
    created_at,
    created_by,
    id
FROM {{ ref('fmcsa_complaints_typed') }}