{{ config(
    alias='fmcsa_company_snapshot',
    materialized='table',
    indexes=[
        {'columns': ['id'], 'type': 'btree'},
    ]
) }}

SELECT
    company_name,
    created_at,
    created_by,
    fax_number,
    fmsca_ai_profile,
    id,
    is_hhg_authorized,
    mailing_address,
    mc_num,
    num_of_tractors,
    num_of_trailers,
    num_of_trucks,
    phone_number,
    registered_address,
    safety_review_date,
    total_complaints_2021,
    total_complaints_2022,
    total_complaints_2023,
    updated_at,
    updated_by
FROM {{ ref('fmcsa_company_snapshot_typed') }}