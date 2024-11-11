SELECT
    company_name::TEXT,
    date_created::TIMESTAMP AS created_at,
    date_updated::TIMESTAMP AS updated_at,
    fax_number::TEXT,
    fmsca_ai_profile::TEXT,
    hhg_authorization::BOOLEAN AS is_hhg_authorized,
    mailing_address::TEXT,
    mc_num::NUMERIC,
    num_of_tractors::NUMERIC,
    num_of_trailers::NUMERIC,
    num_of_trucks::NUMERIC,
    phone_number::TEXT,
    registered_address::TEXT,
    safety_review_date::DATE,
    total_complaints_2021::NUMERIC,
    total_complaints_2022::NUMERIC,
    total_complaints_2023::NUMERIC,
    usdot_num::NUMERIC AS id,
    user_created::TEXT AS created_by,
    user_updated::TEXT AS updated_by
FROM {{ source('clever_raw', 'fmcsa_company_snapshot') }}