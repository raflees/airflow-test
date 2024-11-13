SELECT
    canadian_driver_inspections,
    canadian_driver_out_of_service,
    canadian_driver_out_of_service_pct,
    canadian_vehicle_inspections,
    canadian_vehicle_out_of_service,
    canadian_vehicle_out_of_service_pct,
    cargo_types,
    carrier_safety_rating,
    carrier_safety_rating_date,
    carrier_safety_rating_review_date,
    carrier_safety_rating_type,
    carrier_type,
    company_id,
    created_at,
    created_by,
    dba_name,
    drivers_count,
    duns_number,
    entity_type,
    fmcsa_link,
    legal_name,
    mailing_address,
    mc_num,
    mcs_150_form_date,
    mileage,
    mileage_year,
    oos_date,
    operating_status,
    operation_classification,
    phone,
    physical_address,
    power_units_count,
    updated_at,
    updated_by,
    us_crashes_fatal_count,
    us_crashes_injury_count,
    us_crashes_total_count,
    us_crashes_tow_count,
    us_driver_inspections_count,
    us_driver_natl_avg_oos_pct,
    us_driver_out_of_service_count,
    us_driver_out_of_service_pct,
    us_hazmat_inspections_count,
    us_hazmat_natl_avg_oos_pct,
    us_hazmat_out_of_service_count,
    us_hazmat_out_of_service_pct,
    us_iep_inspections_count,
    us_iep_natl_avg_oos_pct,
    us_iep_out_of_service_count,
    us_iep_out_of_service_pct,
    us_vehicle_inspections_count,
    us_vehicle_natl_avg_oos_pct,
    us_vehicle_out_of_service_count,
    us_vehicle_out_of_service_pct
FROM {{ ref('fmcsa_safer_data_final') }}