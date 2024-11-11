{{ config(
    materialized='table'
) }}

SELECT
    about::JSON AS about,  -- TO UNNEST
    area_service::TEXT AS area_service,
    booking_appointment_link::TEXT AS booking_appointment_link,
    borough::TEXT AS borough,
    business_status::TEXT AS business_status,
    category::TEXT AS category,
    cid::TEXT AS cid,
    city::TEXT AS city,
    country::TEXT AS country,
    country_code::TEXT AS country_code,
    description::TEXT AS description,
    full_address::TEXT AS full_address,
    google_id::TEXT AS google_id,
    latitude::NUMERIC AS latitude,
    located_google_id::TEXT AS located_google_id,
    located_in::TEXT AS located_in,
    location_link::TEXT AS place_location_link,
    logo::TEXT AS logo_url,
    longitude::NUMERIC AS longitude,
    -- menu_link::TEXT AS menu_link,  -- ALL NULL
    name::TEXT AS name,
    order_links::TEXT AS order_links,
    other_hours::JSON AS other_hours,  -- TO UNNEST
    owner_id::NUMERIC AS owner_id,
    owner_link::TEXT AS owner_link,
    owner_title::TEXT AS owner_title,
    phone::TEXT AS phone,
    photo::TEXT AS photo_url,
    photos_count::NUMERIC AS photos_count,
    place_id::TEXT AS place_id,
    plus_code::TEXT AS plus_code,
    popular_times::NUMERIC AS popular_times,
    postal_code::TEXT AS postal_code,  -- BAD VALUES IN POSTAL, HANDLED IN FINAL
    posts::JSON AS posts,  -- TO UNNEST
    -- range::TEXT AS range,  -- ALL NULL
    rating::NUMERIC AS rating,
    -- reservation_links::NUMERIC AS reservation_links,  -- ALL NULL
    reviews::NUMERIC AS review_count,
    reviews_id::NUMERIC AS reviews_id,
    reviews_link::TEXT AS reviews_link,
    reviews_tags::TEXT AS reviews_tags,
    site::TEXT AS site,
    state::TEXT AS state,
    street::TEXT AS street,
    street_view::TEXT AS street_view_link,
    subtypes::TEXT AS subtypes,
    time_zone::TEXT AS time_zone,
    type::TEXT AS type,
    -- typical_time_spent::NUMERIC AS typical_time_spent,  -- ALL NULL
    us_state::TEXT AS us_state,
    verified::BOOLEAN AS verified,
    working_hours::JSON AS working_hours  -- TO UNNEST
    -- CAST(working_hours_old_format AS TEXT) AS working_hours_old_format,  -- REMOVED AS THERE'S working_hours
FROM {{ source('clever_raw', 'company_profiles_google_maps') }}