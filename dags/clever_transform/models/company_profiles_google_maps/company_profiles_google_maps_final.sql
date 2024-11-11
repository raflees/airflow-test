{{ config(
    alias='company_profiles_google_maps',
    materialized='table',
    indexes=[
        {'columns': ['place_id'], 'type': 'btree'}
    ]
) }}

SELECT
    booking_appointment_link,
    borough,
    business_status,
    category,
    cid,
    city,
    country_code,
    country,
    description,
    full_address,
    google_id,
    has_area_service,
    latitude,
    located_google_id,
    located_in,
    logo_url,
    longitude,
    CASE
        WHEN postal_code ~ '^[\d.]+$'
        THEN LPAD(postal_code::NUMERIC::INT::TEXT, 5, '0')  -- READ / REMOVE DECIMAL / TEXT
    ELSE
        NULL
    END AS postal_code,
    name,
    order_links,
    owner_id,
    owner_link,
    owner_title,
    phone,
    photo_url,
    photos_count,
    place_id,
    place_location_link,
    plus_code,
    popular_times,
    rating,
    reviews_id,
    reviews_link,
    reviews_tags,
    review_count,
    site,
    state,
    street_view_link,
    street,
    subtypes,
    time_zone,
    type,
    us_state,
    verified
FROM {{ ref('company_profiles_google_maps_typed') }}