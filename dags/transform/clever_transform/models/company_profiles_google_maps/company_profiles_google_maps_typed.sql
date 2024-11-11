{{ config(
    materialized='table'
) }}

SELECT
    CAST(about AS TEXT)::json AS about,  -- TO UNNEST
    CAST(area_service AS BOOLEAN) AS area_service,
    CAST(booking_appointment_link AS TEXT) AS booking_appointment_link,
    CAST(borough AS TEXT) AS borough,
    CAST(business_status AS TEXT) AS business_status,
    CAST(category AS TEXT) AS category,
    CAST(cid AS TEXT) AS cid,
    CAST(city AS TEXT) AS city,
    CAST(country AS TEXT) AS country,
    CAST(country_code AS TEXT) AS country_code,
    CAST(description AS TEXT) AS description,
    CAST(full_address AS TEXT) AS full_address,
    CAST(google_id AS TEXT) AS google_id,
    CAST(latitude AS NUMERIC) AS latitude,
    CAST(located_google_id AS TEXT) AS located_google_id,
    CAST(located_in AS TEXT) AS located_in,
    CAST(location_link AS TEXT) AS location_link,
    CAST(logo AS TEXT) AS logo,
    CAST(longitude AS NUMERIC) AS longitude,
    CAST(menu_link AS TEXT) AS menu_link,  -- ALL NULL
    CAST(name AS TEXT) AS name,
    CAST(order_links AS TEXT) AS order_links,
    CAST(other_hours AS TEXT)::json AS other_hours,  -- TO UNNEST
    CAST(owner_id AS NUMERIC) AS owner_id,
    CAST(owner_link AS TEXT) AS owner_link,
    CAST(owner_title AS TEXT) AS owner_title,
    CAST(phone AS TEXT) AS phone,
    CAST(photo AS TEXT) AS photo_url,
    CAST(photos_count AS NUMERIC) AS photos_count,
    CAST(place_id AS TEXT) AS place_id,
    CAST(plus_code AS TEXT) AS plus_code,
    CAST(popular_times AS NUMERIC) AS popular_times,
    CAST(postal_code AS TEXT) AS postal_code,
    CAST(posts AS TEXT)::json AS posts,  -- TO UNNEST
    CAST(range AS TEXT) AS range,  -- ALL NULL
    CAST(rating AS NUMERIC) AS rating,
    CAST(reservation_links AS NUMERIC) AS reservation_links,  -- ALL NULL
    CAST(reviews AS NUMERIC) AS reviews,
    CAST(reviews_id AS NUMERIC) AS reviews_id,
    CAST(reviews_link AS TEXT) AS reviews_link,
    CAST(reviews_tags AS TEXT) AS reviews_tags,
    CAST(site AS TEXT) AS site,
    CAST(state AS TEXT) AS state,
    CAST(street AS TEXT) AS street,
    CAST(street_view AS TEXT) AS street_view,
    CAST(subtypes AS TEXT) AS subtypes,
    CAST(time_zone AS TEXT) AS time_zone,
    CAST(type AS TEXT) AS type,
    CAST(typical_time_spent AS NUMERIC) AS typical_time_spent,  -- ALL NULL
    CAST(us_state AS TEXT) AS us_state,
    CAST(verified AS BOOLEAN) AS verified,
    CAST(working_hours AS TEXT)::json AS working_hours  -- TO UNNEST
    -- CAST(working_hours_old_format AS TEXT) AS working_hours_old_format,  -- REMOVED AS THERE'S working_hours
FROM {{ source('clever_raw', 'company_profiles_google_maps') }}