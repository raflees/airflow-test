{{ config(
    alias='company_profiles_google_maps',
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
    comps.place_id,
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
    verified,
    -- RANKING
    number_of_reviews,
    average_review_rating,
    positive_reviews_pct,
    negative_reviews_pct,
    total_posts,
    number_of_reviews_in_year_ranking AS number_of_reviews_ranking,
    average_review_rating_in_year_ranking AS average_review_rating_ranking,
    positive_reviews_pct_in_year_ranking AS positive_reviews_pct_ranking,
    negative_reviews_pct_in_year_ranking AS negative_reviews_pct_ranking,
    total_posts_in_year_ranking AS total_posts_ranking
FROM {{ ref('company_profiles_google_maps_typed') }} comps
LEFT JOIN {{ ref('company_profiles_google_maps_ranked') }} rnk
    ON rnk.place_id = comps.place_id AND rnk.is_latest_ranking
