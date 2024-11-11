{{ config(
    materialized='table'
) }}

SELECT
    google_id::TEXT,
    review_id::TEXT AS id,
    name::TEXT AS place_name,
    place_id::TEXT,
    location_link::TEXT AS place_location_link,
    reviews_link::TEXT AS place_reviews_link,
    reviews::NUMERIC AS place_review_count,
    rating::NUMERIC AS place_rating,
    -- review_pagination_id::TEXT,  NO VALUE
    author_link::TEXT,
    author_title::TEXT,
    author_id::NUMERIC,
    -- author_image::TEXT AS author_image_link, NO VALUE
    author_reviews_count::NUMERIC,
    review_text::TEXT,
    review_img_urls::TEXT,  -- COULD UNNEST BUT DECIDED THERE'S NOT MUCH VALUE
    review_questions,
    -- review_photo_ids, NO VALUE
    owner_answer::TEXT,
    owner_answer_timestamp::NUMERIC,
    -- owner_answer_timestamp_datetime_utc,  NO VALUE
    review_link::TEXT,
    review_rating::NUMERIC,
    review_timestamp::NUMERIC,
    -- review_datetime_utc,  NO VALUE
    review_likes AS review_likes_count
    -- reviews_id  NO VALUE
FROM {{ source('clever_raw', 'customer_reviews_google') }}