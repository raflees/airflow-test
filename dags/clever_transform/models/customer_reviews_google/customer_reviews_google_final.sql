{{ config(
    alias='customer_reviews_google',
    indexes=[
        {'columns': ['place_id', 'id'], 'type': 'btree'},
    ]
) }}

SELECT
    id,
    author_id,
    author_link,
    author_reviews_count,
    author_title,
    google_id,
    owner_answer_timestamp,
    owner_answer,
    place_id,
    place_location_link,
    place_name,
    place_rating,
    place_review_count,
    place_reviews_link,
    review_img_urls,
    review_likes_count,
    review_link,
    review_questions,
    review_rating,
    review_text,
    TO_TIMESTAMP(review_timestamp::NUMERIC) AS review_timestamp
FROM {{ ref('customer_reviews_google_typed') }}