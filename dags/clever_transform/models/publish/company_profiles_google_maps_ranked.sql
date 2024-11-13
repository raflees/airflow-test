SELECT
	place_id,
    year,
    average_review_rating_in_year_ranking,
    negative_reviews_pct_in_year_ranking,
    number_of_reviews_in_year_ranking,
    positive_reviews_pct_in_year_ranking,
    total_posts_in_year_ranking,
    is_latest_year
FROM {{ ref('company_profiles_google_maps_ranked_final') }}