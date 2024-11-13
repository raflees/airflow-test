WITH BASE AS (
    SELECT place_id FROM {{ ref('company_profiles_google_maps_typed') }}
), POST_COUNT AS (
	SELECT
		place_id,
		EXTRACT(YEAR FROM posted_at) AS year,
		COUNT(*) AS total_posts
	FROM {{ ref('company_profiles_posts_final') }}
	GROUP BY 1, 2
), REVIEWS AS (
	SELECT
		place_id,
		EXTRACT(YEAR FROM review_timestamp) AS year,
		COUNT(*) AS number_of_reviews,
		AVG(review_rating) AS average_review_rating
	FROM {{ ref('customer_reviews_google_final') }} rev
	GROUP BY 1, 2
)
SELECT
	place_id,
	year,
	number_of_reviews,
	average_review_rating,
	total_posts,
    CASE
		WHEN year = MAX(year) OVER()
			THEN TRUE
		ELSE
			FALSE
	END AS is_latest_year
FROM BASE
LEFT JOIN REVIEWS
    USING (place_id)
LEFT JOIN POST_COUNT
	USING (place_id, year)