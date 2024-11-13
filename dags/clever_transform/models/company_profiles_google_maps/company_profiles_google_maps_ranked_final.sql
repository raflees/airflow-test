{{ config(
	tags=["ranking"]
) }}

WITH REVIEWS AS (
	SELECT
		place_id,
		EXTRACT(YEAR FROM review_timestamp) AS year,
        CASE
            WHEN COUNT(sent.strongest_sentiment) = 0 THEN NULL  -- Division protection
        ELSE
            COALESCE(SUM(
                CASE
                    WHEN sent.strongest_sentiment = 'positive' THEN 1
                END
            ), 0)*1.0/COUNT(sent.strongest_sentiment)
        END AS positive_reviews_pct,
		CASE
            WHEN COUNT(sent.strongest_sentiment) = 0 THEN NULL  -- Division protection
        ELSE
            COALESCE(SUM(
                CASE
                    WHEN sent.strongest_sentiment = 'negative' THEN 1
                END
            ), 0)*1.0/COUNT(sent.strongest_sentiment)
        END AS negative_reviews_pct
	FROM {{ ref('customer_reviews_google_final') }} rev
	LEFT JOIN {{ source('analysis', 'reviews_sentiment_analysis') }} sent
		ON rev.id = sent.review_id
	GROUP BY 1, 2
)
SELECT
	place_id,
	year,
	number_of_reviews,
	average_review_rating,
	positive_reviews_pct,
	negative_reviews_pct,
	COALESCE(total_posts, 0) AS total_posts,
    DENSE_RANK() OVER (
		PARTITION BY year
		ORDER BY number_of_reviews DESC
	) AS number_of_reviews_in_year_ranking,
    DENSE_RANK() OVER (
		PARTITION BY year
		ORDER BY average_review_rating DESC
	) AS average_review_rating_in_year_ranking,
    DENSE_RANK() OVER (
		PARTITION BY year
		ORDER BY positive_reviews_pct DESC
	) AS positive_reviews_pct_in_year_ranking,
    DENSE_RANK() OVER (
		PARTITION BY year
		ORDER BY negative_reviews_pct DESC
	) AS negative_reviews_pct_in_year_ranking,
    DENSE_RANK() OVER (
		PARTITION BY year
		ORDER BY COALESCE(total_posts, 0) DESC
	) AS total_posts_in_year_ranking,
    is_latest_year
FROM {{ ref('company_profiles_google_maps_metrics') }}
LEFT JOIN REVIEWS
    USING (place_id, year)