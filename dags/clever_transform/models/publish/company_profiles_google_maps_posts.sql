SELECT
	place_id,
	body,
	posted_at,
	post_link,
	image_link
FROM {{ ref('company_profiles_posts_final') }}