{{ config(
	materialized='view'
) }}

SELECT
	place_id,
	posts_item.value ->> 'body' AS body,
	posts_item.value ->> 'timestamp' AS posted_at,
	posts_item.value ->> 'link' AS post_link,
	posts_item.value ->> 'image' AS image_link
FROM {{ ref('company_profiles_google_maps_typed') }},
	json_array_elements(posts) AS posts_item
WHERE posts IS NOT NULL