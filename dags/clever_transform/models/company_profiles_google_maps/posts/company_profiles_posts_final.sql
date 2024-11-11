{{ config(
	alias='company_profiles_posts',
    materialized='table'
) }}

SELECT
	place_id,
	body,
	TO_TIMESTAMP(posted_at::NUMERIC) AS posted_at,
	post_link,
	image_link
FROM {{ ref('company_profiles_posts_unnested') }}