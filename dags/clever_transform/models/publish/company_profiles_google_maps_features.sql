SELECT
	place_id,
	category,
	feature,
	feature_description
FROM {{ ref('company_profiles_features_final') }}