{{ config(
	alias='company_profiles_features',
	indexes=[
        {'columns': ['place_id', 'category', 'feature'], 'type': 'btree'},
    ]
) }}

SELECT
	place_id,
	category,
	feature,
	feature_description
FROM {{ ref('company_profiles_features_unnested') }}
WHERE COALESCE(feature_description, 'true') <> 'false'