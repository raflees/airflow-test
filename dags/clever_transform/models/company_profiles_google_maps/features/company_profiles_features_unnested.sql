SELECT
	place_id,
	t1.key AS category,
	t2.key AS feature,
	CASE
		WHEN t2.value = 'true'
			THEN NULL::TEXT
		ELSE t2.value
	END AS feature_description
FROM {{ ref('company_profiles_google_maps_typed') }},
	json_each_text(about) AS t1,
	json_each_text(t1.value::json) AS t2
WHERE t2.value <> 'false'