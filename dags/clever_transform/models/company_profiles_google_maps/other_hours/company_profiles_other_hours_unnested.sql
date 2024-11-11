{{ config(
	materialized='view'
) }}

SELECT
    place_id,
    other_hours_record.key AS hours_type,
    hours_of_day.key AS weekday,
    hours_of_day.value AS hours
FROM {{ ref('company_profiles_google_maps_typed') }},
    json_array_elements(other_hours) AS other_hours_item,
    json_each(other_hours_item.value) AS other_hours_record,
    json_each_text(other_hours_record.value) AS hours_of_day
WHERE other_hours IS NOT NULL