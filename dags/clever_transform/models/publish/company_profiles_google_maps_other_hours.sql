SELECT
	place_id,
    hours_type,
    monday_hours,
    tuesday_hours,
    wednesday_hours,
    thursday_hours,
    friday_hours,
    saturday_hours,
    sunday_hours
FROM {{ ref('company_profiles_other_hours_final') }}