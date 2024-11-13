SELECT
	place_id,
    monday_hours,
	tuesday_hours,
	wednesday_hours,
	thursday_hours,
	friday_hours,
	saturday_hours,
	sunday_hours
FROM {{ ref('company_profiles_working_hours_final') }}