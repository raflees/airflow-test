SELECT
    place_id,
    TRIM(working_hours ->> 'Monday', '""') AS monday,
    TRIM(working_hours ->> 'Tuesday', '""') AS tuesday,
    TRIM(working_hours ->> 'Wednesday', '""') AS wednesday,
    TRIM(working_hours ->> 'Thursday', '""') AS thursday,
    TRIM(working_hours ->> 'Friday', '""') AS friday,
    TRIM(working_hours ->> 'Saturday', '""') AS saturday,
    TRIM(working_hours ->> 'Sunday', '""') AS sunday
FROM {{ ref('company_profiles_google_maps_typed') }}