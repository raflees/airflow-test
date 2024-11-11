SELECT
    place_id,
    trim(CAST(working_hours -> 'Monday' AS TEXT), '""') AS monday,
    trim(CAST(working_hours -> 'Tuesday' AS TEXT), '""') AS tuesday,
    trim(CAST(working_hours -> 'Wednesday' AS TEXT), '""') AS wednesday,
    trim(CAST(working_hours -> 'Thursday' AS TEXT), '""') AS thursday,
    trim(CAST(working_hours -> 'Friday' AS TEXT), '""') AS friday,
    trim(CAST(working_hours -> 'Saturday' AS TEXT), '""') AS saturday,
    trim(CAST(working_hours -> 'Sunday' AS TEXT), '""') AS sunday
FROM {{ ref('company_profiles_google_maps_typed') }}