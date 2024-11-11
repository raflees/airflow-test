{{ config(
    alias='company_profiles_working_hours',
    indexes=[
        {'columns': ['place_id'], 'type': 'btree'}
    ]
) }}

SELECT
    place_id,
    monday,
    tuesday,
    wednesday,
    thursday,
    friday,
    saturday,
    sunday
FROM {{ ref('company_profiles_working_hours_unnested') }}