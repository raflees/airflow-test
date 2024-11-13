{{ config(
    alias='company_profiles_working_hours',
    indexes=[
        {'columns': ['place_id'], 'type': 'btree'}
    ]
) }}

SELECT
    place_id,
    monday as monday_hours,
    tuesday as tuesday_hours,
    wednesday as wednesday_hours,
    thursday as thursday_hours,
    friday as friday_hours,
    saturday as saturday_hours,
    sunday as sunday_hours
FROM {{ ref('company_profiles_working_hours_unnested') }}