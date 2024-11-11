{{ config(
    alias='company_profiles_other_hours',
    pre_hook="CREATE EXTENSION IF NOT EXISTS tablefunc;",
    indexes=[
        {'columns': ['place_id', 'hours_type'], 'type': 'btree'}
    ]
) }}

-- PIVOTING TABLE AROUND WEEKDAYS

SELECT
    key::JSON ->> 'place_id' AS place_id,
    key::JSON ->> 'hours_type' AS hours_type,
    monday as monday_hours,
    tuesday as tuesday_hours,
    wednesday as wednesday_hours,
    thursday as thursday_hours,
    friday as friday_hours,
    saturday as saturday_hours,
    sunday as sunday_hours
FROM crosstab(
    'select
        JSON_BUILD_OBJECT(''place_id'', place_id, ''hours_type'', hours_type)::TEXT as key,
        weekday,
        hours
    from {{ ref("company_profiles_other_hours_unnested") }}
    where lower(weekday) IN (''friday'', ''monday'', ''saturday'', ''sunday'', ''thursday'', ''tuesday'', ''wednesday'')
    order by 1, 2'
) AS pivot(key text, friday text, monday text, saturday text, sunday text, thursday text, tuesday text, wednesday text)