
{{ config(
    tags=["mnpi"]
) }}

WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'zoom_webinar_registrant') }}
)
select *
from base
