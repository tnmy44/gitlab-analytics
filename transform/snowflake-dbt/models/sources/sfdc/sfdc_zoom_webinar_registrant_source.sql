
{{ config(
    tags=["mnpi_exception"]
) }}

WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'zoom_webinar_registrant') }}
)
select *
from base
