WITH source AS (

    SELECT *
    FROM {{ ref('activity_attendee') }}

)

SELECT *
FROM source