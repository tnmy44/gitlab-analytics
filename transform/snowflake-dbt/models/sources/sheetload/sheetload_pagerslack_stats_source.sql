WITH source AS (

    SELECT *
    FROM {{ ref('sheetload','pagerslack_stats') }}

),renamed AS (

    SELECT
        attempts::NUMBER                AS attempts,
        weekend_pinged::BOOLEAN         AS weekend_pinged,
        unavailable::BOOLEAN            AS unavailable,
        incident_url::VARCHAR           AS incident_url,
        reported_by::VARCHAR            AS reported_by,
        reported_at::TIMESTAMP          AS reported_at,
        time_to_response::NUMBER        AS time_to_response,
        time_at_response::TIMESTAMP     AS time_at_response,
        1                               AS escalations,
        CASE
            WHEN date_part('hour', time_at_response::TIMESTAMP) > 0
                AND date_part('hour', time_at_response::TIMESTAMP) < 8
            THEN 'APAC'
            WHEN date_part('hour', time_at_response::TIMESTAMP) >= 8
                AND date_part('hour', time_at_response::TIMESTAMP) <= 16
            THEN 'EMEA'
            ELSE 'AMER'
        END                             AS timezone,
        time_to_response::NUMBER/60000  AS minutes_to_response,
        iff(unavailable::BOOLEAN=True, 'Response', 'No response')
                                        AS response_type_copy,
        iff(unavailable::BOOLEAN=True, 'Escalated', 'Bot')
                                        AS response_type
    FROM
        source

)

SELECT *
FROM renamed
