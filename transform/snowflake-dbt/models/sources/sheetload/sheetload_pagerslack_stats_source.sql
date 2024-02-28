WITH SOURCE AS ( 

   SELECT *
   FROM {{ source('sheetload','pagerslack_stats') }} 

),renamed AS ( 

    SELECT incident_url::VARCHAR AS incident_url,
           TRY_TO_TIMESTAMP_NTZ("reported_at")::DATE AS reported_at,
           reported_by::VARCHAR AS reported_by,
           response_type::VARCHAR AS response_type,
           response_type_copy::VARCHAR AS response_type_copy,
           TRY_TO_TIMESTAMP_NTZ("time_at_response")::DATE AS time_at_response,
           timezone::VARCHAR AS timezone,
           unavailable::BOOLEAN AS unavailable,
           weekend_pinged::BOOLEAN AS weekend_pinged,
           attempts::NUMBER AS attempts,
           escalations::NUMBER AS escalations,
           minutes_to_response::NUMBER AS minutes_to_response,
           time_to_response::NUMBER AS time_to_response
   FROM source 

   )

SELECT *
FROM source
