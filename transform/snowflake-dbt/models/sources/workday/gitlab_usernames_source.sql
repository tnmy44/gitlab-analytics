WITH source AS (

  SELECT * 
  FROM {{ source('workday','gitlab_usernames') }}
  
)

SELECT 

    workday_id::VARCHAR                  AS workday_id,
    effective_date::DATE                 AS effective_date,    
    employee_id::NUMBER                  AS employee_id,
    gitlab_username::VARCHAR             AS gitlab_username,
    date_time_completed::TIMESTAMP_TZ    AS date_time_completed,
    _fivetran_deleted::BOLEAN            AS is_deleted,
    _fivetran_synced::TIMESTAMP_TZ       AS uploaded_at
    
FROM  source
