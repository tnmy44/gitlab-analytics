WITH source AS
(

SELECT activity_timestamp::TIMESTAMP_NTZ  AS activity_timestamp,
       activity_type::VARCHAR             AS activity_type,
       first_activity_date::TIMESTAMP_NTZ AS first_activity_date,
       full_name::VARCHAR                 AS full_name,
       member_token::VARCHAR              AS member_token,
       profiles::VARCHAR                  AS profiles,
       service_name::VARCHAR              AS service_name,
       _uploaded_at::TIMESTAMP            AS _uploaded_at,
   FROM {{ source('commonroom', 'activities') }}

)

SELECT *
  FROM base;