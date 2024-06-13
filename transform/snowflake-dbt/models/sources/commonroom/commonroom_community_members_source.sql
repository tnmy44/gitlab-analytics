WITH source AS
(

SELECT first_activity_date::TIMESTAMP_NTZ AS first_activity_date,
       first_activity_source::VARCHAR     AS first_activity_source,
       full_name::VARCHAR                 AS full_name,
       impact_points_all_time::NUMBER     AS impact_points_all_time,
       last_activity_date::TIMESTAMP_NTZ  AS last_activity_date,
       location::VARCHAR                  AS location,
       member_tokens::VARCHAR             AS member_tokens,
       primary_email::VARCHAR             AS primary_email,
       profiles::VARCHAR                  AS profiles,
       segment_names::VARCHAR             AS segment_names,
       tags::VARCHAR                      AS tags,
       _uploaded_at::TIMESTAMP            AS _uploaded_at
   FROM {{ source('commonroom', 'community_members') }}

)

SELECT *
  FROM base;