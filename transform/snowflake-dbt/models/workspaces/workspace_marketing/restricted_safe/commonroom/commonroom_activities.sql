WITH source AS
(

  SELECT *
  FROM {{ ref('commonroom_activities_source') }}

)

SELECT *
  FROM base;