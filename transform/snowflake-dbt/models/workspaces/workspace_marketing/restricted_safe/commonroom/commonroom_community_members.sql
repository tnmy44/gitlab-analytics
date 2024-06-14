WITH source AS
(

  SELECT *
  FROM {{ ref('commonroom_community_members_source') }}

)

SELECT *
  FROM base;