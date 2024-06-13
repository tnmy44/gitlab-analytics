WITH source AS
(

  SELECT *
  FROM {{ ref('commonroom_organizations_source') }}

)

SELECT *
  FROM base;