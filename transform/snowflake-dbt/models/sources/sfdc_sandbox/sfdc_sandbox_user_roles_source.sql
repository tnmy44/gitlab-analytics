WITH base AS (

    SELECT * 
    FROM {{ source('salesforce_sandbox', 'user_role') }}

), renamed AS (

    SELECT
      id                    AS user_role_id,
      name                  AS user_role_name,

      --metadata
      lastmodifiedbyid      AS last_modified_id,
      lastmodifieddate      AS last_modified_date,
      systemmodstamp

    FROM base
)

SELECT *
FROM renamed

UNION ALL

SELECT 
  id,
  name,
  lastmodifiedbyid,
  lastmodifieddate,
  systemmodstamp
FROM {{ ref ('fy25_sfdc_sandbox_user_roles_hierarchy') }}