{{ config(
    tags=["commonroom"]
) }}

WITH source AS
(

    SELECT *
    FROM {{ ref('commonroom_activities_source') }}

), source_pii AS (

    SELECT {{ nohash_sensitive_columns('commonroom_organizations_source', 'organization_name') }}
    FROM source
)

SELECT *
  FROM source_pii