{{ config(
    tags=["commonroom"]
) }}

WITH source AS
(

    SELECT *
    FROM {{ ref('commonroom_activities_source') }}

), source_pii AS (

    SELECT {{ nohash_sensitive_columns('commonroom_activities_source', 'member_token') }}
    FROM source
)

SELECT *
  FROM source_pii