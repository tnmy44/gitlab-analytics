{{ config(
    tags=["commonroom"]
) }}

WITH source AS
(

    SELECT *
    FROM {{ ref('commonroom_community_members_source') }}

), source_pii AS (

    SELECT {{ nohash_sensitive_columns('commonroom_community_members_source', 'full_name') }}
    FROM source

)

SELECT *
  FROM source_pii