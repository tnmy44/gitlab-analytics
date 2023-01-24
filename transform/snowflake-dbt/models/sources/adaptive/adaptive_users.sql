WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'users') }}
),

SELECT
  PARSE_JSON(_data) ['@id']::varchar              AS id,
  PARSE_JSON(_data) ['@login']::varchar           AS login,
  PARSE_JSON(_data) ['@email']::varchar           AS email,
  PARSE_JSON(_data) ['@name']::varchar            AS name,
  PARSE_JSON(_data) ['@permissionSetId']::varchar AS permission_set_id,
  PARSE_JSON(_data) ['@guid']::varchar            AS guid,
  PARSE_JSON(_data) ['@timeZone']::varchar        AS time_zone,
  PARSE_JSON(_data) ['subscriptions']::variant    AS subscriptions,
  __loaded_at
FROM
  source
