WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'users') }}
)

SELECT
  PARSE_JSON(_data) ['@id']::VARCHAR              AS id,
  PARSE_JSON(_data) ['@login']::VARCHAR           AS login,
  PARSE_JSON(_data) ['@email']::VARCHAR           AS user_email,
  PARSE_JSON(_data) ['@name']::VARCHAR            AS user_name,
  PARSE_JSON(_data) ['@permissionSetId']::VARCHAR AS permission_set_id,
  PARSE_JSON(_data) ['@guid']::VARCHAR            AS user_guid,
  PARSE_JSON(_data) ['@timeZone']::VARCHAR        AS time_zone,
  PARSE_JSON(_data) ['subscriptions']::VARIANT    AS subscriptions,
  __loaded_at
FROM
  source
