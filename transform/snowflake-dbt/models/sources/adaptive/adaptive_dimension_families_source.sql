WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'dimension_families') }}
),

parsed AS (
  SELECT
    PARSE_JSON(_data) AS data,
    __loaded_at
  FROM
    source
)

SELECT
  data['@accounts']::varchar   AS accounts,
  data['@dimensions']::varchar AS dimensions,
  data['@name']::varchar       AS name,
  data['@id']::varchar         AS id,
  __loaded_at
FROM parsed
