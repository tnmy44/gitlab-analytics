WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'dimension_families') }}
),

parsed AS (
  SELECT
    PARSE_JSON(_data) v,
    __loaded_at
  FROM
    source
)

SELECT
  v['@accounts']::varchar   AS accounts,
  v['@dimensions']::varchar AS dimensions,
  v['@name']::varchar       AS name,
  v['@id']::varchar         AS id,
  __loaded_at
FROM parsed
