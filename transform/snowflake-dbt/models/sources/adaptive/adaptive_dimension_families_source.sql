WITH source AS (
  SELECT * FROM
    {{ source('adaptive', 'dimension_families') }}
),

parsed AS (
  SELECT
    PARSE_JSON(_data) AS dimension_family_data,
    __loaded_at
  FROM
    source
)

SELECT
  dimension_family_data['@accounts']::varchar   AS account_ids,
  dimension_family_data['@dimensions']::varchar AS dimensions,
  dimension_family_data['@name']::varchar       AS dimension_family_name,
  dimension_family_data['@id']::varchar         AS id,
  __loaded_at
FROM parsed
