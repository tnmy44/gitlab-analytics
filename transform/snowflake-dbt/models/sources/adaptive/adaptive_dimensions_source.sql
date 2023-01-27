-- no merge key, behaves as append
{{ config(
    materialized="incremental",
    incremental_strategy='merge'
    )
}}
WITH source AS (
  SELECT
    _data,
    __loaded_at AS uploaded_at
  FROM {{ source('adaptive', 'dimensions') }}
  -- when selecting all records, there's a timesout using XL WH
  -- Select 45 most recent records, this is for display purposes
  ORDER BY uploaded_at DESC
  LIMIT 45
),

intermediate AS (
  SELECT * FROM source
  {% if is_incremental() %}
    WHERE source.uploaded_at > (SELECT MAX(t.uploaded_at) FROM {{ this }} AS t)
  {% endif %}
)

SELECT
  PARSE_JSON(intermediate._data) ['@id']::VARCHAR    AS parent_id,
  PARSE_JSON(intermediate._data) ['@name']::VARCHAR  AS parent_name,
  PARSE_JSON(intermediate._data) ['@seqNo']::VARCHAR AS parent_seq_num,
  dimension_values.value['@id']::VARCHAR             AS id,
  dimension_values.value['@name']::VARCHAR           AS dimension_name,
  dimension_values.value['@description']::VARCHAR    AS description,
  dimension_values.value['@shortName']::VARCHAR      AS short_name,
  dimension_values.value['attributes']::VARIANT      AS attributes,
  intermediate.__loaded_at                           AS uploaded_at
FROM
  intermediate,
  LATERAL FLATTEN(input => PARSE_JSON(intermediate._data) ['dimensionValue']) AS dimension_values
