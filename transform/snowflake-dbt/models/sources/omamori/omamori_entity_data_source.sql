{{ config(
    materialized="incremental",
    unique_key="id",
    on_schema_change='append_new_columns'
    )
}}

WITH max_uploaded_at_source AS (
  SELECT COALESCE(MAX(uploaded_at), '1970-01-01')::TIMESTAMP AS last_uploaded_at FROM {{ this }}
),

source AS (
  SELECT
    PARSE_JSON(value) AS json_value,
    -- obtain 'uploaded_at' from the parquet filename, i.e:
    -- 'entity_data/20230502/20230502T000025-entity_data-from-19700101T000000-until-20230426T193740.parquet'
    TO_TIMESTAMP(
      SPLIT_PART(
        SPLIT_PART(
          metadata$filename,
          '/',
          3
        ),
        '-',
        1
      ),
      'YYYYMMDDTHH24MISS'
    )                 AS uploaded_at

  FROM
    {{ source('omamori', 'entity_data_external') }}

  {% if is_incremental() %}
    -- Filter only for records from new files based off uploaded_at
    -- but first filter on the parititioned column (date_part) to speed up query
    WHERE date_part >= (SELECT last_uploaded_at::DATE FROM max_uploaded_at_source)
      AND uploaded_at > (SELECT last_uploaded_at FROM max_uploaded_at_source)
  {% endif %}
),

renamed AS (
  SELECT
    json_value['id']::VARCHAR          AS id,
    json_value['entity_id']::VARCHAR   AS entity_id,
    json_value['entity_type']::VARCHAR AS entity_type,
    json_value['created_at']::VARCHAR  AS created_at,
    json_value['updated_at']::VARCHAR  AS updated_at,
    uploaded_at
  FROM source
),

dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped
