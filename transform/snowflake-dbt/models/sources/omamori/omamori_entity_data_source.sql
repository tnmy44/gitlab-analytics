{{ config(
    materialized="incremental",
    unique_key="id",
    on_schema_change='append_new_columns'
    )
}}

WITH max_uploaded_at_source as (
  SELECT MAX(t.uploaded_at) uploaded_at FROM {{ this }} AS t
),
source AS (
  SELECT
    PARSE_JSON(value) AS value,
    to_date(split_part(metadata$filename, '/', 3),'YYYYMMDD') AS date_part,
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
    ) uploaded_at

 FROM
    {{ source('omamori', 'entity_data_external') }}

  {% if is_incremental() %}
    -- Filter only for records from new files based off uploaded_at
    -- but first filter on the parititioned column to speed up query
    WHERE date_part >= (SELECT uploaded_at::DATE FROM max_uploaded_at_source)
    AND uploaded_at > (SELECT uploaded_at FROM max_uploaded_at_source)
  {% endif %}
),

renamed AS (
  value['id']::varchar AS id,
  value['entity_id']::varchar AS entity_id,
  value['entity_type']::varchar AS entity_type,
  value['created_at']::varchar AS created_at,
  value['updated_at']::varchar AS updated_at,
  uploaded_at

  from source
)
dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped
