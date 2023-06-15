{{ config(
    materialized="incremental",
    unique_key="id",
    on_schema_change='append_new_columns'
    )
}}

WITH source AS (
  SELECT
    PARSE_JSON(value) AS json_value,
    -- obtain 'uploaded_at_gcs' from the parquet filename, i.e:
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
    )                 AS uploaded_at_gcs

  FROM
    {{ source('omamori', 'users_external') }}

  {% if is_incremental() %}
    -- Filter only for records from new files based off uploaded_at_gcs
    -- but first filter on the parititioned column (date_part) to speed up query
    WHERE date_part >= (SELECT COALESCE(MAX(uploaded_at_gcs)::DATE, '1970-01-01') AS last_uploaded_at_gcs FROM {{ this }})
      AND uploaded_at_gcs > (SELECT COALESCE(MAX(uploaded_at_gcs), '1970-01-01') AS last_uploaded_at_gcs FROM {{ this }})
  {% endif %}
),

renamed AS (
  SELECT


    json_value['id']::INT                                          AS id,
    json_value['username']::VARCHAR                                AS username,
    json_value['name']::VARCHAR                                    AS full_name,
    json_value['role']::VARCHAR                                    AS user_role,
    json_value['service_user']::BOOLEAN                            AS service_user,

    (json_value['created_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS created_at,
    (json_value['updated_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS updated_at,
    uploaded_at_gcs
  FROM source
),

dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped
