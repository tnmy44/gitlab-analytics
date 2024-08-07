{{ config({
    "tags": ["data_classification", "mnpi_exception"]
    })
}}

WITH mnpi_tags AS (

  SELECT 'MNPI'::VARCHAR          AS classification_type,
         tag_name::VARCHAR        AS tag_name,
         tag_value::VARCHAR       AS tag_value,
         object_database::VARCHAR AS accessed_database,
         object_schema::VARCHAR   AS accessed_schema,
         object_name::VARCHAR     AS accessed_table,
         NULL                     AS accessed_column
  FROM {{ source('snowflake_account_usage', 'tag_references') }}
  WHERE tag_name  = 'MNPI_DATA'
  AND tag_value = 'yes'

), pii_tags AS (

SELECT 'PII'::VARCHAR           AS classification_type,
       tag_name::VARCHAR        AS tag_name,
       tag_value::VARCHAR       AS tag_value,
       object_database::VARCHAR AS accessed_database,
       object_schema::VARCHAR   AS accessed_schema,
       object_name::VARCHAR     AS accessed_table,
       column_name::VARCHAR     AS accessed_column
  FROM {{ source('snowflake_account_usage', 'tag_references') }}
  WHERE tag_database = 'SNOWFLAKE'
  AND tag_schema   = 'CORE'

), tags AS (

  SELECT classification_type,
         tag_name,
         tag_value,
         accessed_database,
         accessed_schema,
         accessed_table,
         accessed_column
  FROM mnpi_tags
  UNION
  SELECT classification_type,
         tag_name,
         tag_value,
         accessed_database,
         accessed_schema,
         accessed_table,
         accessed_column
  FROM pii_tags

)

  SELECT *
  FROM tags