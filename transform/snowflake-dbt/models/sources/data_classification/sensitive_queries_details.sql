{{ config({
    "materialized": "incremental",
    "tags": ["data_classification", "mnpi_exception"]
    })
}}

WITH mnpi_tags AS (

  SELECT 'MNPI'          AS classification_type,
         tag_name        AS tag_name,
         tag_value       AS tag_value,
         object_database AS object_database,
         object_schema   AS object_schema,
         object_name     AS object_table,
         NULL            AS column_name
  FROM {{ source('snowflake', 'tag_references') }}
  WHERE tag_name  = 'MNPI_DATA'
  AND tag_value = 'yes'

), pii_tags AS (

SELECT 'PII'           AS classification_type,
       tag_name        AS tag_name,
       tag_value       AS tag_value,
       object_database AS object_database,
       object_schema   AS object_schema,
       object_name     AS object_table,
       column_name     AS column_name
  FROM {{ source('snowflake', 'tag_references') }}
  WHERE tag_database = 'SNOWFLAKE'
  AND tag_schema   = 'CORE'

), tags AS (

  SELECT classification_type,
         tag_name,
         tag_value,
         object_database,
         object_schema,
         object_table,
         column_name
  FROM mnpi_tags
  UNION
  SELECT classification_type,
         tag_name,
         tag_value,
         object_database,
         object_schema,
         object_table,
         column_name
  FROM pii_tags
), access_history AS (

  SELECT access_history.query_id                    AS query_id,
         access_history.user_name                   AS user_name,
         base_objects.value:objectName::VARCHAR     AS accessed_table,
         base_columns.value:columnName::VARCHAR     AS accessed_column
  FROM {{ source('snowflake', 'access_history') }} AS access_history,
  LATERAL FLATTEN (input => access_history.BASE_OBJECTS_ACCESSED) AS base_objects,
  LATERAL FLATTEN (input => base_objects.value:columns)  AS base_columns

  {% if is_incremental() %}

  WHERE access_history.query_start_time > (SELECT MAX(query_start_time) FROM {{this}})

  {% endif %}

), query_history AS (

  SELECT query_history.query_id         AS query_id,
         query_history.query_text       AS query_text,
         query_history.database_name    AS database_name,
         query_history.schema_name      AS schema_name,
         query_history.query_type       AS query_type,
         query_history.user_name        AS user_name,
         query_history.role_name        AS role_name,
         query_history.execution_status AS execution_status,
         query_history.start_time       AS start_time,
         query_history.end_tim          AS end_time
  FROM {{ source('snowflake', 'query_history') }} AS query_history
  WHERE query_history.query_type IN ('GET_FILES','SELECT', 'UNLOAD')
  AND query_history.execution_status = 'SUCCESS'

  {% if is_incremental() %}

  WHERE access_history.query_start_time > (SELECT MAX(query_start_time) FROM {{this}})

  {% endif %}

), queries AS (

  SELECT query_history.query_id                            AS query_id,
         access_history.user_name                          AS user_name,
         SPLIT_PART(access_history.accessed_table, '.', 1) AS accessed_database,
         SPLIT_PART(access_history.accessed_table, '.', 2) AS accessed_schema,
         SPLIT_PART(access_history.accessed_table, '.', 3) AS accessed_table,
         access_history.accessed_column                    AS accessed_column,
         query_history.query_text                          AS query_text,
         query_history.database_name                       AS database_name,
         query_history.schema_name                         AS schema_name,
         query_history.query_type                          AS query_type,
         query_history.user_name                           AS user_name,
         query_history.role_name                           AS role_name,
         query_history.execution_status                    AS execution_status,
         query_history.start_time                          AS start_time,
         query_history.end_time                            AS end_time
  FROM query_history
  JOIN access_history
  ON query_history.query_id = access_history.query_id
  AND query_history.user_name = access_history.user_name

)

  SELECT *
  FROM queries