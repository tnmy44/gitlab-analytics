{{ config({
    "materialized": "incremental",
    "tags": ["data_classification", "mnpi_exception"]
    })
}}

WITH sensitive_data_tags AS (

  SELECT *
  FROM {{ ref('sensitive_queries_tags') }}

), access_history AS (

  SELECT access_history.query_id                    AS query_id,
         access_history.user_name                   AS user_name,
         base_objects.value:objectName::VARCHAR     AS accessed_table,
         base_columns.value:columnName::VARCHAR     AS accessed_column
  FROM {{ source('snowflake_account_usage', 'access_history') }} AS access_history,
  LATERAL FLATTEN (input => access_history.BASE_OBJECTS_ACCESSED) AS base_objects,
  LATERAL FLATTEN (input => base_objects.value:columns)  AS base_columns
  WHERE access_history.query_start_time >= '2024-06-07 00:00:00'::TIMESTAMP
  AND access_history.user_name = 'RBACOVIC'

  {% if is_incremental() %}

  AND access_history.query_start_time > (SELECT MAX(query_start_time) FROM {{this}})

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
         query_history.end_time         AS end_time
  FROM {{ source('snowflake_account_usage', 'query_history') }} AS query_history
  WHERE query_history.query_type IN ('GET_FILES','SELECT', 'UNLOAD')
  AND query_history.execution_status = 'SUCCESS'
  AND query_history.start_time >= '2024-06-07 00:00:00'::TIMESTAMP
  AND query_history.user_name = 'RBACOVIC'
  AND query_history.role_name = 'RBACOVIC'

  {% if is_incremental() %}

  AND access_history.query_start_time > (SELECT MAX(query_start_time) FROM {{this}})

  {% endif %}

), queries AS (

  SELECT query_history.query_id                            AS query_id,
         SPLIT_PART(access_history.accessed_table, '.', 1) AS accessed_database,
         SPLIT_PART(access_history.accessed_table, '.', 2) AS accessed_schema,
         SPLIT_PART(access_history.accessed_table, '.', 3) AS accessed_table,
         access_history.accessed_column                    AS accessed_column,
         query_history.query_text,
         query_history.database_name executed_from_database_name,
         query_history.schema_name   executed_from_schema_name,
         query_history.query_type,
         query_history.user_name,
         query_history.role_name,
         query_history.execution_status,
         query_history.start_time,
         query_history.end_time
    FROM query_history
    JOIN access_history
    ON query_history.query_id  = access_history.query_id
    AND query_history.user_name = access_history.user_name

), joined AS (

  SELECT sensitive_data_tags.classification_type::VARCHAR AS classification_type,
         queries.query_id::VARCHAR                        AS query_id,
         queries.user_name::VARCHAR                       AS user_name,
         queries.accessed_database::VARCHAR               AS accessed_database,
         queries.accessed_schema::VARCHAR                 AS accessed_schema,
         queries.accessed_table::VARCHAR                  AS accessed_table,
         queries.query_text::VARCHAR                      AS query_text,
         queries.executed_from_database_name::VARCHAR     AS executed_from_database_name,
         queries.executed_from_schema_name::VARCHAR       AS executed_from_schema_name,
         queries.query_type::VARCHAR                      AS query_type,
         queries.role_name::VARCHAR                       AS role_name,
         queries.execution_status::VARCHAR                AS execution_status,
         queries.start_time::TIMESTAMP                    AS start_time,
         queries.end_time::TIMESTAMP                      AS end_time
         --queries.accessed_column,
         --sensitive_data_tags.tag_name,
         --sensitive_data_tags.tag_value
    FROM queries
    JOIN sensitive_data_tags
    ON queries.accessed_database  = sensitive_data_tags.accessed_database
    AND queries.accessed_schema   = sensitive_data_tags.accessed_schema
    AND queries.accessed_table    = sensitive_data_tags.accessed_table
    AND (
          (classification_type = 'PII'  AND queries.accessed_column = sensitive_data_tags.accessed_column)
       OR (classification_type = 'MNPI' AND 1=1)
      )
    WHERE queries.accessed_database = 'RBACOVIC_PREP'
    AND queries.accessed_schema   = 'BENCHMARK_PII'
    AND queries.accessed_table NOT IN ('SENSITIVE_OBJECTS_CLASSIFICATION','LOG_CLASSIFICATION')
    GROUP BY ALL

)

  SELECT joined.classification_type,
         joined.query_id,
         joined.user_name,
         joined.accessed_database,
         joined.accessed_schema,
         joined.accessed_table,
         joined.query_text,
         joined.executed_from_database_name,
         joined.executed_from_schema_name,
         joined.query_type,
         joined.role_name,
         joined.execution_status,
         joined.start_time,
         joined.end_time
  FROM joined