{{ config({
    "materialized": "incremental",
    "tags": ["data_classification", "mnpi_exception"]
    })
}}

WITH sensitive_queries_base AS (

  SELECT *
  FROM {{ ref('sensitive_queries_base') }}

), grouped AS (

  SELECT sensitive_queries_base.classification_type::VARCHAR         AS classification_type,
         sensitive_queries_base.query_id::VARCHAR                    AS query_id,
         sensitive_queries_base.user_name::VARCHAR                   AS user_name,
         sensitive_queries_base.accessed_database::VARCHAR           AS accessed_database,
         sensitive_queries_base.accessed_schema::VARCHAR             AS accessed_schema,
         sensitive_queries_base.accessed_table::VARCHAR              AS accessed_table,
         sensitive_queries_base.query_text::VARCHAR                  AS query_text,
         sensitive_queries_base.executed_from_database_name::VARCHAR AS executed_from_database_name,
         sensitive_queries_base.executed_from_schema_name::VARCHAR   AS executed_from_schema_name,
         sensitive_queries_base.query_type::VARCHAR                  AS query_type,
         sensitive_queries_base.role_name::VARCHAR                   AS role_name,
         sensitive_queries_base.execution_status::VARCHAR            AS execution_status,
         sensitive_queries_base.start_time::TIMESTAMP                AS start_time,
         sensitive_queries_base.end_time::TIMESTAMP                  AS end_time
  FROM sensitive_queries_base

  {% if is_incremental() %}

  WHERE sensitive_queries_base.query_start_time > (SELECT MAX(query_start_time) FROM {{this}})

  {% endif %}
  GROUP BY ALL

)

  SELECT *
  FROM grouped