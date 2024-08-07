{{ config({
    "materialized": "incremental",
    "tags": ["data_classification", "mnpi_exception"]
    })
}}

WITH sensitive_queries_base AS (

  SELECT *
  FROM {{ ref('sensitive_queries_base') }}

), details AS (

  SELECT sensitive_queries_base.classification_type::VARCHAR  AS classification_type,
         sensitive_queries_base.query_id::VARCHAR             AS query_id,
         sensitive_queries_base.accessed_column::VARCHAR      AS accessed_column,
         sensitive_queries_base.tag_name::VARCHAR             AS tag_name,
         sensitive_queries_base.tag_value::VARCHAR            AS tag_value,
         start_time::TIMESTAMP                                AS start_time
  FROM sensitive_queries_base

  {% if is_incremental() %}

  AND sensitive_queries_base.query_start_time > (SELECT MAX(query_start_time) FROM {{this}})

  {% endif %}

)

  SELECT details.classification_type,
         details.query_id,
         details.accessed_column,
         details.tag_name,
         details.tag_value,
         details.start_time
  FROM details