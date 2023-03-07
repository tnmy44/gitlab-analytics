{{
  config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=["mnpi_exception"]
  )
}}

WITH redis_clicks AS (
  SELECT
    behavior_structured_event_pk,
    behavior_at, 
    gsc_pseudonymized_user_id,
    dim_namespace_id,
    dim_project_id,
    gsc_plan
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2022-11-01' -- no events added to SP context before Nov 2022

  {% if is_incremental() %}
  
    AND behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})
  
  {% endif %}
),

namespaces_hist AS (
  SELECT
    *
  FROM {{ ref('gitlab_dotcom_namespace_lineage_scd') }}
),

contexts AS (
  SELECT
    *
  FROM {{ ref('fct_behavior_structured_event_service_ping_context') }}
),

final AS (
  SELECT
    redis_clicks.behavior_structured_event_pk,
    redis_clicks.behavior_at,
    redis_clicks.gsc_pseudonymized_user_id,
    redis_clicks.dim_namespace_id,
    redis_clicks.dim_project_id,
    redis_clicks.gsc_plan,
    namespaces_hist.ultimate_parent_id AS ultimate_parent_namespace_id,
    contexts.redis_event_name,
    contexts.key_path,
    contexts.data_source
  FROM redis_clicks
  INNER JOIN contexts ON contexts.behavior_structured_event_pk = redis_clicks.behavior_structured_event_pk
  LEFT JOIN namespaces_hist ON namespaces_hist.namespace_id = redis_clicks.dim_namespace_id
    AND redis_clicks.behavior_at BETWEEN namespaces_hist.lineage_valid_from AND namespaces_hist.lineage_valid_to
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-21",
    updated_date="2023-02-17"
) }}