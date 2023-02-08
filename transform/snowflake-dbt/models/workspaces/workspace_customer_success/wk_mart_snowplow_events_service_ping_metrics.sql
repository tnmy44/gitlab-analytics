{{
  config(
    materialized='incremental',
    tags=["mnpi_exception"]
  )
}}

WITH service_ping_events AS (
  SELECT
    *
  FROM {{ ref('wk_fct_snowplow_events_service_ping') }}

   {% if is_incremental() %}
  
    WHERE behavior_at >= (SELECT MAX(behavior_at) FROM {{this}})
  
  {% endif %}
),

metric_bridge AS (
  SELECT
    *
  FROM {{ ref('bdg_metrics_redis_events') }}
),

joined AS (
  SELECT
    service_ping_events.behavior_structured_event_pk,
    service_ping_events.behavior_at,
    service_ping_events.gsc_pseudonymized_user_id,
    service_ping_events.dim_namespace_id,
    service_ping_events.dim_project_id,
    service_ping_events.gsc_plan,
    service_ping_events.ultimate_parent_namespace_id,
    service_ping_events.key_path,
    COALESCE(metrics_with_events.metrics_path, metrics_without_events.metrics_path) AS metrics_path,
    COALESCE(metrics_with_events.aggregate_operator, metrics_without_events.aggregate_operator) AS aggregate_operator,
    COALESCE(metrics_with_events.aggregate_attribute, metrics_without_events.aggregate_attribute) AS aggregate_attribute,
    COALESCE(metrics_with_events.metrics_status, metrics_without_events.metrics_status) AS metrics_status,
    COALESCE(metrics_with_events.time_frame, metrics_without_events.time_frame) AS time_frame
  FROM service_ping_events
  /*
    Some events need to be joined back to the metrics dictionary on the redis event name because the key path is null,
    and some need to be joined back on the key path because there are no associated redis event names. 
  */
  LEFT JOIN metric_bridge AS metrics_with_events ON service_ping_events.redis_event_name = metrics_with_events.redis_event
  LEFT JOIN metric_bridge AS metrics_without_events ON service_ping_events.key_path = metrics_with_events.metrics_path
  /* 
    Only pull in events that match match one of two conditions:
      1. The redis event sent in the Service Ping context matches an event in the metric yml file
      2. The Service Ping context has a non-null `key_path`. In this case, the `key_path` field
         contains the metric we should count
  */
  WHERE metrics_with_events.redis_event IS NOT NULL
    OR service_ping_events.key_path IS NOT NULL
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-21",
    updated_date="2023-02-08"
) }}