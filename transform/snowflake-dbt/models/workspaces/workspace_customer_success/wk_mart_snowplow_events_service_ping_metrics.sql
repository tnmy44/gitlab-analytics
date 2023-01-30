{{
  config(
    materialized='view',
    tags=["mnpi_exception"]
  )
}}

WITH service_ping_events AS (
  SELECT
    *
  FROM {{ ref('wk_fct_snowplow_events_service_ping') }}
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
    metric_bridge.metrics_path,
    metric_bridge.aggregate_operator,
    metric_bridge.aggregate_attribute,
    metric_bridge.metrics_status
  FROM service_ping_events
  LEFT JOIN metric_bridge ON service_ping_events.redis_event_name = metric_bridge.redis_event
  /* 
    Only pull in events that match match one of two conditions:
      1. The redis event sent in the Service Ping context matches an event in the metric yml file
      2. The Service Ping context has a non-null `key_path`. In this case, the `key_path` field
         contains the metric we should count
  */
  WHERE metric_bridge.redis_event IS NOT NULL
    OR service_ping_events.key_path IS NOT NULL
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-21",
    updated_date="2023-01-30"
) }}