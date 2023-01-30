{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH events AS (
  SELECT
    *
  FROM {{ ref('wk_mart_snowplow_events_service_ping_metrics') }}
  /*
    Only include events where `key_path` is not null. When `key_path` is not null,
    this indicates that there is a one-to-one relationship between a metric and a Redis
    event. In these cases, the Service Ping schema is not designed to send the Redis event name,
    it only sends the metric path. Because of this, we use this file to aggregate those events
    correctly. Additionally, because of the aggregation logic below, this model should only be
    used to aggregate event-based all-time metrics.
  */
  WHERE key_path IS NOT NULL
)

final AS (
  SELECT
    events.ultimate_parent_namespace_id,
    events.key_path AS metrics_path,
    COUNT(*) AS all_time_count
  FROM events
  {{ dbt_utils.group_by(n = 2) }}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2023-01-30",
    updated_date="2023-01-30"
) }}