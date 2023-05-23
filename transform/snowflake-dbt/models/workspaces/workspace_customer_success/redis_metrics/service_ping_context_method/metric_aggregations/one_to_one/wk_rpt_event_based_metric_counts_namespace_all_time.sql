{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

WITH events AS (
  SELECT *
  FROM {{ ref('wk_fct_snowplow_events_service_ping') }}
  /*
  Only include events where `key_path` is not null. When `key_path` is not null,
  this indicates that there is a one-to-one relationship between a metric and a Redis
  event. In these cases, the Service Ping schema is not designed to send the Redis event name,
  it only sends the metric path. Because of this, we use this file to aggregate those events
  correctly. Additionally, because of the aggregation logic in this file, this model should only be
  used to aggregate event-based all-time metrics.
  */
  WHERE key_path IS NOT NULL
),

agg AS (
  SELECT
    DATE_TRUNC('month', events.behavior_at) AS month,
    events.ultimate_parent_namespace_id,
    events.key_path                         AS metrics_path,
    COUNT(*)                                AS monthly_value
  FROM events
  {{ dbt_utils.group_by(n = 3) }}
),

final AS (
  SELECT
    agg.month,
    agg.ultimate_parent_namespace_id,
    agg.metrics_path,
    agg.monthly_value,
    SUM(agg.monthly_value) OVER(
      PARTITION BY
        agg.ultimate_parent_namespace_id,
        agg.metrics_path
      ORDER BY
        agg.month ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_metric_value
  FROM agg
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2023-01-30",
    updated_date="2023-03-08"
) }}
