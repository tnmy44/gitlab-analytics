{{
  config(
    materialized='incremental'
  )
}}

WITH filtered_ping_metrics AS (
  SELECT
    dim_installation_id,
    metric_value::TIMESTAMP AS installation_creation_date
  FROM {{ ref('prep_ping_instance_flattened') }}
  WHERE ping_created_at > '2023-03-15' --filtering out records that came before GitLab v15.10, when metric was released. Filter in place for full refresh runs.
    AND metrics_path = 'installation_creation_date'
    AND metric_value != 0 -- 0, when cast to timestamp, returns 1970-01-01
    {% if is_incremental() %}

      AND ping_created_at > (SELECT MAX(ping_created_at) FROM {{ this }})

    {% endif %}
),

agg AS (
  SELECT
    dim_installation_id,
    MIN(installation_creation_date) AS installation_creation_date
  FROM filtered_ping_metrics
  {{ dbt_utils.group_by(n=1) }}
)

{{ dbt_audit(
    cte_ref="agg",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2023-03-31",
    updated_date="2023-03-31"
) }}
