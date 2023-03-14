{{
  config(
    materialized='incremental',
    unique_key='event_id',
    tags=["mnpi_exception"]
  )
}}

WITH redis_clicks AS (
  SELECT
    *
  FROM {{ ref('snowplow_structured_events_all') }}
  WHERE event_action IN (
    'g_analytics_valuestream',
    'action_active_users_project_repo',
    'push_package',
    'ci_templates_unique',
    'p_terraform_state_api_unique_users',
    'i_search_paid'
  )
),

namespaces_hist AS (
  SELECT
    *
  FROM {{ ref('gitlab_dotcom_namespace_lineage_scd') }}
),

joined AS (
  SELECT
    redis_clicks.event_id,
    redis_clicks.derived_tstamp,
    redis_clicks.event_action,
    redis_clicks.gsc_pseudonymized_user_id,
    redis_clicks.gsc_namespace_id,
    redis_clicks.gsc_project_id,
    redis_clicks.gsc_plan,
    namespaces_hist.ultimate_parent_id AS ultimate_parent_namespace_id
  FROM redis_clicks
  LEFT JOIN namespaces_hist ON namespaces_hist.namespace_id = redis_clicks.gsc_namespace_id
    AND redis_clicks.derived_tstamp BETWEEN namespaces_hist.lineage_valid_from AND namespaces_hist.lineage_valid_to
  {% if is_incremental() %}
  
      WHERE redis_clicks.derived_tstamp >= (SELECT MAX(derived_tstamp) FROM {{this}})
  
  {% endif %}
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-06-06",
    updated_date="2022-01-23"
) }}