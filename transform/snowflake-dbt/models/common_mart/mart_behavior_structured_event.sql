{{ config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['product'],
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    cluster_by=['behavior_at::DATE']
  )

}}

{{ simple_cte([
    ('event', 'dim_behavior_event'),
    ('namespace', 'dim_namespace'),
    ('project', 'dim_project'),
    ('operating_system', 'dim_behavior_operating_system'),
    ('dates', 'dim_date')
]) }},

structured_behavior AS (

  SELECT
    behavior_structured_event_pk,
    behavior_at,
    app_id,
    user_snowplow_domain_id,
    contexts,
    page_url_path,
    page_url_fragment,
    gsc_google_analytics_client_id,
    gsc_pseudonymized_user_id,
    gsc_extra,
    gsc_plan,
    gsc_source,
    event_value,
    session_index,
    session_id,
    dim_behavior_referrer_page_sk,
    dim_behavior_event_sk,
    dim_namespace_id,
    dim_project_id,
    dim_behavior_operating_system_sk
  FROM {{ ref('fct_behavior_structured_event') }}
  {% if is_incremental() %}

    WHERE behavior_at > (SELECT MAX({{ var('incremental_backfill_date', 'behavior_at') }}) FROM {{ this }})
      AND behavior_at <= (SELECT DATEADD(MONTH, 1, MAX({{ var('incremental_backfill_date', 'behavior_at') }})) FROM {{ this }})

  {% else %}
  -- This will cover the first creation of the table and requires that the table be backfilled
  WHERE behavior_at > DATE_TRUNC('month', CURRENT_DATE() )

  {% endif %}

),

report AS (
  SELECT
    structured_behavior.behavior_structured_event_pk,
    structured_behavior.behavior_at,
    dates.date_actual AS behavior_date,
    structured_behavior.app_id,
    structured_behavior.user_snowplow_domain_id,
    structured_behavior.contexts,
    structured_behavior.page_url_path,
    structured_behavior.page_url_fragment,
    structured_behavior.gsc_google_analytics_client_id,
    structured_behavior.gsc_pseudonymized_user_id,
    structured_behavior.gsc_extra,
    structured_behavior.gsc_plan,
    structured_behavior.gsc_source,
    structured_behavior.event_value,
    structured_behavior.session_index,
    structured_behavior.session_id,
    event.event_category,
    event.event_action,
    event.event_label,
    event.event_property,
    namespace.dim_namespace_id,
    namespace.ultimate_parent_namespace_id,
    namespace.namespace_is_internal,
    namespace.namespace_is_ultimate_parent,
    namespace.namespace_type,
    namespace.visibility_level,
    project.dim_project_id,
    operating_system.device_type,
    operating_system.is_device_mobile,
    structured_behavior.dim_behavior_referrer_page_sk
  FROM structured_behavior
  LEFT JOIN event
    ON structured_behavior.dim_behavior_event_sk = event.dim_behavior_event_sk
  LEFT JOIN namespace
    ON structured_behavior.dim_namespace_id = namespace.dim_namespace_id
  LEFT JOIN project
    ON structured_behavior.dim_project_id = project.dim_project_id
  LEFT JOIN operating_system
    ON structured_behavior.dim_behavior_operating_system_sk = operating_system.dim_behavior_operating_system_sk
  LEFT JOIN dates
    ON{{ get_date_id('structured_behavior.behavior_at') }} = dates.date_id
)

{{ dbt_audit(
    cte_ref="report",
    created_by="@pempey",
    updated_by="@pempey",
    created_date="2023-02-22",
    updated_date="2023-02-22"
) }}