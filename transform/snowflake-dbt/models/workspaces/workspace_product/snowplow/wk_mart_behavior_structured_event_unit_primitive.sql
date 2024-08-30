{{ config(
    tags=['product']
) }}

{{ simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('dim_behavior_event', 'dim_behavior_event'),
    ('dim_installation', 'dim_installation'),
    ('dim_namespace', 'dim_namespace'),
    ('dim_app_release_major_minor', 'dim_app_release_major_minor'),
    ('wk_ping_installation_latest', 'wk_ping_installation_latest')
    ])
}}

, up_events AS (

  SELECT
    fct_behavior_structured_event.*,
    dim_behavior_event.event,
    dim_behavior_event.event_name,
    dim_behavior_event.platform,
    dim_behavior_event.environment,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_property,
    SPLIT_PART(dim_behavior_event.event_action, 'request_', 2) AS unit_primitive
  FROM fct_behavior_structured_event
  LEFT JOIN dim_behavior_event
    ON fct_behavior_structured_event.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
  WHERE behavior_at >= '2024-07-29' 
    AND event_action LIKE 'request_%'
    AND app_id = 'gitlab_ai_gateway'

), flattened AS (

  SELECT 
    up_events.*,
    flattened_namespace.value::VARCHAR AS enabled_by_namespace_id
  FROM up_events,
  LATERAL FLATTEN(input => TRY_PARSE_JSON(up_events.gsc_feature_enabled_by_namespace_ids), outer => TRUE) AS flattened_namespace
    
), flattened_with_installation_id AS (

  SELECT
    flattened.*,
    dim_installation.dim_installation_id,
    dim_installation.product_delivery_type      AS enabled_by_product_delivery_type,
    dim_installation.product_deployment_type    AS enabled_by_product_deployment_type
  FROM flattened
  LEFT JOIN dim_installation
    ON flattened.dim_instance_id = dim_installation.dim_instance_id
      AND flattened.host_name = dim_installation.host_name

), joined AS (

  SELECT
    -- primary key
    flattened_with_installation_id.behavior_structured_event_pk,

    -- foreign keys
    flattened_with_installation_id.dim_behavior_event_sk,
    flattened_with_installation_id.dim_user_location_sk,
    dim_app_release_major_minor.dim_app_release_major_minor_sk,
    flattened_with_installation_id.dim_installation_id,
    flattened_with_installation_id.gsc_feature_enabled_by_namespace_ids,
    flattened_with_installation_id.enabled_by_namespace_id,
    dim_namespace.ultimate_parent_namespace_id                              AS enabled_by_ultimate_parent_namespace_id,

    -- dates
    flattened_with_installation_id.behavior_at,

    -- degenerate dimensions
    flattened_with_installation_id.dim_instance_id,
    flattened_with_installation_id.host_name,
    wk_ping_installation_latest.latest_is_internal_installation             AS installation_is_internal,
    dim_namespace.namespace_is_internal                                     AS enabled_by_internal_namespace,
    flattened_with_installation_id.enabled_by_product_delivery_type,
    flattened_with_installation_id.enabled_by_product_deployment_type,
    flattened_with_installation_id.gsc_pseudonymized_user_id,
    flattened_with_installation_id.gitlab_global_user_id,
    flattened_with_installation_id.user_snowplow_domain_id,
    flattened_with_installation_id.app_id,
    flattened_with_installation_id.is_staging_event,

    -- standard context attributes
    flattened_with_installation_id.contexts,
    flattened_with_installation_id.gitlab_standard_context,
    flattened_with_installation_id.gsc_environment,
    flattened_with_installation_id.gsc_source,
    flattened_with_installation_id.gsc_plan,
    flattened_with_installation_id.gsc_google_analytics_client_id,
    flattened_with_installation_id.delivery_type,
    flattened_with_installation_id.gsc_correlation_id,
    flattened_with_installation_id.gsc_extra,
    flattened_with_installation_id.gsc_is_gitlab_team_member,
    flattened_with_installation_id.gsc_instance_version,
    dim_app_release_major_minor.major_minor_version                       AS enabled_by_major_minor_version_at_event_time, 
    dim_app_release_major_minor.major_minor_version_num                   AS enabled_by_major_minor_version_num_at_event_time, 

    -- user attributes
    flattened_with_installation_id.user_city,
    flattened_with_installation_id.user_country,
    flattened_with_installation_id.user_region,
    flattened_with_installation_id.user_timezone_name,
    flattened_with_installation_id.user_region_name,

    -- event attributes
    flattened_with_installation_id.event_value,
    flattened_with_installation_id.event_category,
    flattened_with_installation_id.event_action,
    flattened_with_installation_id.event_label,
    flattened_with_installation_id.clean_event_label,
    flattened_with_installation_id.event_property,
    flattened_with_installation_id.unit_primitive

  FROM flattened_with_installation_id
  LEFT JOIN dim_namespace
    ON flattened_with_installation_id.enabled_by_namespace_id = dim_namespace.dim_namespace_id
  LEFT JOIN dim_app_release_major_minor
    ON regexp_substr(flattened_with_installation_id.gsc_instance_version,'(.*)[.]',1, 1, 'e') = dim_app_release_major_minor.major_minor_version
  LEFT JOIN wk_ping_installation_latest
    ON flattened_with_installation_id.dim_installation_id = wk_ping_installation_latest.dim_installation_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-08-29",
    updated_date="2024-08-29"
) }}

