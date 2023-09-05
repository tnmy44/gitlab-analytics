{{ config({
        "materialized": "table",
        "unique_key": "event_reporting_month_pk",
        "tags": ["product", "mnpi_exception"]
    })
}}

WITH structured_events AS (

    SELECT 
        DATE_TRUNC('month', behavior_date) as reporting_month, 
        event_category,
        dim_namespace_id as gsc_namespace_id,
        dim_project_id as gsc_project_id,
        gsc_pseudonymized_user_id,
        plan_name_modified,
        CASE 
        WHEN event_action = 'pull_package' 
        THEN 'pull_package'
        WHEN event_action = 'push_package'
        THEN 'push_package'
        WHEN event_action = 'delete_package_file' or event_action = 'delete_package_files'
        THEN 'delete_package_files'
        WHEN event_action = 'delete_package'
        THEN 'delete_package'
        WHEN event_action = 'npm_request_forward'
        THEN 'npm_request_forward'
        WHEN event_action = 'copy_pypi_request_forward'
        THEN 'pypi_request_forward'
        WHEN event_action = 'list_package'
        THEN 'list_package'
        WHEN event_action IN ('copy_conan_command', 'copy_conan_setup_xml')
            AND event_label = 'code_instruction'
        THEN 'conan_code_snippet'
        WHEN event_action = 'confirm_delete'
        THEN 'container_registry_ui_delete_actions'
        WHEN event_action = 'register_package'
        THEN 'register_package' 
        WHEN event_action IN ('bulk_registry_tag_delete', 'registry_repository_delete', 'registry_tag_delete')
        THEN 'ui_delete'
        WHEN event_category = 'container_registry:notification'
        THEN 'other_container_registry_events'
        WHEN event_category = 'projects:registry:repositories:index'
            AND event_label = 'quickstart_dropdown'
        THEN 'quick_setup'
        WHEN event_label = 'docker_container_retention_and_expiration_policies'
        THEN 'docker_container_retention_and_expiration_policies' 
        END as metric,
        COUNT(DISTINCT behavior_structured_event_pk) as total_events
    FROM {{ ref('mart_behavior_structured_event') }}
    WHERE metric IS NOT NULL
      AND namespace_is_internal = FALSE
  {{ dbt_utils.group_by(n=3) }}

), pageviews AS (

    SELECT 
        date_trunc('month', behavior_at) as reporting_month, 
        NULL as event_category,
        plan_name_modified,
        gsc_namespace_id,
        gsc_project_id,
        gsc_pseudonymized_user_id,
        CASE 
            WHEN page_url_path LIKE '%/container_registry%'
            THEN 'page_view_container_registry'
            WHEN page_url_path LIKE '%/container_registries%'
            THEN 'page_view_container_registries'
            WHEN page_url_path LIKE '%/packages'
            THEN 'page_view_packages'
            WHEN page_url_path LIKE '%/groups%'
                AND page_url_path LIKE '%container_regist%'
            THEN 'page_view_container_registry_ui'
        END as metric, 
        COUNT(DISTINCT fct_behavior_website_page_view_sk) as total_events
    FROM {{ ref('fct_behavior_website_page_view') }}
    WHERE metric IS NOT NULL
      AND 
  {{ dbt_utils.group_by(n=3) }}

), final AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['reporting_month', 'metric']) }} AS event_reporting_month_pk,
    reporting_month, 
    event_category, 
    plan_name_modified,
    gsc_namespace_id,
    gsc_project_id,
    gsc_pseudonymized_user_id,
    total_events 
  FROM structured_events

UNION ALL 

  SELECT 
    {{ dbt_utils.surrogate_key(['reporting_month', 'metric']) }} AS event_reporting_month_pk,
    reporting_month, 
    event_category, 
    plan_name_modified,
    gsc_namespace_id,
    gsc_project_id,
    gsc_pseudonymized_user_id,
    total_events 
  FROM pageviews
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@nhervas",
    updated_by="@nhervas",
    created_date="2023-09-05",
    updated_date="2023-09-05"
) }}
