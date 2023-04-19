{{ config({
    "materialized": "incremental",
    "unique_key": "behavior_structured_event_pk"
    })
}}

WITH filtered_snowplow_events AS (

  SELECT
    behavior_at,
    gsc_pseudonymized_user_id,
    event_category,
    event_action,
    CASE 
    WHEN 
    event_label LIKE 'group_dropdown_frequent_items_list_item_%'
    THEN 'group_dropdown_frequent_items_list_item'
    WHEN 
    event_label LIKE 'groups_dropdown_frequent_items_list_item_%'
    THEN 'group_dropdown_frequent_items_list_item'    
    WHEN
    event_label LIKE 'project_dropdown_frequent_items_list_item_%'
    THEN 'project_dropdown_frequent_items_list_item'
    WHEN
    event_label LIKE 'projects_dropdown_frequent_items_list_item_%'
    THEN 'project_dropdown_frequent_items_list_item'
    ELSE event_label 
    END AS event_label,
    event_property,
    gsc_plan,
    device_type,
    behavior_structured_event_pk,
    app_id,
    ultimate_parent_namespace_id,
    session_id,
    gsc_extra
  FROM {{ ref('mart_behavior_structured_event') }}
  WHERE 
  behavior_at >= '2021-10-01'
  AND
  app_id IN ('gitlab', 'gitlab_customers')
  AND
  (
      (
        event_label IN (
          'main_navigation',
          'profile_dropdown',
          'groups_side_navigation',
          'kubernetes_sections_tabs',
          'hamburger_menu',
          'menu_view_all_projects',
          'menu_view_all_groups',
          'menu_milestones',
          'menu_snippets',
          'menu_environments',
          'menu_operations',
          'menu_security',
          'new_dropdown',
          'plus_menu_dropdown',
          'main_navigation',
          'profile_dropdown'
        )
      ) OR
      (
        event_action IN (
          'click_whats_new_drawer',
          'click_forum'
        )
      ) OR
      (
        event_label IN (
          'Menu',
          'groups_dropdown',
          'projects_dropdown'
        )
        AND event_action = 'click_dropdown'
      ) 
      OR
      (
        event_action IN ('click_menu', 'click_menu_item')
        AND 
        (event_category LIKE 'dashboard%' OR event_category LIKE 'root%' OR event_category LIKE 'projects%')
        ) 
      OR
      (
        event_action = 'render' AND event_label = 'user_side_navigation'
      )
    )
    OR
    event_label LIKE ANY ('groups_dropdown_%','project_dropdown_%','group_dropdown_%','projects_dropdown_%')
    OR 
    (
    event_label IN ('packages_registry','container_registry','infrastructure_registry','kubernetes','terraform')
    AND
    event_action = 'click_menu_item'
    AND
    event_category LIKE 'groups%'
    )  
    {% if is_incremental() %}

    AND  behavior_at > (SELECT MAX(behavior_at) FROM {{ this }})

  {% endif %}
)

{{ dbt_audit(
    cte_ref="filtered_snowplow_events",
    created_by="@mdrussell",
    updated_by="@mpetersen",
    created_date="2022-10-11",
    updated_date="2023-02-09"
) }}
