{{ config({
        "materialized": "table",
        "tags": ["product", "mnpi_exception"]
    })
}}

WITH page_views AS (

    SELECT 
            date_trunc('month', behavior_at) as reporting_month, 
            plan_name_modified,
            gsc_namespace_id as ultimate_parent_namespace_id,
            gsc_project_id,
            CASE 
                WHEN page_url_path LIKE '%ci/editor%' 
                THEN 'pipeline_editor_pageview'
            END as metric, 
            COUNT(DISTINCT fct_behavior_website_page_view_sk) as total_events
        FROM {{ ref('fct_behavior_website_page_view') }} as page_view
        LEFT JOIN {{ ref('dim_plan') }} as plan 
        ON plan.plan_name = page_view.gsc_plan
        LEFT JOIN {{ref ('dim_namespace')}} as namespaces
        ON namespaces.ultimate_parent_namespace_id = page_view.gsc_namespace_id
        WHERE metric IS NOT NULL
        --- filter out any namespaces that are internal and include any data from any records with no namespace
        AND (namespace_is_internal = FALSE OR gsc_namespace_id IS NULL)
        GROUP BY ALL

), structured_events AS (

    SELECT 
        date_trunc('month', behavior_at) as reporting_month, 
        plan_name_modified,
        ultimate_parent_namespace_id,
        dim_project_id as gsc_project_id,
        CASE
        WHEN event_label = 'pipelines_filtered_search'
            AND event_action = 'click_filtered_search'
        THEN 'click_search'
        WHEN event_label = 'pipeline_id_iid_listbox'
        THEN 'pipeline_iid_chart'
        WHEN event_label = 'pipelines_filter_tabs'
            AND event_action = 'click_filter_tabs'
        THEN 'pipeline_table_tab_clicks'
        WHEN event_label = 'pipelines_table_component'
            AND event_action in( 'click_retry_button', 'click_manual_actions', 'click_artifacts_dropdown', 'click_cancel_button')
        THEN 'pipeline_table_action_buttons'
        WHEN event_label = 'pipeline_editor'
            AND event_action = 'browse_catalog'
            AND event_category = 'projects:ci:pipeline_editor:show'
        THEN 'browser_catalog_clicks'
        END as metric,
        COUNT(DISTINCT behavior_structured_event_pk) as total_events
    FROM {{ ref('mart_behavior_structured_event') }}
    WHERE metric IS NOT NULL
    --- filter out any namespaces that are internal and include any data from any records with no namespace
        AND (namespace_is_internal = FALSE OR ultimate_parent_namespace_id IS NULL)
    GROUP BY ALL

), final AS (

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['reporting_month', 'metric', 'ultimate_parent_namespace_id']) }} AS event_reporting_month_pk,
        reporting_month, 
        plan_name_modified,
        ultimate_parent_namespace_id, 
        gsc_project_id,
        metric,
        total_events
    FROM page_views

    UNION ALL 

    SELECT 
        {{ dbt_utils.generate_surrogate_key(['reporting_month', 'metric', 'ultimate_parent_namespace_id']) }} AS event_reporting_month_pk,
        reporting_month, 
        plan_name_modified,
        ultimate_parent_namespace_id, 
        gsc_project_id,
        metric,
        total_events
    FROM structured_events

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@nhervas",
    updated_by="@nhervas",
    created_date="2024-02-15",
    updated_date="2024-02-20"
) }}
