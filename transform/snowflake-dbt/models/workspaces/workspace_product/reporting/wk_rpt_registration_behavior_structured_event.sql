{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_behavior_structured_event','mart_behavior_structured_event')
    ])
}},

behavior_structured_event AS (

	SELECT 
		behavior_at,
		behavior_date,
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
		event_category,
		event_action,
		event_label,
		event_property,
		dim_namespace_id,
		ultimate_parent_namespace_id,
		namespace_is_internal,
		namespace_is_ultimate_parent,
		namespace_type,
		visibility_level,
		dim_project_id,
		device_type,
		is_device_mobile,
		dim_behavior_referrer_page_sk,
		has_gitlab_service_ping_context,
		has_gitlab_experiment_context,
		has_customer_standard_context,
		browser_name,
		dim_behavior_browser_sk,
		dim_plan_id,
		plan_id_modified,
		plan_name,
		plan_name_modified
	FROM mart_behavior_structured_event mart
	WHERE behavior_at::DATE >= DATEADD(DAY, -190, CURRENT_DATE::DATE)
	AND LOWER(event_category) LIKE '%registration%'

)

{{ dbt_audit(
    cte_ref="base",
    created_by="@eneuberger",
    updated_by="@eneuberger",
    created_date="2023-04-12",
    updated_date="2023-04-12"
) }}