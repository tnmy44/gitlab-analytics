{{ config(materialized='table') }}

{{ simple_cte([
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('mart_arr_all', 'mart_arr_with_zero_dollar_charges')
])}}

, product_usage_primary_instance AS (

SELECT
   * 
FROM
   rpt_product_usage_health_score 
WHERE
   is_primary_instance_subscription = true
) 

, final as (

SELECT

--mart_arr_all columns
    mart_arr_all.primary_key,
    mart_arr_all.arr_month,
    mart_arr_all.dim_billing_account_id,
    mart_arr_all.dim_crm_account_id,
    mart_arr_all.dim_subscription_id,
    mart_arr_all.dim_product_detail_id,
    mart_arr_all.fiscal_quarter_name_fy,
    mart_arr_all.fiscal_year,
    mart_arr_all.subscription_start_month,
    mart_arr_all.subscription_end_month,
    mart_arr_all.sold_to_country,
    mart_arr_all.billing_account_name,
    mart_arr_all.billing_account_number,
    mart_arr_all.ssp_channel,
    mart_arr_all.po_required,
    mart_arr_all.auto_pay,
    mart_arr_all.default_payment_method_type,
    mart_arr_all.crm_account_name,
    mart_arr_all.dim_parent_crm_account_id,
    mart_arr_all.parent_crm_account_name,
    mart_arr_all.parent_crm_account_sales_segment,
    mart_arr_all.parent_crm_account_industry,
    mart_arr_all.crm_account_employee_count_band,
    mart_arr_all.health_score_color,
    mart_arr_all.health_number,
    mart_arr_all.is_jihu_account,
    mart_arr_all.parent_crm_account_lam,
    mart_arr_all.parent_crm_account_lam_dev_count,
    mart_arr_all.parent_crm_account_business_unit,
    mart_arr_all.parent_crm_account_geo,
    mart_arr_all.parent_crm_account_region,
    mart_arr_all.parent_crm_account_area,
    mart_arr_all.parent_crm_account_role_type,
    mart_arr_all.parent_crm_account_territory,
    mart_arr_all.parent_crm_account_max_family_employee,
    mart_arr_all.parent_crm_account_upa_country,
    mart_arr_all.parent_crm_account_upa_state,
    mart_arr_all.parent_crm_account_upa_city,
    mart_arr_all.parent_crm_account_upa_street,
    mart_arr_all.parent_crm_account_upa_postal_code,
    mart_arr_all.crm_account_employee_count,
    mart_arr_all.dim_subscription_id_original,
    mart_arr_all.subscription_status,
    mart_arr_all.subscription_sales_type,
    mart_arr_all.subscription_name,
    mart_arr_all.subscription_name_slugify,
    mart_arr_all.oldest_subscription_in_cohort,
    mart_arr_all.subscription_lineage,
    mart_arr_all.subscription_cohort_month,
    mart_arr_all.subscription_cohort_quarter,
    mart_arr_all.billing_account_cohort_month,
    mart_arr_all.billing_account_cohort_quarter,
    mart_arr_all.crm_account_cohort_month,
    mart_arr_all.crm_account_cohort_quarter,
    mart_arr_all.parent_account_cohort_month,
    mart_arr_all.parent_account_cohort_quarter,
    mart_arr_all.auto_renew_native_hist,
    mart_arr_all.auto_renew_customerdot_hist,
    mart_arr_all.turn_on_cloud_licensing,
    mart_arr_all.turn_on_operational_metrics,
    mart_arr_all.contract_operational_metrics,
    mart_arr_all.contract_auto_renewal,
    mart_arr_all.turn_on_auto_renewal,
    mart_arr_all.contract_seat_reconciliation,
    mart_arr_all.turn_on_seat_reconciliation,
    mart_arr_all.invoice_owner_account,
    mart_arr_all.creator_account,
    mart_arr_all.was_purchased_through_reseller,
    mart_arr_all.product_tier_name,
    mart_arr_all.product_delivery_type,
    mart_arr_all.product_deployment_type,
    mart_arr_all.product_category,
    mart_arr_all.product_rate_plan_category,
    mart_arr_all.product_ranking,
    mart_arr_all.service_type,
    mart_arr_all.product_rate_plan_name,
    mart_arr_all.is_licensed_user,
    mart_arr_all.is_arpu,
    mart_arr_all.unit_of_measure,
    mart_arr_all.mrr,
    mart_arr_all.arr,
    mart_arr_all.quantity,
    mart_arr_all.months_since_billing_account_cohort_start,
    mart_arr_all.quarters_since_billing_account_cohort_start,
    mart_arr_all.months_since_crm_account_cohort_start,
    mart_arr_all.quarters_since_crm_account_cohort_start,
    mart_arr_all.months_since_parent_account_cohort_start,
    mart_arr_all.quarters_since_parent_account_cohort_start,
    mart_arr_all.months_since_subscription_cohort_start,
    mart_arr_all.quarters_since_subscription_cohort_start,
    mart_arr_all.arr_band_calc,
    mart_arr_all.child_account_base_arr,
    mart_arr_all.child_arr_rank,
    mart_arr_all.is_top_100_child_account_by_arr_month,
    mart_arr_all.is_top_100_child_account_fy25_start,

-- product_usage_primary_instance columns
    product_usage_primary_instance.snapshot_month,
    product_usage_primary_instance.customer_since_date,
    product_usage_primary_instance.account_age_months,
    product_usage_primary_instance.subscription_age_months,
    product_usage_primary_instance.combined_instance_creation_date,
    product_usage_primary_instance.instance_age_days,
    product_usage_primary_instance.instance_age_months,
    product_usage_primary_instance.subscription_start_date,
    product_usage_primary_instance.ultimate_subscription_flag,
    product_usage_primary_instance.is_oss_program,
    product_usage_primary_instance.instance_type,
    product_usage_primary_instance.included_in_health_measures_str,
    product_usage_primary_instance.uuid,
    product_usage_primary_instance.hostname,
    product_usage_primary_instance.dim_namespace_id,
    product_usage_primary_instance.dim_installation_id,
    product_usage_primary_instance.instance_identifier,
    product_usage_primary_instance.hostname_or_namespace_id,
    product_usage_primary_instance.ping_created_at,
    product_usage_primary_instance.cleaned_version,
    product_usage_primary_instance.license_utilization,
    product_usage_primary_instance.billable_user_count,
    product_usage_primary_instance.license_user_count,
    product_usage_primary_instance.license_user_count_source,
    product_usage_primary_instance.license_utilization_score,
    product_usage_primary_instance.license_utilization_color,
    product_usage_primary_instance.last_activity_28_days_user,
    product_usage_primary_instance.user_engagement,
    product_usage_primary_instance.user_engagement_score,
    product_usage_primary_instance.user_engagement_color,
    product_usage_primary_instance.action_monthly_active_users_project_repo_28_days_user_clean,
    product_usage_primary_instance.git_operation_utilization,
    product_usage_primary_instance.scm_score,
    product_usage_primary_instance.scm_color,
    product_usage_primary_instance.ci_pipelines_28_days_user,
    product_usage_primary_instance.ci_pipeline_utilization,
    product_usage_primary_instance.ci_builds_28_days_user,
    product_usage_primary_instance.ci_builds_all_time_user,
    product_usage_primary_instance.ci_builds_all_time_event,
    product_usage_primary_instance.ci_runners_all_time_event,
    product_usage_primary_instance.ci_builds_28_days_event,
    product_usage_primary_instance.ci_pipeline_utilization_score,
    product_usage_primary_instance.ci_pipeline_utilization_color,
    product_usage_primary_instance.ci_score,
    product_usage_primary_instance.ci_color,
    product_usage_primary_instance.deployments_28_days_user,
    product_usage_primary_instance.deployments_28_days_event,
    product_usage_primary_instance.successful_deployments_28_days_event,
    product_usage_primary_instance.failed_deployments_28_days_event,
    product_usage_primary_instance.completed_deployments_l28d,
    product_usage_primary_instance.projects_all_time_event,
    product_usage_primary_instance.environments_all_time_event,
    product_usage_primary_instance.deployments_utilization,
    product_usage_primary_instance.deployments_utilization_score,
    product_usage_primary_instance.deployments_utilization_color,
    product_usage_primary_instance.deployments_per_user_l28d,
    product_usage_primary_instance.deployments_per_user_l28d_score,
    product_usage_primary_instance.deployments_per_user_l28d_color,
    product_usage_primary_instance.successful_deployments_pct,
    product_usage_primary_instance.successful_deployments_pct_score,
    product_usage_primary_instance.successful_deployments_pct_color,
    product_usage_primary_instance.cd_measure_count,
    product_usage_primary_instance.cd_score,
    product_usage_primary_instance.cd_color,
    product_usage_primary_instance.user_unique_users_all_secure_scanners_28_days_user,
    product_usage_primary_instance.ci_internal_pipelines_28_days_event,
    product_usage_primary_instance.secret_detection_scans_28_days_event,
    product_usage_primary_instance.dependency_scanning_scans_28_days_event,
    product_usage_primary_instance.container_scanning_scans_28_days_event,
    product_usage_primary_instance.dast_scans_28_days_event,
    product_usage_primary_instance.sast_scans_28_days_event,
    product_usage_primary_instance.coverage_fuzzing_scans_28_days_event,
    product_usage_primary_instance.api_fuzzing_scans_28_days_event,
    product_usage_primary_instance.sum_of_all_scans_l28d,
    product_usage_primary_instance.secret_detection_scan_percentage,
    product_usage_primary_instance.dependency_scanning_scan_percentage,
    product_usage_primary_instance.container_scanning_scan_percentage,
    product_usage_primary_instance.dast_scan_percentage,
    product_usage_primary_instance.sast_scan_percentage,
    product_usage_primary_instance.coverage_fuzzing_scan_percentage,
    product_usage_primary_instance.api_fuzzing_scan_percentage,
    product_usage_primary_instance.secure_scanners_utilization,
    product_usage_primary_instance.secure_scanners_utilization_score,
    product_usage_primary_instance.secure_scanners_utilization_color,
    product_usage_primary_instance.average_scans_per_pipeline,
    product_usage_primary_instance.average_scans_per_pipeline_score,
    product_usage_primary_instance.average_scans_per_pipeline_color,
    product_usage_primary_instance.secret_detection_usage_flag,
    product_usage_primary_instance.dependency_scanning_usage_flag,
    product_usage_primary_instance.container_scanning_usage_flag,
    product_usage_primary_instance.dast_usage_flag,
    product_usage_primary_instance.sast_usage_flag,
    product_usage_primary_instance.coverage_fuzzing_usage_flag,
    product_usage_primary_instance.api_fuzzing_usage_flag,
    product_usage_primary_instance.number_of_scanner_types,
    product_usage_primary_instance.number_of_scanner_types_score,
    product_usage_primary_instance.number_of_scanner_types_color,
    product_usage_primary_instance.security_measure_count,
    product_usage_primary_instance.security_score,
    product_usage_primary_instance.security_color,
    product_usage_primary_instance.security_score_ultimate_only,
    product_usage_primary_instance.security_color_ultimate_only,
    product_usage_primary_instance.license_utilization_weight,
    product_usage_primary_instance.user_engagement_weight,
    product_usage_primary_instance.scm_weight,
    product_usage_primary_instance.ci_weight,
    product_usage_primary_instance.cd_weight,
    product_usage_primary_instance.security_weight,
    product_usage_primary_instance.remaining_weight,
    product_usage_primary_instance.adjusted_license_utilization_weight,
    product_usage_primary_instance.adjusted_user_engagement_weight,
    product_usage_primary_instance.adjusted_scm_weight,
    product_usage_primary_instance.adjusted_ci_weight,
    product_usage_primary_instance.adjusted_cd_weight,
    product_usage_primary_instance.adjusted_security_weight,
    product_usage_primary_instance.adjusted_license_utilization_score,
    product_usage_primary_instance.adjusted_user_engagement_score,
    product_usage_primary_instance.adjusted_scm_score,
    product_usage_primary_instance.adjusted_ci_score,
    product_usage_primary_instance.adjusted_cd_score,
    product_usage_primary_instance.adjusted_security_score,
    product_usage_primary_instance.overall_product_score,
    product_usage_primary_instance.overall_product_color,
    product_usage_primary_instance.scm_adopted,
    product_usage_primary_instance.ci_adopted,
    product_usage_primary_instance.cd_adopted,
    product_usage_primary_instance.security_adopted,
    product_usage_primary_instance.total_use_cases_adopted,
    product_usage_primary_instance.adopted_use_case_names_array,
    product_usage_primary_instance.adopted_use_case_names_string,
    product_usage_primary_instance.ci_color_previous_month,
    product_usage_primary_instance.ci_color_previous_3_month,
    product_usage_primary_instance.ci_pipeline_utilization_previous_month,
    product_usage_primary_instance.ci_pipeline_utilization_previous_3_month,
    product_usage_primary_instance.scm_color_previous_month,
    product_usage_primary_instance.scm_color_previous_3_month,
    product_usage_primary_instance.git_operation_utilization_previous_month,
    product_usage_primary_instance.git_operation_utilization_previous_3_month,
    product_usage_primary_instance.cd_color_previous_month,
    product_usage_primary_instance.cd_color_previous_3_month,
    product_usage_primary_instance.security_color_previous_month,
    product_usage_primary_instance.security_color_previous_3_month,
    product_usage_primary_instance.is_primary_instance_subscription,

--Ci Score Roll Up
    (product_usage_primary_instance.ci_pipeline_utilization) * (mart_arr_all.arr) AS ci_utilization_dollar,
    sum(ci_utilization_dollar) OVER (PARTITION BY mart_arr_all.arr_month, mart_arr_all.dim_crm_account_id) as total_account_ci_utilization_dollar,
    div0(total_account_ci_utilization_dollar,child_account_base_arr) as weighted_ci_adoption_child_account,
    CASE WHEN weighted_ci_adoption_child_account > 0.333 THEN 88
         WHEN weighted_ci_adoption_child_account > 0.1 AND weighted_ci_adoption_child_account <=0.333 THEN 63
         WHEN weighted_ci_adoption_child_account <= 0.1 THEN 25
         ELSE NULL END AS ci_score_child_account,
    CASE WHEN weighted_ci_adoption_child_account > 0.333 THEN 'Green'
         WHEN weighted_ci_adoption_child_account > 0.1 AND weighted_ci_adoption_child_account <=0.333 THEN 'Yellow'
         WHEN weighted_ci_adoption_child_account <= 0.1 THEN 'Red'
         ELSE 'NO DATA AT ALL' END AS ci_color_child_account
FROM
  mart_arr_all
  LEFT JOIN product_usage_primary_instance
    ON  product_usage_primary_instance.dim_subscription_id_original =    mart_arr_all.dim_subscription_id_original
    AND     product_usage_primary_instance.snapshot_month =   mart_arr_all.arr_month
    AND     product_usage_primary_instance.delivery_type =  mart_arr_all.product_delivery_type
)

SELECT 
  *
FROM final