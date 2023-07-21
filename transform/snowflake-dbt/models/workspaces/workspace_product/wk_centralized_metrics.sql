{{ config(
    materialized='view', --can be table
    tags=["product"]
) }}

{{ simple_cte([
    ('rpt_ping_metric_totals_w_estimates_monthly', 'rpt_ping_metric_totals_w_estimates_monthly'),
    ('rpt_event_xmau_metric_monthly', 'rpt_event_xmau_metric_monthly')
    ])
}}, service_ping_with_estimate AS (

  SELECT
    ping_created_date_month AS reporting_month,
    section_name,
    stage_name,
    group_name,
    estimation_grain,
    metrics_path,
    ping_product_tier,
    ping_delivery_type,
    ping_edition,
    total_usage_with_estimate,
    recorded_usage,
    estimated_usage,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau
FROM rpt_ping_metric_totals_w_estimates_monthly
ORDER BY reporting_month DESC

), gitlab_dotcom_usage AS (

  SELECT
    event_calendar_month AS reporting_month,
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_umau,
    event_name_array,
    user_group,
    user_count
  FROM rpt_event_xmau_metric_monthly
  ORDER BY reporting_month DESC

), service_ping_gitlab_dotcom_unioned AS (

  SELECT
    reporting_month,
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    estimation_grain,
    metrics_path AS metric,
    IFF(ping_delivery_type = 'SaaS', 'SaaS', ping_product_tier) AS product_tier,
    IFF(ping_delivery_type = 'Self-Managed', 'Recorded Self-Managed', ping_delivery_type) AS breakdown,
    ping_delivery_type AS delivery,
    IFF(ping_delivery_type = 'SaaS', 'SaaS', ping_edition) AS edition, 
    SUM(recorded_usage) AS mau_value
  FROM service_ping_with_estimate
  {{ dbt_utils.group_by(n=14) }}
  
  UNION ALL
  
  SELECT --estimated SM totals
    reporting_month,
    section_name,
    stage_name,
    group_name,
      is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    estimation_grain,
    metrics_path AS metric,
    ping_product_tier AS product_tier,
    'Estimated Self-Managed Uplift' AS breakdown,
    ping_delivery_type AS delivery,
    ping_edition AS edition,
    ROUND(SUM(estimated_usage)) AS mau_value
  FROM service_ping_with_estimate
  WHERE ping_delivery_type = 'Self-Managed'
  {{ dbt_utils.group_by(n=14) }}
  
  UNION ALL
  
  SELECT --get paid SaaS xMAU (if applicable)
    reporting_month,
    section_name,
    stage_name,
    group_name,
      is_smau,
    is_gmau,
    is_gmau AS is_paid_gmau,
    is_umau,
    'Paid SaaS' AS estimation_grain,
    ARRAY_TO_STRING(event_name_array, ',') AS metric,
    'Paid SaaS' AS product_tier,
    'Paid SaaS' AS breakdown,
    'Paid SaaS' AS delivery,
    'Paid SaaS' AS edition,
    SUM(user_count) AS mau_value
  FROM gitlab_dotcom_usage
  WHERE  user_group = 'paid' 
    AND reporting_month >= (SELECT MIN(reporting_month) 
FROM service_ping_with_estimate) 
  {{ dbt_utils.group_by(n=14) }}

), results AS (

SELECT 
  {{ dbt_utils.surrogate_key(['service_ping_gitlab_dotcom_unioned.reporting_month', 'service_ping_gitlab_dotcom_unioned.event_name', 'service_ping_gitlab_dotcom_unioned.product_tier','service_ping_gitlab_dotcom_unioned.breakdown']) }}       
                                               AS event_namespace_monthly_pk,  
  *,
  LAG(mau_value,12) OVER (PARTITION BY
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    estimation_grain,
    metric,
    product_tier,
    breakdown,
    delivery,
    edition
 ORDER BY reporting_month ASC ) AS previous_year_mau,
  LAG(mau_value,1) OVER (PARTITION BY
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    estimation_grain,
    metric,
    product_tier,
    breakdown,
    delivery,
    edition
ORDER BY reporting_month ASC ) AS previous_month_mau,
    MAX(IFF(is_umau = TRUE, mau_value,NULL)) OVER (PARTITION BY reporting_month,    
 estimation_grain,
    product_tier,
    breakdown,
    delivery,
    edition) AS umau_value
FROM service_ping_gitlab_dotcom_unioned

)


{{ dbt_audit(
    cte_ref="centralized_metrics",
    created_by="@dpeterson",
    updated_by="@dpeterson",
    created_date="2023-07-20",
    updated_date="2023-07-20"
) }}
