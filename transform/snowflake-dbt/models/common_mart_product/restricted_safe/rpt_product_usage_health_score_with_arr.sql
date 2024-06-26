{{ config(materialized='table') }}

WITH final as (

SELECT
    rpt_product_usage_health_score.*,
        CASE WHEN ci_pipeline_utilization > 0.33 THEN 88
                  WHEN ci_pipeline_utilization >= 0.1 AND ci_pipeline_utilization <=0.33 THEN 63
                  WHEN ci_pipeline_utilization < 0.1 THEN 25
                  ELSE NULL END AS ci_pipeline_utilization_score,
  DIV0NULL(yearlies_actual.actuals_raw,yearlies_target.targets_raw) AS target_attainment
FROM
  {{ ref('mart_arr_with_zero_dollar_charges') }} AS mart_arr_all
  LEFT JOIN {{ ref('rpt_product_usage_health_score') }} AS rpt_product_usage_health_score
    ON rpt_product_usage_health_score.dim_subscription_id_original = mart_arr_all.dim_subscription_id_original
    AND rpt_product_usage_health_score.snapshot_month = mart_arr_all.arr_month
    AND rpt_product_usage_health_score.delivery_type = mart_arr_all.product_delivery_type
)

SELECT 
  *
FROM final