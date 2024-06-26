{{ config(materialized='table') }}

WITH final as (

SELECT
    rpt_product_usage_health_score.*,
        CASE WHEN weighted_ci_adoption_child_account > 0.33 THEN 88
                  WHEN weighted_ci_adoption_child_account >= 0.1 AND weighted_ci_adoption_child_account <=0.33 THEN 63
                  WHEN weighted_ci_adoption_child_account < 0.1 THEN 25
                  ELSE NULL END AS ci_score_child_account
        CASE WHEN weighted_ci_adoption_child_account > 0.33 THEN 'Green'
                  WHEN weighted_ci_adoption_child_account >= 0.1 AND weighted_ci_adoption_child_account <=0.33 THEN 'Yellow'
                  WHEN weighted_ci_adoption_child_account < 0.1 THEN 'Red'
                  ELSE 'NO DATA AT ALL' END AS ci_color_child_account

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