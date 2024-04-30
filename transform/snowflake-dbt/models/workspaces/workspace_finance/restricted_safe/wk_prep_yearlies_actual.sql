{{ config(materialized='view') }}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('mart_arr_all', 'mart_arr_with_zero_dollar_charges'),
    ('wk_fct_sales_funnel_actual', 'wk_fct_sales_funnel_actual'),
    ('wk_fct_crm_opportunity', 'wk_fct_crm_opportunity'),
    ('dim_date','dim_date')
]) }},

total_bookings_final AS (
SELECT 
    '1.1 Total Bookings' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON.FCT_CRM_OPPORTUNITY' AS source_table,
     fiscal_quarter_name_fy AS quarter,
     SUM(net_arr) AS actuals_raw
    
    FROM wk_fct_sales_funnel_actual
    LEFT JOIN dim_date 
    ON date_actual = actual_date
    WHERE date_trunc('month',actual_date) <= date_trunc('month',current_date) 
    AND sales_funnel_kpi_name = 'Net ARR'
    AND fiscal_quarter_name_fy LIKE '%FY25%'
    GROUP BY 1,2,3
    ORDER BY 3 DESC
),

adoption_metrics_1 AS (
  SELECT
    *
  FROM rpt_product_usage_health_score
  WHERE INSTANCE_TYPE = 'Production'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_month, dim_subscription_id_original, delivery_type
  ORDER BY billable_user_count desc nulls last, ping_created_at desc nulls last) = 1
),

adoption_metrics_2 AS (
  SELECT
    '3.1 Adoption Metrics' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_ARR_ALL, PROD.COMMON_MART_PRODUCT.RPT_PRODUCT_USAGE_HEALTH_SCORE' AS source_table,
    fiscal_quarter_name_fy,
    arr_month,
    IFF(adoption_metrics_1.ping_created_at is not null, true, false) AS usage_data_sent,
    SUM(arr) AS arr,
    SUM(arr)/SUM(SUM(arr)) OVER (partition by arr_month) AS percent_of_total
  FROM
   mart_arr_all
   left join
      adoption_metrics_1
      on arr_month = snapshot_month
      AND mart_arr_all.dim_subscription_id_original = adoption_metrics_1.dim_subscription_id_original
      AND mart_arr_all.product_delivery_type = adoption_metrics_1.delivery_type
   WHERE product_tier_name <> 'Storage'
		AND product_tier_name <> 'Not Applicable'
  
  GROUP BY 1,2,3,4,5
),

adoption_metrics_final AS (
  SELECT
    yearly_name,
    source_table,
    dim_date.fiscal_quarter_name_fy as quarter,
    percent_of_total AS actuals_raw
  FROM
    adoption_metrics_2
  LEFT JOIN dim_date
  ON arr_month = date_actual
  WHERE arr_month = dateadd(month, -1, date_trunc(month, current_date))
  AND usage_data_sent = true
),

churn_contraction_1 AS (
   SELECT
      mart_crm_opportunity.CLOSE_FISCAL_QUARTER_NAME,
      mart_crm_opportunity.ARR_BASIS_FOR_CLARI,
      mart_crm_opportunity.order_type,
      mart_crm_opportunity.WON_ARR_BASIS_FOR_CLARI - mart_crm_opportunity.ARR_BASIS_FOR_CLARI AS renewal_net_arr 
   FROM
      mart_crm_opportunity 
      JOIN
         dim_crm_account 
         ON dim_crm_account.dim_crm_account_id = mart_crm_opportunity.dim_crm_account_id
   WHERE
      mart_crm_opportunity.SALES_TYPE = 'Renewal' 
      AND mart_crm_opportunity.STAGE_NAME IN 
      (
         'Closed Won',
         '8-Closed Lost'
      )
      AND mart_crm_opportunity.IS_JIHU_ACCOUNT = FALSE 
      AND mart_crm_opportunity.ORDER_TYPE IN 
      (
         '3. Growth',
         '4. Contraction',
         '5. Churn - Partial',
         '6. Churn - Final'
      )
      AND mart_crm_opportunity.CLOSE_MONTH <= date_trunc('month', current_date) 
),

churn_contraction_2 AS (
   SELECT
      CLOSE_FISCAL_QUARTER_NAME,
      SUM(CASE WHEN order_type in ('4. Contraction','5. Churn - Partial','6. Churn - Final') then renewal_net_arr end) AS renewal_net_arr_loss,     
      SUM(arr_bASis_for_clari) AS atr 
   FROM
      churn_contraction_1
   GROUP BY
      1 
),

churn_contraction_final AS (
SELECT
   '3.4 Churn and Contraction' AS yearly_name,
   'PROD.RESTRICTED_SAFE_COMMON_SALES.MART_CRM_OPPORTUNITY, PROD.COMMON.DIM_SUBSCRIPTION' AS source_table,
   CLOSE_FISCAL_QUARTER_NAME AS quarter,
   renewal_net_arr_loss / atr* - 1 AS actuals_raw
FROM
   churn_contraction_2
WHERE quarter LIKE 'FY25%'
),

ultimate_bookings_final AS (
SELECT 
  '4.2 Ultimate Bookings' AS yearly_name,
  'PROD.RESTRICTED_SAFE_COMMON.FCT_CRM_OPPORTUNITY' AS source_table,
   fiscal_quarter_name_fy AS quarter,
   SUM(wk_fct_sales_funnel_actual.net_arr) AS actuals_raw
    
  FROM wk_fct_sales_funnel_actual 
  LEFT JOIN wk_fct_crm_opportunity 
  ON wk_fct_sales_funnel_actual.dim_crm_opportunity_id = wk_fct_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN prod.common.dim_date 
  ON date_actual = actual_date
  WHERE date_trunc('month',actual_date) <= date_trunc('month',current_date) 
  AND sales_funnel_kpi_name = 'Net ARR'
  AND fiscal_quarter_name_fy LIKE '%FY25%'
  AND wk_fct_crm_opportunity.product_category LIKE '%Ultimate%'
  GROUP BY 1,2,3
  ORDER BY 3 DESC
),

dedicated_bookings_final AS (
SELECT 
  '4.3 Dedicated Bookings' AS yearly_name,
  'PROD.RESTRICTED_SAFE_COMMON.FCT_CRM_OPPORTUNITY' AS source_table,
  fiscal_quarter_name_fy AS quarter,
  SUM(wk_fct_sales_funnel_actual.net_arr) AS actuals_raw
    
  FROM wk_fct_sales_funnel_actual
  LEFT JOIN wk_fct_crm_opportunity 
  on wk_fct_sales_funnel_actual.dim_crm_opportunity_id = wk_fct_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN prod.common.dim_date 
  ON date_actual = actual_date
  WHERE date_trunc('month',actual_date) <= date_trunc('month',current_date) 
  AND sales_funnel_kpi_name = 'Net ARR'
  AND fiscal_quarter_name_fy LIKE '%FY25%'
  AND wk_fct_crm_opportunity.product_details ILIKE '%dedicated%'
  GROUP BY 1,2,3
  ORDER BY 3 DESC

),

final AS (

SELECT * FROM total_bookings_final 

UNION ALL

SELECT * FROM adoption_metrics_final

UNION ALL

SELECT * FROM churn_contraction_final

UNION ALL

SELECT * FROM ultimate_bookings_final

UNION ALL

SELECT * FROM dedicated_bookings_final

)

SELECT 
  * 
FROM 
  final
  