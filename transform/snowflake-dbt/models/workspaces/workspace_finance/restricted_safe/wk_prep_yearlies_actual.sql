{{ config(materialized='view') }}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('mart_arr_all', 'mart_arr_with_zero_dollar_charges'),
    ('dim_date','dim_date')
]) }},

total_arr_final AS (
SELECT 
    '1.1 Total ARR' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
     close_fiscal_quarter_name AS quarter,
     SUM(booked_net_arr) AS actuals_raw
    
    FROM mart_crm_opportunity
    WHERE stage_name = 'Closed Won'
    AND close_fiscal_quarter_name like '%FY25%'
    AND close_month <= date_trunc('month',current_date)
    GROUP BY 1,2,3
),

duo_final AS (
  SELECT 
    '2.1 Duo' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
     close_fiscal_quarter_name AS quarter,
     SUM(booked_net_arr) AS actuals_raw,
    
    FROM mart_crm_opportunity
    WHERE stage_name = 'Closed Won'
    AND close_fiscal_quarter_name like '%FY25%'
    AND close_month <= date_trunc('month',current_date)
    AND product_category like '%Duo%'
    GROUP BY 1,2,3
),

adoption_metrics_1 AS (
  SELECT
    *
  FROM rpt_product_usage_health_score
  WHERE INSTANCE_TYPE = 'Production'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SNAPSHOT_MONTH, DIM_SUBSCRIPTION_ID_ORIGINAL
  ORDER BY BILLABLE_USER_COUNT desc nulls last, PING_CREATED_AT desc nulls last) = 1
),

adoption_metrics_2 AS (
  SELECT
    '3.1 Adoption Metrics' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_ARR, PROD.COMMON_MART_PRODUCT.RPT_PRODUCT_USAGE_HEALTH_SCORE' AS source_table,
    fiscal_quarter_name_fy,
    arr_month,
    CASE WHEN adoption_metrics_1.dim_subscription_id_original is not null then true else false end AS usage_data_sent,
    SUM(arr) AS arr,
    SUM(arr)/SUM(SUM(arr)) OVER (partition by arr_month) AS percent_of_total
  FROM
   mart_arr_all
   left join 
      adoption_metrics_1
      on arr_month = snapshot_month
      AND mart_arr_all.dim_subscription_id_original = adoption_metrics_1.dim_subscription_id_original
      AND mart_arr_all.product_delivery_type = adoption_metrics_1.delivery_type

  GROUP BY 1,2,3,4,5
),

adoption_metrics_final AS (
  SELECT 
    yearly_name,
    source_table,
    fiscal_quarter_name_fy AS quarter,
    percent_of_total AS actuals_raw
  FROM
    adoption_metrics_2 
  WHERE fiscal_quarter_name_fy like 'FY25%'
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
      left JOIN
         dim_subscription 
         on mart_crm_opportunity.dim_crm_opportunity_id = dim_subscription.dim_crm_opportunity_id 
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
WHERE quarter like 'FY25%'
),

ultimate_arr_final AS (
SELECT 
    '4.2 Ultimate ARR' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
     close_fiscal_quarter_name AS quarter,
     SUM(booked_net_arr) AS actuals_raw
    
    FROM mart_crm_opportunity
    WHERE stage_name = 'Closed Won'
    AND close_fiscal_quarter_name like '%FY25%'
    AND close_month <= date_trunc('month',current_date)
    AND product_category like '%Ultimate%'
    AND product_category not like '%Dedicated%'
    GROUP BY 1,2,3
),

dedicated_arr_final AS (
  SELECT 
    '4.3 Dedicated ARR' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
     close_fiscal_quarter_name AS quarter,
     SUM(booked_net_arr) AS actuals_raw
    
    FROM mart_crm_opportunity
    WHERE stage_name = 'Closed Won'
    AND close_fiscal_quarter_name like '%FY25%'
    AND close_month <= date_trunc('month',current_date)
    AND product_category like '%Dedicated%'
    GROUP BY 1,2,3
),

plan_arr_final AS (
  SELECT 
    '4.4 Plan' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
     close_fiscal_quarter_name AS quarter,
     SUM(booked_net_arr) AS actuals_raw
    
    FROM mart_crm_opportunity
    WHERE stage_name = 'Closed Won'
    AND close_fiscal_quarter_name like '%FY25%'
    AND close_month <= date_trunc('month',current_date)
    AND product_category like '%Agile%'
    GROUP BY 1,2,3
),

final AS (

SELECT * FROM total_arr_final 

UNION ALL

SELECT * FROM duo_final

UNION ALL

SELECT * FROM adoption_metrics_final

UNION ALL

SELECT * FROM churn_contraction_final

UNION ALL

SELECT * FROM ultimate_arr_final

UNION ALL

SELECT * FROM dedicated_arr_final

UNION ALL

SELECT * FROM plan_arr_final
)

SELECT 
  * 
FROM 
  final