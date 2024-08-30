{{ simple_cte([
 ('rpt_retention_parent_account', 'rpt_retention_parent_account'),
 ('rpt_retention_future_parent_account', 'rpt_retention_future_parent_account')
]) }},

final AS (

    SELECT
      primary_key,
      dim_parent_crm_account_id,
      parent_crm_account_name                   AS parent_crm_account_name_live,
      is_arr_month_finalized,
      retention_month,
      retention_fiscal_quarter_name_fy,
      retention_fiscal_year,
      parent_crm_account_sales_segment          AS parent_crm_account_sales_segment_live,
      parent_crm_account_sales_segment_grouped  AS parent_crm_account_sales_segment_grouped_live,
      parent_crm_account_geo                    AS parent_crm_account_geo_live,
      retention_arr_band,
      prior_year_arr,
      prior_year_mrr,
      net_retention_arr,
      net_retention_mrr,
      churn_arr,
      gross_retention_arr,
      prior_year_quantity,
      net_retention_quantity,
      net_retention_product_category,
      prior_year_product_category,
      net_retention_product_ranking,
      prior_year_product_ranking
    FROM rpt_retention_parent_account
    WHERE retention_month < '2023-08-01'

    UNION ALL

    SELECT
      primary_key,
      dim_parent_crm_account_id,
      parent_crm_account_name_live,
      is_arr_month_finalized,
      retention_month,
      -- allow these fields to be used as a filter and display the live data when the quarter is not in the third month
      CASE
        WHEN MONTH_OF_FISCAL_YEAR % 3 != 0 AND CURRENT_FIRST_DAY_OF_MONTH = FIRST_DAY_OF_MONTH -- is not the last month of the quarter but is the current month
          THEN DIM_DATE.FISCAL_QUARTER_NAME_FY ELSE rpt_retention_future_parent_account.retention_fiscal_quarter_name_fy 
      END                                                                                                                        AS retention_fiscal_quarter_name_fy, 
      CASE
        WHEN MONTH_OF_FISCAL_YEAR % 12 != 0 AND CURRENT_FIRST_DAY_OF_MONTH = FIRST_DAY_OF_MONTH
          THEN DIM_DATE.FISCAL_YEAR ELSE rpt_retention_future_parent_account.retention_fiscal_year
      END                                                                                                                        AS retention_fiscal_year,
      parent_crm_account_sales_segment_live,
      parent_crm_account_sales_segment_grouped_live,
      parent_crm_account_geo_live,
      retention_arr_band,
      prior_year_arr,
      prior_year_mrr,
      net_retention_arr,
      net_retention_mrr,
      churn_arr,
      gross_retention_arr,
      prior_year_quantity,
      net_retention_quantity,
      net_retention_product_category,
      prior_year_product_category,
      net_retention_product_ranking,
      prior_year_product_ranking,
      MAX(retention_month) OVER ()                                                                                                    AS max_retention_month, -- So that when a new quarter begins, running an extract wont pull in blank data for the first 5 days
    FROM rpt_retention_future_parent_account
    LEFT JOIN dim_date
      ON retention_month = date_actual
    WHERE retention_month >= '2023-08-01'
    
)

{{ dbt_audit(
 cte_ref="final",
 created_by="@chrissharp",
 updated_by="@chrissharp",
 created_date="2023-11-08",
 updated_date="2023-11-08"
) }}
