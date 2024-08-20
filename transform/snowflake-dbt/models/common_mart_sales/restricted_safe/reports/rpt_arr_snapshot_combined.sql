{{ simple_cte([
    ('rpt_arr_snapshot_combined_8th_calendar_day','rpt_arr_snapshot_combined_8th_calendar_day'),
    ('rpt_arr_snapshot_combined_5th_calendar_day','rpt_arr_snapshot_combined_5th_calendar_day')
]) }},

final AS (

    SELECT
      arr_month,
      is_arr_month_finalized,
      fiscal_quarter_name_fy,
      fiscal_year,
      subscription_start_month,
      subscription_end_month,
      dim_billing_account_id,
      sold_to_country,
      billing_account_name,
      billing_account_number,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_geo,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      dim_subscription_id,
      subscription_name,
      subscription_status,
      subscription_sales_type,
      product_name,
      product_name_grouped,
      product_rate_plan_name,
      product_deployment_type,
      product_tier_name,
      product_delivery_type,
      product_ranking,
      service_type,
      unit_of_measure,
      mrr,
      arr,
      quantity,
      is_arpu,
      is_licensed_user,
      parent_account_cohort_month,
      months_since_parent_account_cohort_start,
      arr_band_calc,
      parent_crm_account_employee_count_band
    FROM rpt_arr_snapshot_combined_8th_calendar_day
    WHERE arr_month < '2024-03-01'

    UNION ALL

    SELECT
      arr_month,
      is_arr_month_finalized,
      -- allow this field to be used as a filter and display the live data when the quarter is not in the third month
       CASE
       WHEN MONTH_OF_FISCAL_YEAR % 3 != 0 AND CURRENT_FIRST_DAY_OF_MONTH = FIRST_DAY_OF_MONTH -- is not the last month of the quarter but is the current month
        THEN DIM_DATE.FISCAL_QUARTER_NAME_FY ELSE rpt_arr_snapshot_combined_5th_calendar_day.FISCAL_QUARTER_NAME_FY 
        END as fiscal_quarter_name_fy, 
       CASE
           WHEN MONTH_OF_FISCAL_YEAR % 12 != 0 AND CURRENT_FIRST_DAY_OF_MONTH = FIRST_DAY_OF_MONTH
            THEN DIM_DATE.FISCAL_YEAR ELSE rpt_arr_snapshot_combined_5th_calendar_day.FISCAL_YEAR 
            END as fiscal_year,

      subscription_start_month,
      subscription_end_month,
      dim_billing_account_id,
      sold_to_country,
      billing_account_name,
      billing_account_number,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      parent_crm_account_billing_country,
      parent_crm_account_sales_segment,
      parent_crm_account_industry,
      parent_crm_account_geo,
      parent_crm_account_owner_team,
      parent_crm_account_sales_territory,
      dim_subscription_id,
      subscription_name,
      subscription_status,
      subscription_sales_type,
      product_name,
      product_name_grouped,
      product_rate_plan_name,
      product_deployment_type,
      product_tier_name,
      product_delivery_type,
      product_ranking,
      service_type,
      unit_of_measure,
      mrr,
      arr,
      quantity,
      is_arpu,
      is_licensed_user,
      parent_account_cohort_month,
      months_since_parent_account_cohort_start,
      arr_band_calc,
      parent_crm_account_employee_count_band
    FROM rpt_arr_snapshot_combined_5th_calendar_day
    LEFT JOIN dim_date
      ON arr_month = date_actual
    WHERE arr_month >= '2024-03-01'
    
)

SELECT *
FROM final