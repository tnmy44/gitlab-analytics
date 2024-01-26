WITH targets_actuals AS (

  SELECT * 
  FROM {{ ref('wk_fct_targets_actuals_snapshot') }}

),

day_5_list AS (

  SELECT 
    date_actual AS day_5_current_week,
    LAG(day_5_current_week) OVER (ORDER BY day_5_current_week) + 1 AS day_6_previous_week -- Add an extra day to exclude the previous thursday from the calculation
  FROM {{ ref('dim_date') }}
  WHERE day_of_week = 5

),

aggregated_data AS (

  SELECT
    actuals_targets_pk,
    snapshot_date,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_crm_user_hierarchy_sk,

    -- attributes
    report_user_segment_geo_region_area_sqs_ot,
    order_type_name,
    sales_qualified_source_name,
    crm_user_sales_segment, 
    crm_user_geo, 
    crm_user_region, 
    crm_user_area, 
    crm_user_business_unit,

     --dates
    snapshot_day,
    snapshot_day_name, 
    snapshot_day_of_week,
    snapshot_first_day_of_week,
    snapshot_week_of_year,
    snapshot_day_of_month,
    snapshot_day_of_quarter,
    snapshot_day_of_year,
    snapshot_fiscal_year,
    snapshot_fiscal_quarter,
    snapshot_day_of_fiscal_quarter,
    snapshot_day_of_fiscal_year,
    snapshot_month_name,
    snapshot_first_day_of_month,
    snapshot_last_day_of_month,
    snapshot_first_day_of_year,
    snapshot_last_day_of_year,
    snapshot_first_day_of_quarter,
    snapshot_last_day_of_quarter,
    snapshot_first_day_of_fiscal_quarter,
    snapshot_last_day_of_fiscal_quarter,
    snapshot_first_day_of_fiscal_year,
    snapshot_last_day_of_fiscal_year,
    snapshot_week_of_fiscal_year,
    snapshot_month_of_fiscal_year,
    snapshot_last_day_of_week,
    snapshot_quarter_name,
    snapshot_fiscal_quarter_date,
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_name_fy,
    snapshot_fiscal_quarter_number_absolute,
    snapshot_fiscal_month_name,
    snapshot_fiscal_month_name_fy,
    snapshot_holiday_desc,
    snapshot_is_holiday,
    snapshot_last_month_of_fiscal_quarter,
    snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    snapshot_last_month_of_fiscal_year,
    snapshot_is_first_day_of_last_month_of_fiscal_year,
    snapshot_days_in_month_count,
    snapshot_week_of_month_normalised,
    snapshot_day_of_fiscal_quarter_normalised,
    snapshot_week_of_fiscal_quarter_normalised,
    snapshot_day_of_fiscal_year_normalised,
    snapshot_is_first_day_of_fiscal_quarter_week,
    snapshot_days_until_last_day_of_month,
    snapshot_current_date_actual,
    snapshot_current_fiscal_year,
    snapshot_current_first_day_of_fiscal_year,
    snapshot_current_fiscal_quarter_name_fy,
    snapshot_current_first_day_of_month,
    snapshot_current_first_day_of_fiscal_quarter,
    snapshot_current_day_of_month,
    snapshot_current_day_of_fiscal_quarter,
    snapshot_current_day_of_fiscal_year,

     -- Targets
    deals_daily_target,
    deals_monthly_target,
    deals_wtd_target,
    deals_mtd_target,
    deals_qtd_target,
    deals_ytd_target,
    mql_daily_target,
    mql_monthly_target,
    mql_wtd_target,
    mql_mtd_target,
    mql_qtd_target,
    mql_ytd_target,
    net_arr_daily_target,
    net_arr_monthly_target,
    net_arr_wtd_target,
    net_arr_mtd_target,
    net_arr_qtd_target,
    net_arr_ytd_target,
    net_arr_company_daily_target,
    net_arr_company_monthly_target,
    net_arr_company_wtd_target,
    net_arr_company_mtd_target,
    net_arr_company_qtd_target,
    net_arr_company_ytd_target,
    net_arr_pipeline_created_daily_target,
    net_arr_pipeline_created_monthly_target,
    net_arr_pipeline_created_wtd_target,
    net_arr_pipeline_created_mtd_target,
    net_arr_pipeline_created_qtd_target,
    net_arr_pipeline_created_ytd_target,
    new_logos_daily_target,
    new_logos_monthly_target,
    new_logos_wtd_target,
    new_logos_mtd_target,
    new_logos_qtd_target,
    new_logos_ytd_target,
    stage_1_opportunities_daily_target,
    stage_1_opportunities_monthly_target,
    stage_1_opportunities_wtd_target,
    stage_1_opportunities_mtd_target,
    stage_1_opportunities_qtd_target,
    stage_1_opportunities_ytd_target,
    total_closed_daily_target,
    total_closed_monthly_target,
    total_closed_wtd_target,
    total_closed_mtd_target,
    total_closed_qtd_target,
    total_closed_ytd_target,
    trials_daily_target,
    trials_monthly_target,
    trials_wtd_target,
    trials_mtd_target,
    trials_qtd_target,
    trials_ytd_target,

    -- Additive fields
    SUM(iacv)                                            AS iacv,
    SUM(net_iacv)                                        AS net_iacv,
    SUM(segment_order_type_iacv_to_net_arr_ratio)        AS segment_order_type_iacv_to_net_arr_ratio,
    SUM(calculated_from_ratio_net_arr)                   AS calculated_from_ratio_net_arr,
    SUM(net_arr)                                         AS net_arr,
    SUM(raw_net_arr)                                     AS raw_net_arr,
    SUM(created_and_won_same_quarter_net_arr_combined)   AS created_and_won_same_quarter_net_arr_combined,
    SUM(new_logo_count)                                  AS new_logo_count,
    SUM(amount)                                          AS amount,
    SUM(recurring_amount)                                AS recurring_amount,
    SUM(true_up_amount)                                  AS true_up_amount,
    SUM(proserv_amount)                                  AS proserv_amount,
    SUM(other_non_recurring_amount)                      AS other_non_recurring_amount,
    SUM(arr_basis)                                       AS arr_basis,
    SUM(arr)                                             AS arr,
    SUM(count_crm_attribution_touchpoints)               AS count_crm_attribution_touchpoints,
    SUM(weighted_linear_iacv)                            AS weighted_linear_iacv,
    SUM(count_campaigns)                                 AS count_campaigns,
    SUM(probability)                                     AS probability,
    SUM(days_in_sao)                                     AS days_in_sao,
    SUM(open_1plus_deal_count)                           AS open_1plus_deal_count,
    SUM(open_3plus_deal_count)                           AS open_3plus_deal_count,
    SUM(open_4plus_deal_count)                           AS open_4plus_deal_count,
    SUM(booked_deal_count)                               AS booked_deal_count,
    SUM(churned_contraction_deal_count)                  AS churned_contraction_deal_count,
    SUM(open_1plus_net_arr)                              AS open_1plus_net_arr,
    SUM(open_3plus_net_arr)                              AS open_3plus_net_arr,
    SUM(open_4plus_net_arr)                              AS open_4plus_net_arr,
    SUM(booked_net_arr)                                  AS booked_net_arr,
    SUM(churned_contraction_net_arr)                     AS churned_contraction_net_arr,
    SUM(calculated_deal_count)                           AS calculated_deal_count,
    SUM(booked_churned_contraction_deal_count)           AS booked_churned_contraction_deal_count,
    SUM(booked_churned_contraction_net_arr)              AS booked_churned_contraction_net_arr,
    SUM(renewal_amount)                                  AS renewal_amount,
    SUM(total_contract_value)                            AS total_contract_value,
    SUM(days_in_stage)                                   AS days_in_stage,
    SUM(calculated_age_in_days)                          AS calculated_age_in_days,
    SUM(days_since_last_activity)                        AS days_since_last_activity,
    SUM(pre_military_invasion_arr)                       AS pre_military_invasion_arr,
    SUM(won_arr_basis_for_clari)                         AS won_arr_basis_for_clari,
    SUM(arr_basis_for_clari)                             AS arr_basis_for_clari,
    SUM(forecasted_churn_for_clari)                      AS forecasted_churn_for_clari,
    SUM(override_arr_basis_clari)                        AS override_arr_basis_clari,
    SUM(vsa_start_date_net_arr)                          AS vsa_start_date_net_arr,
    SUM(cycle_time_in_days_combined)                     AS cycle_time_in_days_combined
  FROM targets_actuals
  GROUP BY all

),

quarterly_targets_totals AS (

  SELECT  
    snapshot_fiscal_year,
    snapshot_fiscal_quarter,
    snapshot_fiscal_quarter_date,
    report_user_segment_geo_region_area_sqs_ot,
    SUM(deals_monthly_target)                            AS deals_quarterly_target,
    SUM(mql_monthly_target)                              AS mql_quarterly_target,
    SUM(net_arr_monthly_target)                          AS net_arr_quarterly_target,
    SUM(net_arr_company_monthly_target)                  AS net_arr_company_quarterly_target,
    SUM(new_logos_monthly_target)                        AS new_logos_quarterly_target,
    SUM(stage_1_opportunities_monthly_target)            AS stage_1_opportunities_quarterly_target, 
    SUM(total_closed_monthly_target)                     AS total_closed_quarterly_target,
    SUM(trials_monthly_target)                           AS trials_quarterly_target,
    SUM(CASE 
          WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date
              THEN booked_net_arr
          ELSE 0
        END)                                               AS total_booked_net_arr,
    SUM(CASE 
          WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date
              THEN churned_contraction_net_arr
          ELSE 0
        END)                                               AS total_churned_contraction_net_arr,       
    SUM(CASE 
          WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date
              THEN booked_deal_count
          ELSE 0
        END)                                               AS total_booked_deal_count,
    SUM(CASE 
          WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date
              THEN churned_contraction_deal_count
          ELSE 0
         END)                                              AS total_churned_contraction_deal_count,   
    
    -- Pipe gen totals
    SUM(CASE 
          WHEN pipeline_created_fiscal_quarter_date = snapshot_fiscal_quarter_date
            AND is_eligible_created_pipeline_flag = 1
              THEN created_in_snapshot_quarter_net_arr
          ELSE 0
        END )                                              AS total_pipe_generation_net_arr,
    SUM(CASE 
          WHEN pipeline_created_fiscal_quarter_date = snapshot_fiscal_quarter_date
            AND is_eligible_created_pipeline_flag = 1
              THEN created_in_snapshot_quarter_deal_count
          ELSE 0
        END )                                              AS total_pipe_generation_deal_count,
    
    -- Created & Landed totals
    SUM(CASE 
          WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date
              THEN created_and_won_same_quarter_net_arr
          ELSE 0
        END)                                               AS total_created_and_booked_same_quarter_net_arr
  FROM targets_actuals
  WHERE snapshot_day_of_fiscal_quarter_normalised = 90

),

historical_targets_actuals AS (

  SELECT
    aggregated_data.*,
    -- check if we are in the current fiscal year or not. If not, use total, if we are use target
    CASE
      WHEN snapshot_current_fiscal_quarter_name_fy < snapshot_fiscal_quarter_name
        THEN net_arr_daily_target
      ELSE booked_net_arr
    END                                         AS adjusted_daily_target_net_arr,
    CASE
      WHEN snapshot_current_fiscal_quarter_name_fy < snapshot_fiscal_quarter_name
        THEN net_arr_monthly_target
      ELSE booked_net_arr
    END                                         AS adjusted_monthly_target_net_arr,
    CASE
      WHEN snapshot_current_fiscal_quarter_name_fy < snapshot_fiscal_quarter_name
        THEN deals_daily_target
      ELSE booked_net_arr
    END                                         AS adjusted_daily_target_deals,
    CASE
      WHEN snapshot_current_fiscal_quarter_name_fy < snapshot_fiscal_quarter_name
        THEN deals_monthly_target
      ELSE booked_net_arr
    END                                         AS adjusted_monthly_target_deals,
    CASE
      WHEN snapshot_current_fiscal_quarter_name_fy = snapshot_fiscal_quarter_name
        THEN net_arr_pipeline_created_daily_target
      ELSE booked_net_arr
    END                                         AS adjusted_daily_target_net_arr_pipeline_created,
    CASE
      WHEN snapshot_current_fiscal_quarter_name_fy <= snapshot_fiscal_quarter_name
        THEN net_arr_pipeline_created_monthly_target
      ELSE booked_net_arr
    END                                         AS adjusted_monthly_target_net_arr_pipeline_created,
    IFF(snapshot_fiscal_quarter_name_fy = snapshot_current_fiscal_quarter_name_fy, TRUE, FALSE) AS is_current_snapshot_quarter
  FROM aggregated_data
  INNER JOIN quarterly_targets_totals
    ON quarterly_targets_totals.snapshot_fiscal_year = aggregated_data.snapshot_fiscal_year
      AND quarterly_targets_totals.snapshot_fiscal_quarter = aggregated_data.snapshot_fiscal_quarter
        AND quarterly_targets_totals.report_user_segment_geo_region_area_sqs_ot = aggregated_data.report_user_segment_geo_region_area_sqs_ot

),

granular_data AS (

  SELECT 
    targets_actuals.*,
    -- Create flags to know whether an action happened in the current snapshot week 
    -- ie. Whether it happened between last Friday and current Thursday
    CASE
      WHEN created_date BETWEEN day_6_previous_week AND day_5_current_week
        THEN 1
      ELSE 0
    END AS is_created_in_snapshot_week, 
    CASE  
      WHEN close_date BETWEEN day_6_previous_week AND day_5_current_week
        THEN 1
      ELSE 0
    END AS is_close_in_snapshot_week, 
    CASE  
      WHEN arr_created_date BETWEEN day_6_previous_week AND day_5_current_week
        THEN 1
      ELSE 0
    END AS is_arr_created_in_snapshot_week, 
    CASE  
      WHEN arr_created_date BETWEEN day_6_previous_week AND day_5_current_week
        THEN 1
      ELSE 0
    END AS is_net_arr_created_in_snapshot_week, 
    CASE  
      WHEN pipeline_created_date BETWEEN day_6_previous_week AND day_5_current_week
        THEN 1
      ELSE 0
    END AS is_pipeline_created_in_snapshot_week,
    CASE  
      WHEN sales_accepted_date BETWEEN day_6_previous_week AND day_5_current_week
        THEN 1
      ELSE 0
    END AS is_sales_accepted_in_snapshot_week,

    -- Pull in the metric only when the corresponding flag is set
    -- ie. Only calculate created arr in the week where the opportunity was created
    CASE
      WHEN is_arr_created_in_snapshot_week = 1
        THEN arr
      ELSE 0
    END AS created_arr_in_snapshot_week,
    CASE
      WHEN is_net_arr_created_in_snapshot_week = 1
        THEN raw_net_arr
      ELSE 0
    END AS created_net_arr_in_snapshot_week,
    CASE
      WHEN is_created_in_snapshot_week = 1
        THEN 1
      ELSE 0
    END AS created_deal_count_in_snapshot_week,
    CASE
      WHEN is_close_in_snapshot_week = 1
        THEN net_arr
      ELSE 0
    END AS closed_net_arr_in_snapshot_week,
    CASE
      WHEN is_close_in_snapshot_week = 1
        THEN 1
      ELSE 0
    END AS closed_deal_count_in_snapshot_week,
    CASE
      WHEN is_close_in_snapshot_week = 1
        THEN 1
      ELSE 0
    END AS closed_new_logo_count_in_snapshot_week,
    CASE
      WHEN is_close_in_snapshot_week = 1
        THEN close_date - created_date
      ELSE 0
    END AS closed_cycle_time_in_snapshot_week,
    IFF(fiscal_quarter_name_fy = current_fiscal_quarter_name_fy, TRUE, FALSE) AS is_current_snapshot_quarter
  FROM targets_actuals
  INNER JOIN day_5_list
    ON actuals.snapshot_date = day_5_list.day_5_current_week

)

SELECT * 
FROM historical_targets_actuals
