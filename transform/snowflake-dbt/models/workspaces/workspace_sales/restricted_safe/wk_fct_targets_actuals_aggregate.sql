WITH targets_actuals AS (

  SELECT * 
  FROM {{ ref('wk_fct_targets_actuals_5th_day_weekly_snapshot') }}

),

aggregate_data AS (

  SELECT

    --target attributes
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_crm_user_hierarchy_sk,
    order_type_name,
    sales_qualified_source_name,
    crm_user_sales_segment,
    crm_user_geo,
    crm_user_region,
    crm_user_area,
    crm_user_business_unit,

    --dates
    snapshot_date,
    snapshot_day,
    snapshot_day_name,
    snapshot_fiscal_year,
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date,
    snapshot_week_of_year,
    snapshot_fiscal_quarter,
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
    snapshot_quarter_name,
    snapshot_fiscal_quarter_name_fy,
    snapshot_fiscal_quarter_number_absolute,
    snapshot_fiscal_month_name,
    snapshot_fiscal_month_name_fy,
    snapshot_last_month_of_fiscal_quarter,
    snapshot_last_month_of_fiscal_year,
    snapshot_days_in_month_count,
    snapshot_week_of_month_normalised,
    snapshot_week_of_fiscal_quarter_normalised,
    current_date_actual,
    current_fiscal_year,
    current_first_day_of_fiscal_year,
    current_fiscal_quarter_name_fy,
    current_first_day_of_month,
    current_first_day_of_fiscal_quarter,
    current_day_of_month,
    current_day_of_fiscal_quarter,
    current_day_of_fiscal_year,

    --targets
    --deals_daily_target,
    deals_monthly_target,
    deals_quarterly_target,
    deals_wtd_target,
    deals_mtd_target,
    deals_qtd_target,
    deals_ytd_target,
    --mql_daily_target,
    mql_monthly_target,
    mql_quarterly_target,
    mql_wtd_target,
    mql_mtd_target,
    mql_qtd_target,
    mql_ytd_target,
    --net_arr_daily_target,
    net_arr_monthly_target,
    net_arr_quarterly_target,
    net_arr_wtd_target,
    net_arr_mtd_target,
    net_arr_qtd_target,
    net_arr_ytd_target,
    --net_arr_company_daily_target,
    net_arr_company_monthly_target,
    net_arr_company_quarterly_target,
    net_arr_company_wtd_target,
    net_arr_company_mtd_target,
    net_arr_company_qtd_target,
    net_arr_company_ytd_target,
    --net_arr_pipeline_created_daily_target,
    net_arr_pipeline_created_monthly_target,
    net_arr_pipeline_created_quarterly_target,
    net_arr_pipeline_created_wtd_target,
    net_arr_pipeline_created_mtd_target,
    net_arr_pipeline_created_qtd_target,
    net_arr_pipeline_created_ytd_target,
    --new_logos_daily_target,
    new_logos_monthly_target,
    new_logos_quarterly_target,
    new_logos_wtd_target,
    new_logos_mtd_target,
    new_logos_qtd_target,
    new_logos_ytd_target,
    --stage_1_opportunities_daily_target,
    stage_1_opportunities_monthly_target,
    stage_1_opportunities_quarterly_target,
    stage_1_opportunities_wtd_target,
    stage_1_opportunities_mtd_target,
    stage_1_opportunities_qtd_target,
    stage_1_opportunities_ytd_target,
    --total_closed_daily_target,
    total_closed_monthly_target,
    total_closed_quarterly_target,
    total_closed_wtd_target,
    total_closed_mtd_target,
    total_closed_qtd_target,
    total_closed_ytd_target,
    --  trials_daily_target,
    trials_monthly_target,
    trials_quarterly_target,
    trials_wtd_target,
    trials_mtd_target,
    trials_qtd_target,
    trials_ytd_target,
    is_current_snapshot_quarter,
    -- numbers for current week

    SUM(open_pipeline_in_snapshot_week)                   AS open_pipeline_in_snapshot_week,
    SUM(pipeline_created_in_snapshot_week)                AS pipeline_created_in_snapshot_week,
    SUM(created_arr_in_snapshot_week)                     AS created_arr_in_snapshot_week,
    SUM(created_net_arr_in_snapshot_week)                 AS created_net_arr_in_snapshot_week,
    SUM(created_deal_count_in_snapshot_week)              AS created_deal_count_in_snapshot_week,
    SUM(closed_net_arr_in_snapshot_week)                  AS closed_net_arr_in_snapshot_week,
    SUM(closed_deal_count_in_snapshot_week)               AS closed_deal_count_in_snapshot_week,
    SUM(closed_new_logo_count_in_snapshot_week)           AS closed_new_logo_count_in_snapshot_week,
    SUM(closed_cycle_time_in_snapshot_week)               AS closed_cycle_time_in_snapshot_week,
    SUM(booked_net_arr_in_snapshot_week)                  AS booked_net_arr_in_snapshot_week,

    -- Additive fields
    SUM(segment_order_type_iacv_to_net_arr_ratio)         AS segment_order_type_iacv_to_net_arr_ratio,
    SUM(calculated_from_ratio_net_arr)                    AS calculated_from_ratio_net_arr,
    SUM(net_arr)                                          AS net_arr,
    SUM(raw_net_arr)                                      AS raw_net_arr,
    SUM(created_and_won_same_quarter_net_arr_combined)    AS created_and_won_same_quarter_net_arr_combined,
    SUM(new_logo_count)                                   AS new_logo_count,
    SUM(amount)                                           AS amount,
    SUM(recurring_amount)                                 AS recurring_amount,
    SUM(true_up_amount)                                   AS true_up_amount,
    SUM(proserv_amount)                                   AS proserv_amount,
    SUM(other_non_recurring_amount)                       AS other_non_recurring_amount,
    SUM(arr_basis)                                        AS arr_basis,
    SUM(arr)                                              AS arr,
    SUM(count_crm_attribution_touchpoints)                AS count_crm_attribution_touchpoints,
    SUM(weighted_linear_iacv)                             AS weighted_linear_iacv,
    SUM(count_campaigns)                                  AS count_campaigns,
    SUM(probability)                                      AS probability,
    SUM(days_in_sao)                                      AS days_in_sao,
    SUM(open_1plus_deal_count)                            AS open_1plus_deal_count,
    SUM(open_3plus_deal_count)                            AS open_3plus_deal_count,
    SUM(open_4plus_deal_count)                            AS open_4plus_deal_count,
    SUM(booked_deal_count)                                AS booked_deal_count,
    SUM(churned_contraction_deal_count)                   AS churned_contraction_deal_count,
    SUM(open_1plus_net_arr)                               AS open_1plus_net_arr,
    SUM(open_3plus_net_arr)                               AS open_3plus_net_arr,
    SUM(open_4plus_net_arr)                               AS open_4plus_net_arr,
    SUM(booked_net_arr)                                   AS booked_net_arr,
    SUM(churned_contraction_net_arr)                      AS churned_contraction_net_arr,
    SUM(calculated_deal_count)                            AS calculated_deal_count,
    SUM(booked_churned_contraction_deal_count)            AS booked_churned_contraction_deal_count,
    SUM(booked_churned_contraction_net_arr)               AS booked_churned_contraction_net_arr,
    SUM(renewal_amount)                                   AS renewal_amount,
    SUM(total_contract_value)                             AS total_contract_value,
    SUM(days_in_stage)                                    AS days_in_stage,
    SUM(calculated_age_in_days)                           AS calculated_age_in_days,
    SUM(days_since_last_activity)                         AS days_since_last_activity,
    SUM(pre_military_invasion_arr)                        AS pre_military_invasion_arr,
    SUM(won_arr_basis_for_clari)                          AS won_arr_basis_for_clari,
    SUM(arr_basis_for_clari)                              AS arr_basis_for_clari,
    SUM(forecasted_churn_for_clari)                       AS forecasted_churn_for_clari,
    SUM(override_arr_basis_clari)                         AS override_arr_basis_clari,
    SUM(vsa_start_date_net_arr)                           AS vsa_start_date_net_arr,
    SUM(cycle_time_in_days_combined)                      AS cycle_time_in_days_combined
  FROM targets_actuals
  GROUP BY ALL

)

SELECT * 
FROM aggregate_data
WHERE NOT is_current_snapshot_quarter