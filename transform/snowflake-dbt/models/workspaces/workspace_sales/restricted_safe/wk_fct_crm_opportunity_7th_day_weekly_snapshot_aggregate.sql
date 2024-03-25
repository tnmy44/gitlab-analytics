WITH actuals AS (

  SELECT * 
  FROM {{ ref('wk_fct_crm_opportunity_daily_snapshot') }}

),

dim_date AS (

  SELECT * 
  FROM {{ ref('dim_date') }}


),

day_7_list AS (
  
  {{ date_spine_7th_day() }}

),

metrics_totals AS (

  -- Add the totals from previous quarters as attributes to aid with pipeline coverage and pacing calculations

  SELECT  
    {{ dbt_utils.generate_surrogate_key(['snapshot_fiscal_quarter_date', 'dim_sales_qualified_source_id', 'dim_order_type_id','dim_order_type_live_id','dim_crm_current_account_set_hierarchy_sk','deal_path_name','sales_type']) }} AS unique_key,
    SUM(created_arr)                  AS created_arr_quarter_total,
    SUM(closed_won_opps)              AS closed_won_opps_quarter_total,
    SUM(closed_opps)                  AS closed_opps_quarter_total,
    SUM(closed_net_arr)               AS closed_net_arr_quarter_total,
    SUM(net_arr)                      AS net_arr_quarter_total,
    SUM(cycle_time_in_days_combined)  AS cycle_time_in_days_combined_quarter_total,
    SUM(booked_deal_count)            AS booked_deal_count_quarter_total,
    SUM(booked_net_arr)               AS booked_net_arr_quarter_total
  FROM actuals
  LEFT JOIN dim_date 
    ON actuals.snapshot_date = dim_date.date_actual
  WHERE actuals.snapshot_date = dim_date.last_day_of_fiscal_quarter
  GROUP BY unique_key

),

aggregate_data AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['snapshot_fiscal_quarter_date', 'dim_sales_qualified_source_id', 'dim_order_type_id','dim_order_type_live_id','dim_crm_current_account_set_hierarchy_sk','deal_path_name','sales_type']) }} AS unique_key,
    -- keys
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_order_type_live_id,
    dim_crm_user_hierarchy_sk,
    dim_crm_current_account_set_hierarchy_sk,
    dim_crm_opp_owner_stamped_hierarchy_sk,

    -- attributes
    order_type,
    order_type_live,
    order_type_grouped,
    stage_name,
    deal_path_name,
    sales_type,
    sales_qualified_source_name,
    sales_qualified_source_grouped,

    --dates
    snapshot_id,
    snapshot_date,
    snapshot_month,
    snapshot_fiscal_year,
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date,
    snapshot_day_of_fiscal_quarter_normalised,
    snapshot_day_of_fiscal_year_normalised,

    -- Running sum of metrics
    SUM(created_arr_in_snapshot_quarter)                               AS created_arr_in_snapshot_quarter,
    SUM(closed_won_opps_in_snapshot_quarter)                           AS closed_won_opps_in_snapshot_quarter,
    SUM(closed_opps_in_snapshot_quarter)                               AS closed_opps_in_snapshot_quarter,
    SUM(closed_net_arr_in_snapshot_quarter)                            AS closed_net_arr_in_snapshot_quarter,
    SUM(booked_net_arr_in_snapshot_quarter)                            AS booked_net_arr_in_snapshot_quarter,

    -- Additive fields
    
    SUM(created_arr)                                                   AS created_arr,
    SUM(closed_won_opps)                                               AS closed_won_opps,
    SUM(closed_opps)                                                   AS closed_opps,
    SUM(closed_net_arr)                                                AS closed_net_arr,
    SUM(segment_order_type_iacv_to_net_arr_ratio)                      AS segment_order_type_iacv_to_net_arr_ratio,
    SUM(calculated_from_ratio_net_arr)                                 AS calculated_from_ratio_net_arr,
    SUM(net_arr)                                                       AS net_arr,
    SUM(raw_net_arr)                                                   AS raw_net_arr,
    SUM(created_and_won_same_quarter_net_arr_combined)                 AS created_and_won_same_quarter_net_arr_combined,
    SUM(new_logo_count)                                                AS new_logo_count,
    SUM(amount)                                                        AS amount,
    SUM(recurring_amount)                                              AS recurring_amount,
    SUM(true_up_amount)                                                AS true_up_amount,
    SUM(proserv_amount)                                                AS proserv_amount,
    SUM(other_non_recurring_amount)                                    AS other_non_recurring_amount,
    SUM(arr_basis)                                                     AS arr_basis,
    SUM(arr)                                                           AS arr,
    SUM(count_crm_attribution_touchpoints)                             AS count_crm_attribution_touchpoints,
    SUM(weighted_linear_iacv)                                          AS weighted_linear_iacv,
    SUM(count_campaigns)                                               AS count_campaigns,
    SUM(probability)                                                   AS probability,
    SUM(days_in_sao)                                                   AS days_in_sao,
    SUM(open_1plus_deal_count)                                         AS open_1plus_deal_count,
    SUM(open_3plus_deal_count)                                         AS open_3plus_deal_count,
    SUM(open_4plus_deal_count)                                         AS open_4plus_deal_count,
    SUM(booked_deal_count)                                             AS booked_deal_count,
    SUM(churned_contraction_deal_count)                                AS churned_contraction_deal_count,
    SUM(open_1plus_net_arr)                                            AS open_1plus_net_arr,
    SUM(open_3plus_net_arr)                                            AS open_3plus_net_arr,
    SUM(open_4plus_net_arr)                                            AS open_4plus_net_arr,
    SUM(booked_net_arr)                                                AS booked_net_arr,
    SUM(churned_contraction_net_arr)                                   AS churned_contraction_net_arr,
    SUM(calculated_deal_count)                                         AS calculated_deal_count,
    SUM(booked_churned_contraction_deal_count)                         AS booked_churned_contraction_deal_count,
    SUM(booked_churned_contraction_net_arr)                            AS booked_churned_contraction_net_arr,
    SUM(renewal_amount)                                                AS renewal_amount,
    SUM(total_contract_value)                                          AS total_contract_value,
    SUM(days_in_stage)                                                 AS days_in_stage,
    SUM(calculated_age_in_days)                                        AS calculated_age_in_days,
    SUM(days_since_last_activity)                                      AS days_since_last_activity,
    SUM(pre_military_invasion_arr)                                     AS pre_military_invasion_arr,
    SUM(won_arr_basis_for_clari)                                       AS won_arr_basis_for_clari,
    SUM(arr_basis_for_clari)                                           AS arr_basis_for_clari,
    SUM(forecasted_churn_for_clari)                                    AS forecasted_churn_for_clari,
    SUM(override_arr_basis_clari)                                      AS override_arr_basis_clari,
    SUM(vsa_start_date_net_arr)                                        AS vsa_start_date_net_arr,
    SUM(cycle_time_in_days_combined)                                   AS cycle_time_in_days_combined
  FROM actuals
  INNER JOIN day_7_list
    ON actuals.snapshot_date = day_7_list.day_7
  GROUP BY ALL

),

combined AS (

  SELECT 
    aggregate_data.*,
    created_arr_quarter_total,
    closed_won_opps_quarter_total,
    closed_opps_quarter_total,
    closed_net_arr_quarter_total,
    net_arr_quarter_total,
    cycle_time_in_days_combined_quarter_total,
    booked_deal_count_quarter_total,
    booked_net_arr_quarter_total
  FROM aggregate_data
  LEFT JOIN metrics_totals
    ON aggregate_data.unique_key = metrics_totals.unique_key
    
)

SELECT *
FROM combined