WITH dim_date AS (

  SELECT *
  FROM {{ref('dim_date')}}
  
),


targets AS (

  SELECT *
  FROM {{ref('wk_fct_sales_funnel_target_daily_pivoted')}}

),

actuals AS (

  SELECT *
  FROM {{ref('wk_fct_crm_opportunity_daily_snapshot')}}

),

day_5_list AS (

  SELECT 
    date_actual AS day_5_current_week,
    LAG(day_5_current_week) OVER (ORDER BY day_5_current_week) + 1 AS day_6_previous_week -- Add an extra day to exclude the previous thursday from the calculation
  FROM dim_date
  WHERE day_of_week = 5

),

combined AS (

  SELECT 
    -- Keys
    actuals.actuals_targets_daily_pk,
    actuals.dim_crm_opportunity_id,
    actuals.dim_sales_qualified_source_id,
    actuals.dim_order_type_id,
    actuals.dim_crm_user_hierarchy_sk,
    actuals.merged_crm_opportunity_id,
    actuals.dim_crm_account_id,
    actuals.dim_crm_person_id,
    actuals.sfdc_contact_id,
    actuals.record_type_id,
    actuals.dim_crm_opp_owner_stamped_hierarchy_sk,
    actuals.technical_evaluation_date_id,
    actuals.ssp_id,
    actuals.ga_client_id,

    -- snapshot info
    actuals.snapshot_date,

    -- flags
    actuals.is_closed,
    actuals.is_won,
    actuals.is_refund,
    actuals.is_downgrade,
    actuals.is_swing_deal,
    actuals.is_edu_oss,
    actuals.is_web_portal_purchase,
    actuals.fpa_master_bookings_flag,
    actuals.is_sao,
    actuals.is_sdr_sao,
    actuals.is_net_arr_closed_deal,
    actuals.is_new_logo_first_order,
    actuals.is_net_arr_pipeline_created_combined,
    actuals.is_win_rate_calc,
    actuals.is_closed_won,
    actuals.is_stage_1_plus,
    actuals.is_stage_3_plus,
    actuals.is_stage_4_plus,
    actuals.is_lost,
    actuals.is_open,
    actuals.is_active,
    actuals.is_credit,
    actuals.is_renewal,
    actuals.is_deleted,
    actuals.is_excluded_from_pipeline_created_combined,
    actuals.is_duplicate,
    actuals.is_contract_reset,
    actuals.is_comp_new_logo_override,
    actuals.is_eligible_open_pipeline_combined,
    actuals.is_eligible_age_analysis_combined,
    actuals.is_eligible_churn_contraction,
    actuals.is_booked_net_arr,
    actuals.is_abm_tier_sao,
    actuals.is_abm_tier_closed_won,

    actuals.primary_solution_architect,
    actuals.product_details,
    actuals.product_category,
    actuals.products_purchased,
    actuals.growth_type,
    actuals.opportunity_deal_size,
    actuals.closed_buckets,

    --channel fields
    actuals.lead_source,
    actuals.dr_partner_deal_type,
    actuals.dr_partner_engagement,
    actuals.partner_account,
    actuals.dr_status,
    actuals.dr_deal_id,
    actuals.dr_primary_registration,
    actuals.distributor,
    actuals.influence_partner,
    actuals.fulfillment_partner,
    actuals.platform_partner,
    actuals.partner_track,
    actuals.resale_partner_track,
    actuals.is_public_sector_opp,
    actuals.is_registration_from_portal,
    actuals.calculated_discount,
    actuals.partner_discount,
    actuals.partner_discount_calc,
    actuals.comp_channel_neutral,

    --additive fields
    actuals.iacv,
    actuals.net_iacv,
    actuals.segment_order_type_iacv_to_net_arr_ratio,
    actuals.calculated_from_ratio_net_arr,
    actuals.net_arr,
    actuals.raw_net_arr,
    actuals.created_and_won_same_quarter_net_arr_combined,
    actuals.new_logo_count,
    actuals.amount,
    actuals.recurring_amount,
    actuals.true_up_amount,
    actuals.proserv_amount,
    actuals.other_non_recurring_amount,
    actuals.arr_basis,
    actuals.arr,
    actuals.count_crm_attribution_touchpoints,
    actuals.weighted_linear_iacv,
    actuals.count_campaigns,
    actuals.probability,
    actuals.days_in_sao,
    actuals.open_1plus_deal_count,
    actuals.open_3plus_deal_count,
    actuals.open_4plus_deal_count,
    actuals.booked_deal_count,
    actuals.churned_contraction_deal_count,
    actuals.open_1plus_net_arr,
    actuals.open_3plus_net_arr,
    actuals.open_4plus_net_arr,
    actuals.booked_net_arr,
    actuals.churned_contraction_net_arr,
    actuals.calculated_deal_count,
    actuals.booked_churned_contraction_deal_count,
    actuals.booked_churned_contraction_net_arr,
    actuals.renewal_amount,
    actuals.total_contract_value,
    actuals.days_in_stage,
    actuals.calculated_age_in_days,
    actuals.days_since_last_activity,
    actuals.pre_military_invasion_arr,
    actuals.won_arr_basis_for_clari,
    actuals.arr_basis_for_clari,
    actuals.forecasted_churn_for_clari,
    actuals.override_arr_basis_clari,
    actuals.vsa_start_date_net_arr,
    actuals.cycle_time_in_days_combined,
    actuals.day_of_week,
    actuals.first_day_of_week,
    actuals.date_id,
    actuals.fiscal_month_name_fy,
    actuals.fiscal_quarter_name_fy,
    actuals.first_day_of_fiscal_quarter,
    actuals.first_day_of_fiscal_year,
    actuals.last_day_of_week,
    actuals.last_day_of_month,
    actuals.last_day_of_fiscal_quarter,
    actuals.last_day_of_fiscal_year,


    --dates
    created_date.date_actual                                        AS created_date,
    created_date.first_day_of_month                                 AS created_month,
    created_date.first_day_of_fiscal_quarter                        AS created_fiscal_quarter_date,
    created_date.fiscal_quarter_name_fy                             AS created_fiscal_quarter_name,
    created_date.fiscal_year                                        AS created_fiscal_year,
    sales_accepted_date.date_actual                                 AS sales_accepted_date,
    sales_accepted_date.first_day_of_month                          AS sales_accepted_month,
    sales_accepted_date.first_day_of_fiscal_quarter                 AS sales_accepted_fiscal_quarter_date,
    sales_accepted_date.fiscal_quarter_name_fy                      AS sales_accepted_fiscal_quarter_name,
    sales_accepted_date.fiscal_year                                 AS sales_accepted_fiscal_year,
    close_date.date_actual                                          AS close_date,
    close_date.first_day_of_month                                   AS close_month,
    close_date.first_day_of_fiscal_quarter                          AS close_fiscal_quarter_date,
    close_date.fiscal_quarter_name_fy                               AS close_fiscal_quarter_name,
    close_date.fiscal_year                                          AS close_fiscal_year,
    stage_0_pending_acceptance_date.date_actual                     AS stage_0_pending_acceptance_date,
    stage_0_pending_acceptance_date.first_day_of_month              AS stage_0_pending_acceptance_month,
    stage_0_pending_acceptance_date.first_day_of_fiscal_quarter     AS stage_0_pending_acceptance_fiscal_quarter_date,
    stage_0_pending_acceptance_date.fiscal_quarter_name_fy          AS stage_0_pending_acceptance_fiscal_quarter_name,
    stage_0_pending_acceptance_date.fiscal_year                     AS stage_0_pending_acceptance_fiscal_year,
    stage_1_discovery_date.date_actual                              AS stage_1_discovery_date,
    stage_1_discovery_date.first_day_of_month                       AS stage_1_discovery_month,
    stage_1_discovery_date.first_day_of_fiscal_quarter              AS stage_1_discovery_fiscal_quarter_date,
    stage_1_discovery_date.fiscal_quarter_name_fy                   AS stage_1_discovery_fiscal_quarter_name,
    stage_1_discovery_date.fiscal_year                              AS stage_1_discovery_fiscal_year,
    stage_2_scoping_date.date_actual                                AS stage_2_scoping_date,
    stage_2_scoping_date.first_day_of_month                         AS stage_2_scoping_month,
    stage_2_scoping_date.first_day_of_fiscal_quarter                AS stage_2_scoping_fiscal_quarter_date,
    stage_2_scoping_date.fiscal_quarter_name_fy                     AS stage_2_scoping_fiscal_quarter_name,
    stage_2_scoping_date.fiscal_year                                AS stage_2_scoping_fiscal_year,
    stage_3_technical_evaluation_date.date_actual                   AS stage_3_technical_evaluation_date,
    stage_3_technical_evaluation_date.first_day_of_month            AS stage_3_technical_evaluation_month,
    stage_3_technical_evaluation_date.first_day_of_fiscal_quarter   AS stage_3_technical_evaluation_fiscal_quarter_date,
    stage_3_technical_evaluation_date.fiscal_quarter_name_fy        AS stage_3_technical_evaluation_fiscal_quarter_name,
    stage_3_technical_evaluation_date.fiscal_year                   AS stage_3_technical_evaluation_fiscal_year,
    stage_4_proposal_date.date_actual                               AS stage_4_proposal_date,
    stage_4_proposal_date.first_day_of_month                        AS stage_4_proposal_month,
    stage_4_proposal_date.first_day_of_fiscal_quarter               AS stage_4_proposal_fiscal_quarter_date,
    stage_4_proposal_date.fiscal_quarter_name_fy                    AS stage_4_proposal_fiscal_quarter_name,
    stage_4_proposal_date.fiscal_year                               AS stage_4_proposal_fiscal_year,
    stage_5_negotiating_date.date_actual                            AS stage_5_negotiating_date,
    stage_5_negotiating_date.first_day_of_month                     AS stage_5_negotiating_month,
    stage_5_negotiating_date.first_day_of_fiscal_quarter            AS stage_5_negotiating_fiscal_quarter_date,
    stage_5_negotiating_date.fiscal_quarter_name_fy                 AS stage_5_negotiating_fiscal_quarter_name,
    stage_5_negotiating_date.fiscal_year                            AS stage_5_negotiating_fiscal_year,
    stage_6_awaiting_signature_date.date_actual                     AS stage_6_awaiting_signature_date_date,
    stage_6_awaiting_signature_date.first_day_of_month              AS stage_6_awaiting_signature_date_month,
    stage_6_awaiting_signature_date.first_day_of_fiscal_quarter     AS stage_6_awaiting_signature_date_fiscal_quarter_date,
    stage_6_awaiting_signature_date.fiscal_quarter_name_fy          AS stage_6_awaiting_signature_date_fiscal_quarter_name,
    stage_6_awaiting_signature_date.fiscal_year                     AS stage_6_awaiting_signature_date_fiscal_year,
    stage_6_closed_won_date.date_actual                             AS stage_6_closed_won_date,
    stage_6_closed_won_date.first_day_of_month                      AS stage_6_closed_won_month,
    stage_6_closed_won_date.first_day_of_fiscal_quarter             AS stage_6_closed_won_fiscal_quarter_date,
    stage_6_closed_won_date.fiscal_quarter_name_fy                  AS stage_6_closed_won_fiscal_quarter_name,
    stage_6_closed_won_date.fiscal_year                             AS stage_6_closed_won_fiscal_year,
    stage_6_closed_lost_date.date_actual                            AS stage_6_closed_lost_date,
    stage_6_closed_lost_date.first_day_of_month                     AS stage_6_closed_lost_month,
    stage_6_closed_lost_date.first_day_of_fiscal_quarter            AS stage_6_closed_lost_fiscal_quarter_date,
    stage_6_closed_lost_date.fiscal_quarter_name_fy                 AS stage_6_closed_lost_fiscal_quarter_name,
    stage_6_closed_lost_date.fiscal_year                            AS stage_6_closed_lost_fiscal_year,
    subscription_start_date.date_actual                             AS subscription_start_date,
    subscription_start_date.first_day_of_month                      AS subscription_start_month,
    subscription_start_date.first_day_of_fiscal_quarter             AS subscription_start_fiscal_quarter_date,
    subscription_start_date.fiscal_quarter_name_fy                  AS subscription_start_fiscal_quarter_name,
    subscription_start_date.fiscal_year                             AS subscription_start_fiscal_year,
    subscription_END_date.date_actual                               AS subscription_END_date,
    subscription_END_date.first_day_of_month                        AS subscription_END_month,
    subscription_END_date.first_day_of_fiscal_quarter               AS subscription_END_fiscal_quarter_date,
    subscription_END_date.fiscal_quarter_name_fy                    AS subscription_END_fiscal_quarter_name,
    subscription_END_date.fiscal_year                               AS subscription_END_fiscal_year,
    sales_qualified_date.date_actual                                AS sales_qualified_date,
    sales_qualified_date.first_day_of_month                         AS sales_qualified_month,
    sales_qualified_date.first_day_of_fiscal_quarter                AS sales_qualified_fiscal_quarter_date,
    sales_qualified_date.fiscal_quarter_name_fy                     AS sales_qualified_fiscal_quarter_name,
    sales_qualified_date.fiscal_year                                AS sales_qualified_fiscal_year,
    last_activity_date.date_actual                                  AS last_activity_date,
    last_activity_date.first_day_of_month                           AS last_activity_month,
    last_activity_date.first_day_of_fiscal_quarter                  AS last_activity_fiscal_quarter_date,
    last_activity_date.fiscal_quarter_name_fy                       AS last_activity_fiscal_quarter_name,
    last_activity_date.fiscal_year                                  AS last_activity_fiscal_year,
    sales_last_activity_date.date_actual                            AS sales_last_activity_date,
    sales_last_activity_date.first_day_of_month                     AS sales_last_activity_month,
    sales_last_activity_date.first_day_of_fiscal_quarter            AS sales_last_activity_fiscal_quarter_date,
    sales_last_activity_date.fiscal_quarter_name_fy                 AS sales_last_activity_fiscal_quarter_name,
    sales_last_activity_date.fiscal_year                            AS sales_last_activity_fiscal_year,
    technical_evaluation_date.date_actual                           AS technical_evaluation_date,
    technical_evaluation_date.first_day_of_month                    AS technical_evaluation_month,
    technical_evaluation_date.first_day_of_fiscal_quarter           AS technical_evaluation_fiscal_quarter_date,
    technical_evaluation_date.fiscal_quarter_name_fy                AS technical_evaluation_fiscal_quarter_name,
    technical_evaluation_date.fiscal_year                           AS technical_evaluation_fiscal_year,
    arr_created_date.date_actual                                    AS arr_created_date,
    arr_created_date.first_day_of_month                             AS arr_created_month,
    arr_created_date.first_day_of_fiscal_quarter                    AS arr_created_fiscal_quarter_date,
    arr_created_date.fiscal_quarter_name_fy                         AS arr_created_fiscal_quarter_name,
    arr_created_date.fiscal_year                                    AS arr_created_fiscal_year,
    arr_created_date.date_actual                                    AS pipeline_created_date,
    arr_created_date.first_day_of_month                             AS pipeline_created_month,
    arr_created_date.first_day_of_fiscal_quarter                    AS pipeline_created_fiscal_quarter_date,
    arr_created_date.fiscal_quarter_name_fy                         AS pipeline_created_fiscal_quarter_name,
    arr_created_date.fiscal_year                                    AS pipeline_created_fiscal_year,
    arr_created_date.date_actual                                    AS net_arr_created_date,
    arr_created_date.first_day_of_month                             AS net_arr_created_month,
    arr_created_date.first_day_of_fiscal_quarter                    AS net_arr_created_fiscal_quarter_date,
    arr_created_date.fiscal_quarter_name_fy                         AS net_arr_created_fiscal_quarter_name,
    arr_created_date.fiscal_year                                    AS net_arr_created_fiscal_year,

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

    -- TARGETS
    "Deals Daily Target",
    "Deals Monthly Target",
    "Deals WTD Target",
    "Deals MTD Target",
    "Deals QTD Target",
    "Deals YTD Target",
    "MQL Daily Target",
    "MQL Monthly Target",
    "MQL WTD Target",
    "MQL MTD Target",
    "MQL QTD Target",
    "MQL YTD Target",
    "Net ARR Daily Target",
    "Net ARR Monthly Target",
    "Net ARR WTD Target",
    "Net ARR MTD Target",
    "Net ARR QTD Target",
    "Net ARR YTD Target",
    "Net ARR Company Daily Target",
    "Net ARR Company Monthly Target",
    "Net ARR Company WTD Target",
    "Net ARR Company MTD Target",
    "Net ARR Company QTD Target",
    "Net ARR Company YTD Target",
    "Net ARR Pipeline Created Daily Target",
    "Net ARR Pipeline Created Monthly Target",
    "Net ARR Pipeline Created WTD Target",
    "Net ARR Pipeline Created MTD Target",
    "Net ARR Pipeline Created QTD Target",
    "Net ARR Pipeline Created YTD Target",
    "New Logos Daily Target",
    "New Logos Monthly Target",
    "New Logos WTD Target",
    "New Logos MTD Target",
    "New Logos QTD Target",
    "New Logos YTD Target",
    "Stage 1 Opportunities Daily Target",
    "Stage 1 Opportunities Monthly Target",
    "Stage 1 Opportunities WTD Target",
    "Stage 1 Opportunities MTD Target",
    "Stage 1 Opportunities QTD Target",
    "Stage 1 Opportunities YTD Target",
    "Total Closed Daily Target",
    "Total Closed Monthly Target",
    "Total Closed WTD Target",
    "Total Closed MTD Target",
    "Total Closed QTD Target",
    "Total Closed YTD Target"
    "Trials Daily Target",
    "Trials Monthly Target",
    "Trials WTD Target",
    "Trials MTD Target",
    "Trials QTD Target",
    "Trials YTD Target"
  FROM actuals
  INNER JOIN day_5_list
    ON actuals.snapshot_date = day_5_list.day_5_current_week
  LEFT JOIN targets 
    ON actuals.actuals_targets_daily_pk = targets.actuals_targets_daily_pk 
  LEFT JOIN dim_date created_date
    ON actuals.created_date_id = created_date.date_id
  LEFT JOIN dim_date sales_accepted_date
    ON actuals.sales_accepted_date_id = sales_accepted_date.date_id
  LEFT JOIN dim_date close_date
    ON actuals.close_date_id = close_date.date_id
  LEFT JOIN dim_date stage_0_pending_acceptance_date
    ON actuals.stage_0_pending_acceptance_date_id = stage_0_pending_acceptance_date.date_id
  LEFT JOIN dim_date stage_1_discovery_date
    ON actuals.stage_1_discovery_date_id = stage_1_discovery_date.date_id
  LEFT JOIN dim_date stage_2_scoping_date
    ON actuals.stage_2_scoping_date_id = stage_2_scoping_date.date_id
  LEFT JOIN dim_date stage_3_technical_evaluation_date
    ON actuals.stage_3_technical_evaluation_date_id = stage_3_technical_evaluation_date.date_id
  LEFT JOIN dim_date stage_4_proposal_date
    ON actuals.stage_4_proposal_date_id = stage_4_proposal_date.date_id
  LEFT JOIN dim_date stage_5_negotiating_date
    ON actuals.stage_5_negotiating_date_id = stage_5_negotiating_date.date_id
  LEFT JOIN dim_date stage_6_awaiting_signature_date
    ON actuals.stage_6_awaiting_signature_date_id = stage_6_awaiting_signature_date.date_id
  LEFT JOIN dim_date stage_6_closed_won_date
    ON actuals.stage_6_closed_won_date_id = stage_6_closed_won_date.date_id
  LEFT JOIN dim_date stage_6_closed_lost_date
    ON actuals.stage_6_closed_lost_date_id = stage_6_closed_lost_date.date_id
  LEFT JOIN dim_date subscription_start_date
    ON actuals.subscription_start_date_id = subscription_start_date.date_id
  LEFT JOIN dim_date subscription_END_date
    ON actuals.subscription_END_date_id = subscription_END_date.date_id
  LEFT JOIN dim_date sales_qualified_date
    ON actuals.sales_qualified_date_id = sales_qualified_date.date_id
  LEFT JOIN dim_date last_activity_date
    ON actuals.last_activity_date_id = last_activity_date.date_id
  LEFT JOIN dim_date sales_last_activity_date
    ON actuals.sales_last_activity_date_id = sales_last_activity_date.date_id
  LEFT JOIN dim_date technical_evaluation_date
    ON actuals.technical_evaluation_date_id = technical_evaluation_date.date_id
  LEFT JOIN dim_date arr_created_date
    ON actuals.arr_created_date_id = arr_created_date.date_id
  

)

SELECT * 
FROM combined
