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

historical_targets_actuals AS (

  SELECT
    actuals_targets_pk,
    snapshot_date,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_crm_user_hierarchy_sk,

    -- attributes
    order_type_name,
    sales_qualified_source_name,
    crm_user_sales_segment, 
    crm_user_geo, 
    crm_user_region, 
    crm_user_area, 
    crm_user_business_unit,

     --dates
    snapshot_date,
    date_day,
    day_name,
    month_actual,
    year_actual,
    quarter_actual,
    day_of_week,
    first_day_of_week,
    week_of_year,
    day_of_month,
    day_of_quarter,
    day_of_year,
    fiscal_year,
    fiscal_quarter,
    day_of_fiscal_quarter,
    day_of_fiscal_year,
    month_name,
    first_day_of_month,
    last_day_of_month,
    first_day_of_year,
    last_day_of_year,
    first_day_of_quarter,
    last_day_of_quarter,
    first_day_of_fiscal_quarter,
    last_day_of_fiscal_quarter,
    first_day_of_fiscal_year,
    last_day_of_fiscal_year,
    week_of_fiscal_year,
    month_of_fiscal_year,
    last_day_of_week,
    quarter_name,
    fiscal_quarter_name,
    fiscal_quarter_name_fy,
    fiscal_quarter_number_absolute,
    fiscal_month_name,
    fiscal_month_name_fy,
    last_month_of_fiscal_quarter,
    last_month_of_fiscal_year,
    week_of_month_normalised,
    day_of_fiscal_quarter_normalised,
    week_of_fiscal_quarter_normalised,
    day_of_fiscal_year_normalised,
    current_date_actual,
    current_fiscal_year,
    current_first_day_of_fiscal_year,
    current_fiscal_quarter_name_fy,
    current_first_day_of_month,
    current_first_day_of_fiscal_quarter,
    current_day_of_month,
    current_day_of_fiscal_quarter,
    current_day_of_fiscal_year,

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

coverage_fields AS (

  SELECT
    historical_targets_actuals.*,
    -- check if we are in the current fiscal year or not. If not, use total, if we are use target
    CASE
      WHEN dim_date.current_fiscal_year <= close_fiscal_year
        THEN net_arr_daily_target
      ELSE booked_net_arr
    END                                         AS adjusted_daily_target_net_arr,
    CASE
      WHEN dim_date.current_fiscal_year <= close_fiscal_year
        THEN net_arr_monthly_target
      ELSE booked_net_arr
    END                                         AS adjusted_monthly_target_net_arr,
    CASE
      WHEN dim_date.current_fiscal_year <= close_fiscal_year
        THEN deals_daily_target
      ELSE booked_net_arr
    END                                         AS adjusted_daily_target_deals,
    CASE
      WHEN dim_date.current_fiscal_year <= close_fiscal_year
        THEN deals_monthly_target
      ELSE booked_net_arr
    END                                         AS adjusted_monthly_target_deals,
    CASE
      WHEN dim_date.current_fiscal_year <= close_fiscal_year
        THEN net_arr_pipeline_created_daily_target
      ELSE booked_net_arr
    END                                         AS adjusted_daily_target_net_arr_pipeline_created,
    CASE
      WHEN dim_date.current_fiscal_year <= close_fiscal_year
        THEN net_arr_pipeline_created_monthly_target
      ELSE booked_net_arr
    END                                         AS adjusted_monthly_target_net_arr_pipeline_created,
    IFF(fiscal_quarter_name_fy = current_fiscal_quarter_name_fy, TRUE, FALSE) AS is_current_snapshot_quarter
  FROM historical_targets_actuals

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

),


unioned AS (

  SELECT 
    actuals_targets_pk,
    dim_crm_opportunity_id,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_crm_user_hierarchy_sk,
    merged_crm_opportunity_id,
    dim_crm_account_id,
    dim_crm_person_id,
    sfdc_contact_id,
    record_type_id,
    dim_crm_opp_owner_stamped_hierarchy_sk,
    technical_evaluation_date_id,
    ssp_id,
    ga_client_id,
    order_type_name,
    sales_qualified_source_name,
    crm_user_sales_segment,
    crm_user_geo,
    crm_user_region,
    crm_user_area,
    crm_user_business_unit,
    snapshot_date,
    is_closed,
    is_won,
    is_refund,
    is_downgrade,
    is_swing_deal,
    is_edu_oss,
    is_web_portal_purchase,
    fpa_master_bookings_flag,
    is_sao,
    is_sdr_sao,
    is_net_arr_closed_deal,
    is_new_logo_first_order,
    is_net_arr_pipeline_created_combined,
    is_win_rate_calc,
    is_closed_won,
    is_stage_1_plus,
    is_stage_3_plus,
    is_stage_4_plus,
    is_lost,
    is_open,
    is_active,
    is_credit,
    is_renewal,
    is_deleted,
    is_excluded_from_pipeline_created_combined,
    is_duplicate,
    is_contract_reset,
    is_comp_new_logo_override,
    is_eligible_open_pipeline_combined,
    is_eligible_age_analysis_combined,
    is_eligible_churn_contraction,
    is_booked_net_arr,
    is_abm_tier_sao,
    is_abm_tier_closed_won,
    primary_solution_architect,
    product_details,
    product_category,
    products_purchased,
    growth_type,
    opportunity_deal_size,
    closed_buckets,
    lead_source,
    dr_partner_deal_type,
    dr_partner_engagement,
    partner_account,
    dr_status,
    dr_deal_id,
    dr_primary_registration,
    distributor,
    influence_partner,
    fulfillment_partner,
    platform_partner,
    partner_track,
    resale_partner_track,
    is_public_sector_opp,
    is_registration_from_portal,
    calculated_discount,
    partner_discount,
    partner_discount_calc,
    comp_channel_neutral,
    iacv,
    net_iacv,
    segment_order_type_iacv_to_net_arr_ratio,
    calculated_from_ratio_net_arr,
    net_arr,
    raw_net_arr,
    created_and_won_same_quarter_net_arr_combined,
    new_logo_count,
    amount,
    recurring_amount,
    true_up_amount,
    proserv_amount,
    other_non_recurring_amount,
    arr_basis,
    arr,
    count_crm_attribution_touchpoints,
    weighted_linear_iacv,
    count_campaigns,
    probability,
    days_in_sao,
    open_1plus_deal_count,
    open_3plus_deal_count,
    open_4plus_deal_count,
    booked_deal_count,
    churned_contraction_deal_count,
    open_1plus_net_arr,
    open_3plus_net_arr,
    open_4plus_net_arr,
    booked_net_arr,
    churned_contraction_net_arr,
    calculated_deal_count,
    booked_churned_contraction_deal_count,
    booked_churned_contraction_net_arr,
    renewal_amount,
    total_contract_value,
    days_in_stage,
    calculated_age_in_days,
    days_since_last_activity,
    pre_military_invasion_arr,
    won_arr_basis_for_clari,
    arr_basis_for_clari,
    forecasted_churn_for_clari,
    override_arr_basis_clari,
    vsa_start_date_net_arr,
    cycle_time_in_days_combined,
    date_day,
    day_name,
    month_actual,
    year_actual,
    quarter_actual,
    day_of_week,
    first_day_of_week,
    week_of_year,
    day_of_month,
    day_of_quarter,
    day_of_year,
    fiscal_year,
    fiscal_quarter,
    day_of_fiscal_quarter,
    day_of_fiscal_year,
    month_name,
    first_day_of_month,
    last_day_of_month,
    first_day_of_year,
    last_day_of_year,
    first_day_of_quarter,
    last_day_of_quarter,
    first_day_of_fiscal_quarter,
    last_day_of_fiscal_quarter,
    first_day_of_fiscal_year,
    last_day_of_fiscal_year,
    week_of_fiscal_year,
    month_of_fiscal_year,
    last_day_of_week,
    quarter_name,
    fiscal_quarter_name,
    fiscal_quarter_name_fy,
    fiscal_quarter_number_absolute,
    fiscal_month_name,
    fiscal_month_name_fy,
    holiday_desc,
    is_holiday,
    last_month_of_fiscal_quarter,
    is_first_day_of_last_month_of_fiscal_quarter,
    last_month_of_fiscal_year,
    is_first_day_of_last_month_of_fiscal_year,
    snapshot_date_fpa,
    snapshot_date_billings,
    days_in_month_count,
    week_of_month_normalised,
    day_of_fiscal_quarter_normalised,
    week_of_fiscal_quarter_normalised,
    day_of_fiscal_year_normalised,
    is_first_day_of_fiscal_quarter_week,
    days_until_last_day_of_month,
    current_date_actual,
    current_fiscal_year,
    current_first_day_of_fiscal_year,
    current_fiscal_quarter_name_fy,
    current_first_day_of_month,
    current_first_day_of_fiscal_quarter,
    current_day_of_month,
    current_day_of_fiscal_quarter,
    current_day_of_fiscal_year,
    created_date,
    created_month,
    created_fiscal_quarter_date,
    created_fiscal_quarter_name,
    created_fiscal_year,
    sales_accepted_date,
    sales_accepted_month,
    sales_accepted_fiscal_quarter_date,
    sales_accepted_fiscal_quarter_name,
    sales_accepted_fiscal_year,
    close_date,
    close_month,
    close_fiscal_quarter_date,
    close_fiscal_quarter_name,
    close_fiscal_year,
    stage_0_pending_acceptance_date,
    stage_0_pending_acceptance_month,
    stage_0_pending_acceptance_fiscal_quarter_date,
    stage_0_pending_acceptance_fiscal_quarter_name,
    stage_0_pending_acceptance_fiscal_year,
    stage_1_discovery_date,
    stage_1_discovery_month,
    stage_1_discovery_fiscal_quarter_date,
    stage_1_discovery_fiscal_quarter_name,
    stage_1_discovery_fiscal_year,
    stage_2_scoping_date,
    stage_2_scoping_month,
    stage_2_scoping_fiscal_quarter_date,
    stage_2_scoping_fiscal_quarter_name,
    stage_2_scoping_fiscal_year,
    stage_3_technical_evaluation_date,
    stage_3_technical_evaluation_month,
    stage_3_technical_evaluation_fiscal_quarter_date,
    stage_3_technical_evaluation_fiscal_quarter_name,
    stage_3_technical_evaluation_fiscal_year,
    stage_4_proposal_date,
    stage_4_proposal_month,
    stage_4_proposal_fiscal_quarter_date,
    stage_4_proposal_fiscal_quarter_name,
    stage_4_proposal_fiscal_year,
    stage_5_negotiating_date,
    stage_5_negotiating_month,
    stage_5_negotiating_fiscal_quarter_date,
    stage_5_negotiating_fiscal_quarter_name,
    stage_5_negotiating_fiscal_year,
    stage_6_awaiting_signature_date_date,
    stage_6_awaiting_signature_date_month,
    stage_6_awaiting_signature_date_fiscal_quarter_date,
    stage_6_awaiting_signature_date_fiscal_quarter_name,
    stage_6_awaiting_signature_date_fiscal_year,
    stage_6_closed_won_date,
    stage_6_closed_won_month,
    stage_6_closed_won_fiscal_quarter_date,
    stage_6_closed_won_fiscal_quarter_name,
    stage_6_closed_won_fiscal_year,
    stage_6_closed_lost_date,
    stage_6_closed_lost_month,
    stage_6_closed_lost_fiscal_quarter_date,
    stage_6_closed_lost_fiscal_quarter_name,
    stage_6_closed_lost_fiscal_year,
    subscription_start_date,
    subscription_start_month,
    subscription_start_fiscal_quarter_date,
    subscription_start_fiscal_quarter_name,
    subscription_start_fiscal_year,
    subscription_end_date,
    subscription_end_month,
    subscription_end_fiscal_quarter_date,
    subscription_end_fiscal_quarter_name,
    subscription_end_fiscal_year,
    sales_qualified_date,
    sales_qualified_month,
    sales_qualified_fiscal_quarter_date,
    sales_qualified_fiscal_quarter_name,
    sales_qualified_fiscal_year,
    last_activity_date,
    last_activity_month,
    last_activity_fiscal_quarter_date,
    last_activity_fiscal_quarter_name,
    last_activity_fiscal_year,
    sales_last_activity_date,
    sales_last_activity_month,
    sales_last_activity_fiscal_quarter_date,
    sales_last_activity_fiscal_quarter_name,
    sales_last_activity_fiscal_year,
    technical_evaluation_date,
    technical_evaluation_month,
    technical_evaluation_fiscal_quarter_date,
    technical_evaluation_fiscal_quarter_name,
    technical_evaluation_fiscal_year,
    arr_created_date,
    arr_created_month,
    arr_created_fiscal_quarter_date,
    arr_created_fiscal_quarter_name,
    arr_created_fiscal_year,
    pipeline_created_date,
    pipeline_created_month,
    pipeline_created_fiscal_quarter_date,
    pipeline_created_fiscal_quarter_name,
    pipeline_created_fiscal_year,
    net_arr_created_date,
    net_arr_created_month,
    net_arr_created_fiscal_quarter_date,
    net_arr_created_fiscal_quarter_name,
    net_arr_created_fiscal_year,
    is_created_in_snapshot_week,
    is_close_in_snapshot_week,
    is_arr_created_in_snapshot_week,
    is_net_arr_created_in_snapshot_week,
    is_pipeline_created_in_snapshot_week,
    is_sales_accepted_in_snapshot_week,
    created_arr_in_snapshot_week,
    created_net_arr_in_snapshot_week,
    created_deal_count_in_snapshot_week,
    closed_net_arr_in_snapshot_week,
    closed_deal_count_in_snapshot_week,
    closed_new_logo_count_in_snapshot_week,
    closed_cycle_time_in_snapshot_week,
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
    adjusted_daily_target_net_arr,
    adjusted_monthly_target_net_arr,
    adjusted_daily_target_deals,
    adjusted_monthly_target_deals,
    adjusted_daily_target_net_arr_pipeline_created,
    adjusted_monthly_target_net_arr_pipeline_created
  FROM granular_data

)

SELECT * 
FROM unioned
