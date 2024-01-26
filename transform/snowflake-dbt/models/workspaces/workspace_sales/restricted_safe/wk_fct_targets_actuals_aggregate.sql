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

quarterly_targets AS (

  SELECT  
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date,
    current_first_day_of_fiscal_quarter,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_crm_user_hierarchy_sk,
    SUM(deals_monthly_target)                            AS deals_quarterly_target,
    SUM(mql_monthly_target)                              AS mql_quarterly_target,
    SUM(net_arr_monthly_target)                          AS net_arr_quarterly_target,
    SUM(net_arr_pipeline_created_monthly_target)         AS net_arr_pipeline_created_quarterly_target,
    SUM(net_arr_company_monthly_target)                  AS net_arr_company_quarterly_target,
    SUM(new_logos_monthly_target)                        AS new_logos_quarterly_target,
    SUM(stage_1_opportunities_monthly_target)            AS stage_1_opportunities_quarterly_target, 
    SUM(total_closed_monthly_target)                     AS total_closed_quarterly_target,
    SUM(trials_monthly_target)                           AS trials_quarterly_target
  FROM targets_actuals
  GROUP BY 1,2,3,4,5,6

),

quarterly_totals AS (

  SELECT
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date,
    current_first_day_of_fiscal_quarter,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_crm_user_hierarchy_sk,
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
            AND is_net_arr_pipeline_created_combined = 1
              THEN created_and_won_same_quarter_net_arr_combined
          ELSE 0
        END )                                              AS total_pipe_generation_net_arr,
    SUM(CASE 
          WHEN pipeline_created_fiscal_quarter_date = snapshot_fiscal_quarter_date
            AND is_net_arr_pipeline_created_combined = 1
              THEN created_in_snapshot_quarter_deal_count
          ELSE 0
        END )                                              AS total_pipe_generation_deal_count,
    
    -- Created & Landed totals
    SUM(CASE 
          WHEN close_fiscal_quarter_date = snapshot_fiscal_quarter_date
              THEN created_and_won_same_quarter_net_arr_combined
          ELSE 0
        END)                                               AS total_created_and_booked_same_quarter_net_arr
  FROM targets_actuals
  WHERE is_excluded_from_pipeline_created_combined = 0
     AND is_deleted = 0
     AND snapshot_day_of_fiscal_quarter_normalised = 90
  GROUP BY 1,2,3,4,5,6

),

base_targets_actuals AS (

  SELECT 
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date,
    current_first_day_of_fiscal_quarter,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_crm_user_hierarchy_sk
  FROM quarterly_totals

  UNION

  SELECT 
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date,
    current_first_day_of_fiscal_quarter,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    dim_crm_user_hierarchy_sk
  FROM quarterly_targets

),

historical_targets_actuals AS (

  SELECT
    base_targets_actuals.snapshot_fiscal_quarter_name,
    base_targets_actuals.snapshot_fiscal_quarter_date,
    base_targets_actuals.current_first_day_of_fiscal_quarter,
    base_targets_actuals.dim_sales_qualified_source_id,
    base_targets_actuals.dim_order_type_id,
    base_targets_actuals.dim_crm_user_hierarchy_sk,
    deals_quarterly_target,
    mql_quarterly_target,
    net_arr_quarterly_target,
    net_arr_company_quarterly_target,
    new_logos_quarterly_target,
    stage_1_opportunities_quarterly_target, 
    total_closed_quarterly_target,
    trials_quarterly_target,
    total_booked_net_arr,
    total_churned_contraction_net_arr,       
    total_booked_deal_count,
    total_churned_contraction_deal_count,   
    total_pipe_generation_net_arr,
    total_pipe_generation_deal_count,
    total_created_and_booked_same_quarter_net_arr,
    -- check if we are in the current fiscal year or not. If not, use total, if we are use target
    CASE
      WHEN base_targets_actuals.current_first_day_of_fiscal_quarter <= base_targets_actuals.snapshot_fiscal_quarter_date
        THEN net_arr_quarterly_target
      ELSE total_booked_net_arr
    END                                         AS adjusted_quarterly_target_net_arr,
    CASE
      WHEN base_targets_actuals.current_first_day_of_fiscal_quarter <= base_targets_actuals.snapshot_fiscal_quarter_date
        THEN deals_quarterly_target
      ELSE total_booked_deal_count
    END                                         AS adjusted_quarterly_target_deals,
    CASE
      WHEN base_targets_actuals.current_first_day_of_fiscal_quarter <= base_targets_actuals.snapshot_fiscal_quarter_date
        THEN net_arr_pipeline_created_quarterly_target
      ELSE total_pipe_generation_net_arr
    END                                         AS adjusted_quarterly_target_net_arr_pipeline_created,
    IFF(base_targets_actuals.snapshot_fiscal_quarter_date = base_targets_actuals.current_first_day_of_fiscal_quarter, TRUE, FALSE) AS is_current_snapshot_quarter
  FROM base_targets_actuals
  LEFT JOIN quarterly_targets
    ON base_targets_actuals.snapshot_fiscal_quarter_date = quarterly_targets.snapshot_fiscal_quarter_date
      AND base_targets_actuals.dim_sales_qualified_source_id = quarterly_targets.dim_sales_qualified_source_id
        AND base_targets_actuals.dim_order_type_id = quarterly_targets.dim_order_type_id
          AND base_targets_actuals.dim_crm_user_hierarchy_sk = quarterly_targets.dim_crm_user_hierarchy_sk
  LEFT JOIN quarterly_totals
    ON base_targets_actuals.snapshot_fiscal_quarter_date = quarterly_totals.snapshot_fiscal_quarter_date
      AND base_targets_actuals.dim_sales_qualified_source_id = quarterly_totals.dim_sales_qualified_source_id
        AND base_targets_actuals.dim_order_type_id = quarterly_totals.dim_order_type_id
          AND base_targets_actuals.dim_crm_user_hierarchy_sk = quarterly_totals.dim_crm_user_hierarchy_sk
  

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
    IFF(snapshot_fiscal_quarter_date = current_first_day_of_fiscal_quarter, TRUE, FALSE) AS is_current_snapshot_quarter
  FROM targets_actuals
  INNER JOIN day_5_list
    ON targets_actuals.snapshot_date = day_5_list.day_5_current_week

)

SELECT * 
FROM historical_targets_actuals
