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
    AND date_actual >= DATEADD(YEAR, -2, current_first_day_of_fiscal_quarter) -- include only the last 8 quarters

),

final AS (

  SELECT 
    targets_actuals.*,

    -- TABLEAU FIELDS
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
    CASE  
      WHEN stage_6_closed_won_date BETWEEN day_6_previous_week AND day_5_current_week
        THEN 1
      ELSE 0
    END AS is_closed_won_in_snapshot_week,
    CASE 
      WHEN stage_6_closed_lost_date BETWEEN day_6_previous_week AND day_5_current_week
        THEN 1
      ELSE 0
    END AS is_closed_lost_in_snapshot_week,


    -- Pull in the metric only when the corresponding flag is set
    -- ie. Only calculate created arr in the week where the opportunity was created

    -- ARR Generated (pipeline generation) 
    CASE
      WHEN is_arr_created_in_snapshot_week = 1
        AND is_net_arr_pipeline_created = 1
          AND stage_1_discovery_date IS NOT NULL
            THEN net_arr
      ELSE 0
    END AS created_arr_in_snapshot_week,

    -- Deals Generated 
    CASE
      WHEN is_created_in_snapshot_week = 1
        THEN calculated_deal_count
      ELSE 0
    END AS created_deal_count_in_snapshot_week,

    -- Booked Net ARR
    CASE
      WHEN is_close_in_snapshot_week = 1
        THEN booked_net_arr
      ELSE 0
    END AS booked_net_arr_in_snapshot_week,

    -- Deals closed lost
    CASE
      WHEN is_close_in_snapshot_week = 1 
        AND stage_name = '8-Closed Lost'
          AND is_net_arr_pipeline_created = 1
            THEN calculated_deal_count
      ELSE 0
    END AS closed_lost_opps_in_snapshot_week,

    -- Deals closed won
    CASE
      WHEN is_close_in_snapshot_week = 1 
        AND stage_name = 'Closed Won'
          AND is_net_arr_pipeline_created = 1
            THEN calculated_deal_count
      ELSE 0
    END AS closed_won_opps_in_snapshot_week,

    -- All closed deals
    CASE
      WHEN is_close_in_snapshot_week = 1 
        AND stage_name IN ('Closed Won', '8-Closed Lost')
          AND is_net_arr_pipeline_created = 1
            THEN calculated_deal_count
      ELSE 0
    END AS closed_opps_in_snapshot_week,

    -- Deal Cycle
    CASE 
      WHEN is_close_in_snapshot_week = 1 
        CASE WHEN is_renewal = 1
          THEN close_date - arr_created_date
        ELSE close_date - created_date 
    END AS closed_cycle_time_in_snapshot_week,


    IFF(snapshot_fiscal_quarter_date = current_first_day_of_fiscal_quarter, TRUE, FALSE) AS is_current_snapshot_quarter
  FROM targets_actuals
  INNER JOIN day_5_list
    ON targets_actuals.snapshot_date = day_5_list.day_5_current_week

)

SELECT * 
FROM final