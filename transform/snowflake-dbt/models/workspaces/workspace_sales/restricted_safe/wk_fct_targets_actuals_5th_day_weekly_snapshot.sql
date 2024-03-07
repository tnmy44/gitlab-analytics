WITH targets_actuals AS (

  SELECT * 
  FROM {{ ref('wk_fct_targets_actuals_snapshot') }}

),

day_7_list AS (

  SELECT 
    CASE WHEN date_actual = last_day_of_fiscal_quarter 
            THEN date_actual
        WHEN day_of_fiscal_quarter % 7 = 0 AND day_of_fiscal_quarter != 91
            THEN date_actual
        WHEN date_actual = first_day_of_fiscal_quarter
            THEN date_actual
    END AS day_7_current_week,
    day_of_fiscal_quarter,
    LAG(day_7_current_week) OVER (ORDER BY day_7_current_week) + 1 AS day_8_previous_week
  FROM {{ref('dim_date')}}
  WHERE day_7_current_week IS NOT NULL 
    AND date_actual >= DATEADD(YEAR, -2, current_first_day_of_fiscal_quarter) -- include only the last 8 quarters 

),

final AS (

  SELECT 
    targets_actuals.*,

    -- TABLEAU FIELDS
    -- Create flags to know whether an action happened in the current snapshot week 
    -- ie. Whether it happened between last Friday and current Thursday
    CASE
      WHEN created_date BETWEEN day_8_previous_week AND day_7_current_week
        THEN 1
      ELSE 0
    END AS is_created_in_snapshot_week, 
    CASE  
      WHEN close_date BETWEEN day_8_previous_week AND day_7_current_week
        THEN 1
      ELSE 0
    END AS is_close_in_snapshot_week, 
    CASE  
      WHEN arr_created_date BETWEEN day_8_previous_week AND day_7_current_week
        THEN 1
      ELSE 0
    END AS is_arr_created_in_snapshot_week, 
    CASE  
      WHEN arr_created_date BETWEEN day_8_previous_week AND day_7_current_week
        THEN 1
      ELSE 0
    END AS is_net_arr_created_in_snapshot_week, 
    CASE  
      WHEN pipeline_created_date BETWEEN day_8_previous_week AND day_7_current_week
        THEN 1
      ELSE 0
    END AS is_pipeline_created_in_snapshot_week,
    CASE  
      WHEN sales_accepted_date BETWEEN day_8_previous_week AND day_7_current_week
        THEN 1
      ELSE 0
    END AS is_sales_accepted_in_snapshot_week,
    CASE  
      WHEN stage_6_closed_won_date BETWEEN day_8_previous_week AND day_7_current_week
        THEN 1
      ELSE 0
    END AS is_closed_won_in_snapshot_week,
    CASE 
      WHEN stage_6_closed_lost_date BETWEEN day_8_previous_week AND day_7_current_week
        THEN 1
      ELSE 0
    END AS is_closed_lost_in_snapshot_week,


    -- Pull in the metric only when the corresponding flag is set
    -- ie. Only calculate created arr in the week where the opportunity was created

    -- ARR created in snapshot week (pipeline generated)
    CASE
      WHEN is_arr_created_in_snapshot_week = 1
        AND is_net_arr_pipeline_created_combined = 1
          AND stage_1_discovery_date IS NOT NULL -- Excludes closed lost opps that could have gone from stage 0 straight to lost
            THEN net_arr
      ELSE 0
    END AS created_arr_in_snapshot_week,
    CASE
      WHEN is_net_arr_created_in_snapshot_week = 1
        THEN raw_net_arr
      ELSE 0
    END AS created_net_arr_in_snapshot_week,
    CASE
      WHEN is_created_in_snapshot_week = 1
        THEN calculated_deal_count
      ELSE 0
    END AS created_deal_count_in_snapshot_week,
    CASE
      WHEN is_close_in_snapshot_week = 1
        THEN calculated_deal_count
      ELSE 0
    END AS closed_deal_count_in_snapshot_week,
    CASE
      WHEN is_close_in_snapshot_week = 1
        THEN net_arr
      ELSE 0
    END AS closed_net_arr_in_snapshot_week,
    CASE
      WHEN is_close_in_snapshot_week = 1
        THEN close_date - created_date
      ELSE 0
    END AS closed_cycle_time_in_snapshot_week,
    CASE 
      WHEN is_close_in_snapshot_week = 1
        THEN booked_net_arr
      ELSE 0
    END AS booked_net_arr_in_snapshot_week,
    CASE 
      WHEN is_pipeline_created_in_snapshot_week = 1
        AND is_net_arr_pipeline_created_combined = 1
          THEN net_arr
      ELSE 0
    END AS pipeline_created_in_snapshot_week,
    CASE 
      WHEN is_arr_created_in_snapshot_week = 1
        AND is_net_arr_pipeline_created_combined = 1
          THEN calculated_deal_count
      ELSE 0
    END AS calculated_deal_count_in_snapshot_week,
    CASE 
      WHEN is_eligible_open_pipeline_combined = 1
        THEN net_arr
      ELSE 0
    END AS open_pipeline_in_snapshot_week,
    CASE 
      WHEN is_closed_lost_in_snapshot_week = 1
        THEN 1
      ELSE 0
    END AS closed_lost_opps_in_snapshot_week,
    CASE 
      WHEN is_closed_won_in_snapshot_week = 1
        THEN 1
      ELSE 0
    END AS closed_won_opps_in_snapshot_week,
    CASE 
      WHEN is_closed_won_in_snapshot_week = 1
        OR is_closed_lost_in_snapshot_week = 1
          THEN 1
      ELSE 0
    END AS closed_opps_in_snapshot_week,
    IFF(snapshot_fiscal_quarter_date = current_first_day_of_fiscal_quarter, TRUE, FALSE) AS is_current_snapshot_quarter
  FROM targets_actuals
  INNER JOIN day_7_list
    ON targets_actuals.snapshot_date = day_7_list.day_7_current_week

)

SELECT * 
FROM final