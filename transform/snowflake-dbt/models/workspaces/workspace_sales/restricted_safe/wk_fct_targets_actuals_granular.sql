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
FROM granular_data
WHERE is_current_snapshot_quarter