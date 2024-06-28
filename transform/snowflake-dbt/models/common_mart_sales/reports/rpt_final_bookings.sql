WITH live_actuals AS (

  SELECT * 
  FROM {{ ref('mart_crm_opportunity_stamped_hierarchy_hist')}}

),

distinct_quarters AS (

  SELECT DISTINCT
    first_day_of_fiscal_quarter,
    fiscal_quarter_name_fy,
    days_in_fiscal_quarter_count
  FROM {{ ref('dim_date')}}

),

booked_arr AS (

  SELECT 
    close_fiscal_quarter_date,
    SUM(booked_net_arr)     AS total_booked_arr
  FROM live_actuals
  GROUP BY 1

),

created_arr AS (

  SELECT
    arr_created_fiscal_quarter_date,
    SUM(created_arr)        AS total_created_arr
  FROM live_actuals
  GROUP BY 1
), 

final AS (

  SELECT
    distinct_quarters.fiscal_quarter_name_fy,
    distinct_quarters.fiscal_quarter_name_fy AS fiscal_quarter_name,
    distinct_quarters.days_in_fiscal_quarter_count + 1 AS final_day_of_fiscal_quarter,   -- to put the live value on the day after the final day of the quarter, i.e. live value is on day 93 if there are 92 days in the quarter
    distinct_quarters.first_day_of_fiscal_quarter AS fiscal_quarter_date,
    created_arr.total_created_arr,
    booked_arr.total_booked_arr
  FROM distinct_quarters 
  INNER JOIN created_arr
    ON distinct_quarters.first_day_of_fiscal_quarter = created_arr.arr_created_fiscal_quarter_date
  INNER JOIN booked_arr
    ON distinct_quarters.first_day_of_fiscal_quarter = booked_arr.close_fiscal_quarter_date
)

SELECT * 
FROM final
