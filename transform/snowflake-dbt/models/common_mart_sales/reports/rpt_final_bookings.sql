WITH date_spine AS (
  SELECT 
    fiscal_quarter_name_fy,
    first_day_of_fiscal_quarter,
    MAX(day_of_fiscal_quarter) AS days_in_fiscal_quarter
  FROM
    prod.common.dim_date
  WHERE 
    first_day_of_fiscal_quarter > '2021-02-01' 
    AND current_date() > last_day_of_fiscal_quarter  -- to only pull in closed quarters for the last few FYs for this illustrative example
  GROUP BY 
    1, 2
  ORDER BY 
    1 ASC
),

live_booked_arr AS (
  SELECT 
    close_fiscal_quarter_date,
    SUM(booked_net_arr) AS total_booked_arr_live
  FROM 
    prod.restricted_safe_common_prep.prep_crm_opportunity
  WHERE 
    is_live = 1
  GROUP BY 
    1
),

live_created_arr AS (
  SELECT 
    arr_created_fiscal_quarter_date,
    SUM(created_arr) AS total_created_arr_live
  FROM 
    prod.restricted_safe_common_prep.prep_crm_opportunity
  WHERE 
    is_live = 1
    AND is_net_arr_pipeline_created = true
  GROUP BY 
    1
  ORDER BY 
    1 DESC
)

SELECT
  d.fiscal_quarter_name_fy,
  d.days_in_fiscal_quarter + 1 AS faux_snapshot_day_of_quarter,  -- to put the live value on the day after the final day of the quarter, i.e. live value is on day 93 if there are 92 days in the quarter
  first_day_of_fiscal_quarter,
  total_created_arr_live,
  total_booked_arr_live
FROM 
  date_spine d
  INNER JOIN live_created_arr c ON d.first_day_of_fiscal_quarter = c.arr_created_fiscal_quarter_date
  INNER JOIN live_booked_arr b ON d.first_day_of_fiscal_quarter = b.close_fiscal_quarter_date;
