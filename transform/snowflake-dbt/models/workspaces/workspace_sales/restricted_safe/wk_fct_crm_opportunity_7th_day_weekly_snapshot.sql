WITH actuals AS (

  SELECT * 
  FROM {{ ref('wk_fct_crm_opportunity_daily_snapshot') }}

),

day_7_list AS (

  {{ date_spine_7th_day() }}

),


final AS (

  SELECT 
    actuals.*
  FROM actuals
  INNER JOIN day_7_list
    ON actuals.snapshot_date = day_7_list.day_7_current_week

)

SELECT * 
FROM final