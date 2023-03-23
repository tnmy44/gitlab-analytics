{{ simple_cte([
    ('mart_atr_snapshot', 'mart_available_to_renew_snapshot_model'),
    ('dim_date', 'dim_date')
]) }},

atr_daily AS (

  SELECT
    dim_date.first_day_of_month,
    mart_atr_snapshot.parent_crm_account_name,
    mart_atr_snapshot.snapshot_date,
    mart_atr_snapshot.subscription_name,
    COALESCE(multi_year_booking_subscription_end_month, bookings_term_end_month) AS subscription_final_month,
    mart_atr_snapshot.is_available_to_renew,
    SUM(arr)::NUMBER                                                             AS total_arr,
    MAX(mart_atr_snapshot.fiscal_year)                                           AS max_year
  FROM mart_atr_snapshot
  INNER JOIN dim_date ON mart_atr_snapshot.snapshot_date = dim_date.date_day
  {{ dbt_utils.group_by(n=6) }}

),

atr_comparison AS (

  SELECT
    *,
    LAG(is_available_to_renew) OVER (PARTITION BY subscription_name ORDER BY snapshot_date) AS is_available_to_renew_previous_day,
    CASE WHEN is_available_to_renew = TRUE
      AND LAG(is_available_to_renew)
      OVER (PARTITION BY subscription_name ORDER BY snapshot_date) = FALSE
      THEN 'Became available to renew'
      WHEN is_available_to_renew = FALSE
        AND LAG(is_available_to_renew)
        OVER (PARTITION BY subscription_name ORDER BY snapshot_date) = TRUE
        THEN 'Dropped as available to renew'
      WHEN LAG(is_available_to_renew)
        OVER (PARTITION BY subscription_name ORDER BY snapshot_date) IS NULL
        THEN 'New subscription'
      WHEN subscription_final_month = first_day_of_month
        THEN 'Subscription ends this month'
      WHEN is_available_to_renew = false 
      AND dense_rank() OVER (PARTITION BY first_day_of_month ORDER BY total_arr DESC) <= 10 
        THEN 'Top available to renew excluded deal'
      ELSE 'No change'
    END                                                                                     AS flag
  FROM atr_daily
  WHERE max_year = DATE_PART(YEAR, CURRENT_DATE())

),

monthly_agg AS (

  SELECT DISTINCT
    first_day_of_month,
    parent_crm_account_name,
    subscription_name,
    total_arr,
    flag
  FROM atr_comparison

),

final AS (

  SELECT 
    first_day_of_month,
    parent_crm_account_name,
    subscription_name,
    total_arr,
    flag
  FROM monthly_agg
  WHERE flag != 'No change'
    AND first_day_of_month != '2021-12-01' -- exclude first month otherwise everyone is a new subscription
  QUALIFY RANK() OVER (
      PARTITION BY first_day_of_month, flag
      ORDER BY total_arr DESC) <= 10

)

SELECT *
FROM final
