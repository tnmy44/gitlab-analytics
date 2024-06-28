{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('fct_mrr_daily', 'fct_mrr_daily')
]) }}

, dim_date_weekly AS (

    SELECT DISTINCT
      first_day_of_week
    FROM dim_date
)

, mrr_weekly AS (

    SELECT
      fct_mrr_daily.mrr_id,
      fct_mrr_daily.dim_date_id,
      fct_mrr_daily.date_actual,
      fct_mrr_daily.dim_charge_id,
      fct_mrr_daily.dim_product_detail_id,
      fct_mrr_daily.dim_subscription_id,
      fct_mrr_daily.dim_billing_account_id,
      fct_mrr_daily.dim_crm_account_id,
      fct_mrr_daily.dim_order_id,
      fct_mrr_daily.subscription_status,
      fct_mrr_daily.unit_of_measure,
      fct_mrr_daily.is_jihu_account,
      fct_mrr_daily.mrr,
      fct_mrr_daily.arr,
      fct_mrr_daily.quantity
    FROM fct_mrr_daily
    INNER JOIN dim_date_weekly
      ON fct_mrr_daily.date_actual = dim_date_weekly.first_day_of_week

)

SELECT *
FROM mrr_weekly
