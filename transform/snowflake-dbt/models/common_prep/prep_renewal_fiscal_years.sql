{{ simple_cte([
    ('dim_date','dim_date'),
    ('dim_subscription', 'dim_subscription'),
    ('fct_charge', 'fct_charge')
]) }}

, renewal_fiscal_years AS (

    SELECT DISTINCT dim_date.fiscal_year
    FROM fct_charge
    INNER JOIN dim_subscription
      ON fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    INNER JOIN dim_date
      ON fct_charge.effective_start_month <= dim_date.date_actual
      AND (fct_charge.effective_end_month > dim_date.date_actual
        OR fct_charge.effective_end_month IS NULL)
      AND dim_date.day_of_month = 1
    WHERE subscription_status IN ('Active', 'Cancelled')
      AND charge_type = 'Recurring'
      AND mrr != 0
    ORDER BY 1 DESC
    LIMIT 11

)

SELECT *
FROM renewal_fiscal_years
