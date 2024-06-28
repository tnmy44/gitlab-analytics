{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_charge', 'prep_charge_mrr'),
    ('dim_crm_account', 'dim_crm_account')
]) }}

, prep_charge_filtered AS (

    SELECT 
      prep_charge.*,
      dim_crm_account.is_jihu_account
    FROM prep_charge
    LEFT JOIN dim_crm_account
      ON prep_charge.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    WHERE is_included_in_arr_calc = TRUE
      AND subscription_status IN ('Active', 'Cancelled')
      AND (mrr != 0 OR LOWER(rate_plan_charge_name) = 'max enrollment')

)

, mrr_daily AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['dim_date.date_id','prep_charge_filtered.dim_charge_id'])}}          AS mrr_id,
      dim_date.date_id                                                                                          AS dim_date_id,
      dim_date.date_actual,
      prep_charge_filtered.dim_charge_id,
      prep_charge_filtered.dim_product_detail_id,
      prep_charge_filtered.dim_subscription_id,
      prep_charge_filtered.dim_billing_account_id,
      prep_charge_filtered.dim_crm_account_id,
      prep_charge_filtered.dim_order_id,
      prep_charge_filtered.subscription_status,
      prep_charge_filtered.unit_of_measure,
      prep_charge_filtered.is_jihu_account,
      SUM(prep_charge_filtered.mrr)                                                                             AS mrr,
      SUM(prep_charge_filtered.arr)                                                                             AS arr,
      SUM(prep_charge_filtered.quantity)                                                                        AS quantity
    FROM prep_charge_filtered
    INNER JOIN dim_date
      ON prep_charge_filtered.effective_start_date <= dim_date.date_actual
      AND (prep_charge_filtered.effective_end_date > dim_date.date_actual
        OR prep_charge_filtered.effective_end_date IS NULL)
    {{ dbt_utils.group_by(n=12) }}

)

SELECT *
FROM mrr_daily
