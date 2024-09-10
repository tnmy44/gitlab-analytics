{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('subscriptions', 'dim_subscription'),
    ('prep_charge_mrr_daily', 'prep_charge_mrr_daily'),
    ('dim_date', 'dim_date'),
    ('prep_product_detail', 'prep_product_detail')
]) }},

 final AS (

  SELECT DISTINCT
    prep_charge_mrr_daily.date_actual,
    prep_charge_mrr_daily.dim_subscription_id,
    subscriptions.dim_subscription_id_original,
    prep_charge_mrr_daily.subscription_name,
    subscriptions.namespace_id                  AS dim_namespace_id,
    prep_charge_mrr_daily.dim_crm_account_id,
    subscriptions.subscription_version,
    prep_charge_mrr_daily.dim_product_detail_id,
    prep_charge_mrr_daily.charge_type,
    {{ dbt_utils.generate_surrogate_key
      (
        [
          'prep_charge_mrr_daily.date_actual',
          'subscriptions.namespace_id',
          'prep_charge_mrr_daily.dim_subscription_id',
          'prep_charge_mrr_daily.dim_product_detail_id'
        ]
      )
    }}                                        AS primary_key
  FROM prep_charge_mrr_daily
  LEFT JOIN subscriptions
    ON prep_charge_mrr_daily.dim_subscription_id = subscriptions.dim_subscription_id
  LEFT JOIN prep_product_detail
    ON prep_charge_mrr_daily.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  -- picking most recent subscription version
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY prep_charge_mrr_daily.date_actual, prep_charge_mrr_daily.subscription_name, prep_product_detail.product_rate_plan_charge_name
      ORDER BY prep_charge_mrr_daily.subscription_version DESC, prep_charge_mrr_daily.rate_plan_charge_number DESC, prep_charge_mrr_daily.rate_plan_charge_version DESC, prep_charge_mrr_daily.rate_plan_charge_segment DESC
    ) = 1
)

SELECT *
FROM final
WHERE dim_namespace_id IS NOT NULL
