{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('subscriptions', 'dim_subscription'),
    ('prep_charge_mrr_daily', 'prep_charge_mrr_daily'),
    ('dim_date', 'dim_date')
]) }},

 final AS (

SELECT DISTINCT
  date_actual,
  dim_subscription_id,
  dim_subscription_id_original,
  subscription_name,
  dim_namespace_id,
  dim_crm_account_id,
  subscription_version,
  dim_product_detail_id,
  charge_type,
  {{ dbt_utils.generate_surrogate_key
    (
      [
        'date_actual',
        'dim_namespace_id',
        'dim_subscription_id',
        'dim_product_detail_id'
      ]
    )
  }}                                        AS primary_key
FROM prep_charge_mrr_daily
-- picking most recent subscription version
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY date_actual, subscription_name, product_rate_plan_charge_name
    ORDER BY SUBSCRIPTION_VERSION DESC, RATE_PLAN_CHARGE_NUMBER DESC, RATE_PLAN_CHARGE_VERSION DESC, RATE_PLAN_CHARGE_SEGMENT DESC
  ) = 1
)

SELECT *
FROM final
WHERE dim_namespace_id IS NOT NULL
