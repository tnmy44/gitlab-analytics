{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('prep_charge', 'prep_charge')
])}}

, charges_filtered AS (
/*
Filter to the most recent subscription version for the original subscription/subscription name
*/

  SELECT 
    {{ dbt_utils.star(
           from=ref('prep_charge'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
  FROM prep_charge
  QUALIFY
    DENSE_RANK() OVER (
    PARTITION BY subscription_name
    ORDER BY subscription_version DESC
  ) = 1

), prep_charge_mrr_daily AS (

/*
Expand the most recent subscription version by the effective dates of the charges
This represents the actual effective dates of the products, as updated with each subscription version and carried through as
a history in the charges.

The dim_subscription_id associated with these charges will be the most recent dim_subscription_id in the subscription_name/dim_subscription_id_original lineage.
To find the dim_subscription_id that was active at the time of the charges instead of the most recent version, join this model to the subscription object based on the subscription_name
or dim_subscription_id_original between the subscription created date (adjusted for the first version of a subscription due to backdated effective dates in subscriptions) and the created date of the next subscription version.

Example code:
LEFT JOIN prep_subscription AS subscriptions_charge
    ON prep_charge_mrr_daily.subscription_name = subscriptions_charge.subscription_name
      AND prep_charge_mrr_daily.date_actual BETWEEN subscriptions_charge.subscription_created_datetime_adjusted AND subscriptions_charge.next_subscription_created_datetime
*/

  SELECT
    prep_date.date_actual,
    charges_filtered.*
  FROM charges_filtered
  INNER JOIN prep_date
    ON charges_filtered.effective_start_date <= prep_date.date_actual
    AND COALESCE(charges_filtered.effective_end_date, CURRENT_DATE) > prep_date.date_actual
  WHERE charge_type != 'OneTime'
    AND subscription_status NOT IN ('Draft')
    AND is_included_in_arr_calc = TRUE

)

{{ dbt_audit(
    cte_ref="prep_charge_mrr_daily",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-09-12",
    updated_date="2024-09-12",
) }}