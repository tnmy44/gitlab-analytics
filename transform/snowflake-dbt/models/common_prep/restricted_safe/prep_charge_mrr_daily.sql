{{ simple_cte([
    ('prep_subscription', 'prep_subscription'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('prep_license', 'prep_license'),
    ('prep_date', 'prep_date'),
    ('prep_usage_self_managed_seat_link', 'prep_usage_self_managed_seat_link'),
    ('prep_charge', 'prep_charge'),
    ('prep_product_detail', 'prep_product_detail'),
    ('prep_product_tier', 'prep_product_tier')
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
    ORDER BY SUBSCRIPTION_VERSION DESC
  ) = 1

), prep_charge_mrr_daily AS (

/*
Expand the most recent subscription version by the effective dates of the charges
This represents the actual effective dates of the products, as updated with each subscription version and carried through as
a history in the charges
*/

  SELECT
    prep_date.date_actual,
    charges_filtered.*
  FROM charges_filtered
  INNER JOIN prep_date
    ON charges_filtered.effective_start_date <= prep_date.date_actual
    AND COALESCE(charges_filtered.effective_end_date,CURRENT_DATE) > prep_date.date_actual
  WHERE charge_type != 'OneTime'
    AND subscription_status NOT IN ('Draft')
    AND is_included_in_arr_calc = TRUE

)

{{ dbt_audit(
    cte_ref="prep_charge_mrr_daily",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-09-09",
    updated_date="2024-09-09",
) }}