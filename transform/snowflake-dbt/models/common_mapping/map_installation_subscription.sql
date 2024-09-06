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

WITH subscriptions AS (

  SELECT 
    dim_subscription.*,
    IFF(dim_subscription.subscription_version = 1, LEAST(zuora_subscription_source.created_date, dim_subscription.subscription_start_date), zuora_subscription_source.created_date) AS subscription_created_datetime,
    COALESCE(
      LEAD(subscription_created_datetime) OVER (PARTITION BY dim_subscription.subscription_name ORDER BY dim_subscription.subscription_version),
      CURRENT_DATE() 
    )                                                                                                                                           AS next_subscription_created_datetime
  FROM prep_subscription

), base AS (

  SELECT
    prep_ping_instance.ping_created_at,
    prep_ping_instance.dim_installation_id,
    COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id)        AS dim_subscription_id,
    LEAD(ping_created_at) OVER (
      PARTITION BY dim_installation_id ORDER BY ping_created_at ASC
      )                                                                  AS next_ping_date
  FROM prep_ping_instance
  LEFT JOIN prep_license AS md5
    ON prep_ping_instance.license_md5 = md5.license_md5
  LEFT JOIN prep_license AS sha256
    ON prep_ping_instance.license_sha256 = sha256.license_sha256
  WHERE COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id) IS NOT NULL
  
), ping_daily_mapping AS (

  SELECT 
    prep_date.date_actual,
    base.dim_installation_id,
    base.dim_subscription_id
  FROM base
  INNER JOIN prep_date
    ON base.ping_created_at <= prep_date.date_actual
      AND (COALESCE(base.next_ping_date, CURRENT_DATE()) > prep_date.date_actual)

), seat_link AS (

  SELECT 
    prep_usage_self_managed_seat_link.*,
    LEAD(report_date) OVER (PARTITION BY dim_installation_id ORDER BY report_date ASC)  AS next_report_date
  FROM prep_usage_self_managed_seat_link

), seat_link_records AS (

  SELECT
    prep_date.date_actual,
    seat_link.dim_installation_id,
    seat_link.dim_subscription_id
  FROM seat_link
  INNER JOIN prep_date
    ON seat_link.report_date <= prep_date.date_actual
    AND (seat_link.next_report_date > prep_date.date_actual
      OR seat_link.next_report_date IS NULL)

), joined AS (

  SELECT 
    ping_daily_mapping.date_actual,
    ping_daily_mapping.dim_installation_id,
    COALESCE(seat_link_records.dim_subscription_id, ping_daily_mapping.dim_subscription_id) AS dim_subscription_id,
    subscriptions.dim_subscription_id_original,
    subscriptions.subscription_name
  FROM ping_daily_mapping
  LEFT OUTER JOIN seat_link_records
    ON ping_daily_mapping.dim_installation_id = seat_link_records.dim_installation_id
      AND ping_daily_mapping.date_actual = seat_link_records.date_actual
  LEFT JOIN subscriptions
    ON COALESCE(seat_link_records.dim_subscription_id, ping_daily_mapping.dim_subscription_id) = subscriptions.dim_subscription_id

), charges_filtered AS (
/*
Filter to the most recent subscription version for the original subscription/subscription name
*/

  SELECT *
  FROM prep_charge
  QUALIFY
    DENSE_RANK() OVER (
    PARTITION BY subscription_name
    ORDER BY SUBSCRIPTION_VERSION DESC
  ) = 1

), prep_charge AS (

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

SELECT DISTINCT
  prep_charge.date_actual,
  COALESCE(joined.dim_subscription_id, subscriptions.dim_subscription_id) AS dim_subscription_id,
  COALESCE(dim_subscription.dim_subscription_id_original, subscriptions.dim_subscription_id_original) AS dim_subscription_id_original,
  prep_charge.subscription_name,
  joined.dim_installation_id,
  subscriptions_ping.dim_crm_account_id,
  COALESCE(subscriptions_ping.subscription_version, subscriptions.subscription_version) AS subscription_version,
  dim_product_detail.product_rate_plan_charge_name,
  prep_charge.charge_type,
  prep_charge.date_actual || '-' || COALESCE(joined.dim_installation_id, 'missing installation_id') || '-' || COALESCE(joined.dim_subscription_id, subscriptions.dim_subscription_id) || dim_product_detail.product_rate_plan_charge_name AS primary_key
FROM prep_charge
LEFT JOIN joined
  ON prep_charge.subscription_name = joined.subscription_name
    AND prep_charge.date_actual = joined.date_actual
LEFT JOIN subscriptions AS subscriptions_ping
  ON joined.dim_subscription_id = subscriptions_ping.dim_subscription_id
LEFT JOIN subscriptions AS subscriptions_charge
  ON prep_charge.subscription_name = subscriptions_charge.subscription_name
    AND prep_charge.date_actual BETWEEN subscriptions_charge.subscription_created_datetime AND subscriptions_charge.next_subscription_created_datetime
LEFT JOIN prep_product_detail
  ON prep_charge.dim_product_detail_id = prep_product_detail.dim_product_detail_id
LEFT JOIN prep_product_tier
  ON prep_product_detail.dim_product_tier_id = dim_product_tier.dim_product_tier_id
WHERE prep_product_detail.product_deployment_type IN ('Self-Managed', 'Dedicated')
  AND prep_product_tier.product_ranking > 0
  AND prep_product_detail.product_rate_plan_charge_name NOT IN (
    '1,000 CI Minutes',
    'Gitlab Storage 10GB - 1 Year',
    'Premium Support',
    '1,000 Compute Minutes',
    'GitLab Geo',
    'Deploy Boards',
    'Service Desk',
    'Audit User',
    'Pivotal Cloud Foundry',
    'File Locking',
    '24x7 US Citizen Support - 1 Year',
    'Dedicated - Administration Fee [Small] - Monthly',
    'Dedicated - Storage 10GB - Monthly',
    'Dedicated - Administration Fee [Medium] - 1 Year',
    'Dedicated - Administration Fee [3XLarge] - 1 Year',
    'Dedicated - Administration Fee [Small] - 1 Year',
    'Dedicated - Administration Fee [2XLarge] - 1 Year',
    '12x5 US Citizen Support - Monthly',
    'Dedicated - Administration Fee  [XLarge] - 1 Year',
    'Dedicated - Storage 10GB - 1 Year',
    'Dedicated - Administration Fee [Large] - 1 Year',
    '12x5 US Citizen Support - 1 Year'
  )
  
