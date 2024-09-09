{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('prep_subscription', 'prep_subscription'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('prep_license', 'prep_license'),
    ('prep_date', 'prep_date'),
    ('prep_usage_self_managed_seat_link', 'prep_usage_self_managed_seat_link'),
    ('prep_charge_mrr_daily', 'prep_charge_mrr_daily'),
    ('prep_product_detail', 'prep_product_detail')
])}}

, subscriptions AS (

  SELECT 
    prep_subscription.*,
    IFF(
      prep_subscription.subscription_version = 1, 
      LEAST(prep_subscription.subscription_created_date, prep_subscription.subscription_start_date),
      prep_subscription.subscription_created_date
      )                                                                                               AS subscription_created_datetime,
    COALESCE(
      LEAD(subscription_created_datetime)
        OVER (
          PARTITION BY prep_subscription.subscription_name
          ORDER BY prep_subscription.subscription_version
          ),
      GREATEST(
        CURRENT_DATE(),
        prep_subscription.subscription_end_date
        ) 
      )                                                                                              AS next_subscription_created_datetime
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

), final AS (

  SELECT DISTINCT
    prep_charge_mrr_daily.date_actual,
    COALESCE(subscriptions_ping.dim_subscription_id, subscriptions_charge.dim_subscription_id)                    AS dim_subscription_id,
    COALESCE(subscriptions_ping.dim_subscription_id_original, subscriptions_charge.dim_subscription_id_original)  AS dim_subscription_id_original,
    prep_charge_mrr_daily.subscription_name,
    joined.dim_installation_id,
    subscriptions_ping.dim_crm_account_id,
    COALESCE(subscriptions_ping.subscription_version, subscriptions_charge.subscription_version)                  AS subscription_version,
    prep_charge_mrr_daily.dim_product_detail_id,
    prep_charge_mrr_daily.charge_type,
    {{ dbt_utils.generate_surrogate_key([
        'prep_charge_mrr_daily.date_actual',
        'joined.dim_installation_id',
        'COALESCE(subscriptions_ping.dim_subscription_id, subscriptions_charge.dim_subscription_id)',
        'prep_charge_mrr_daily.dim_product_detail_id'
      ]) 
    }}                                                                                                            AS primary_key
  FROM prep_charge_mrr_daily
  LEFT JOIN joined
    ON prep_charge_mrr_daily.subscription_name = joined.subscription_name
      AND prep_charge_mrr_daily.date_actual = joined.date_actual
  LEFT JOIN subscriptions AS subscriptions_ping
    ON joined.dim_subscription_id = subscriptions_ping.dim_subscription_id
  LEFT JOIN subscriptions AS subscriptions_charge
    ON prep_charge_mrr_daily.subscription_name = subscriptions_charge.subscription_name
      AND prep_charge_mrr_daily.date_actual BETWEEN subscriptions_charge.subscription_created_datetime AND subscriptions_charge.next_subscription_created_datetime
  LEFT JOIN prep_product_detail
    ON prep_charge_mrr_daily.dim_product_detail_id = prep_product_detail.dim_product_detail_id
  WHERE prep_product_detail.product_deployment_type IN ('Self-Managed', 'Dedicated')

)

SELECT *
FROM final
WHERE dim_installation_id IS NOT NULL