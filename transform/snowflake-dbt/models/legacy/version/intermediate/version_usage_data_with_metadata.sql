{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}


{{ simple_cte([
    ('licenses', 'customers_db_licenses_source'),
    ('zuora_subscriptions', 'zuora_subscription'),
    ('zuora_accounts', 'zuora_account'),
    ('version_releases', 'version_releases')
]) }}

, usage_data AS (

    SELECT {{ dbt_utils.star(from=ref('version_usage_data'), except=["LICENSE_STARTS_AT", "LICENSE_EXPIRES_AT"]) }}
    FROM {{ ref('version_usage_data') }}
  
), usage_data_with_licenses AS (

    SELECT
      usage_data.*,
      COALESCE(sha256.license_id, md5.license_id)                                              AS license_id,
      COALESCE(sha256.zuora_subscription_id, md5.zuora_subscription_id)                        AS zuora_subscription_id,
      COALESCE(sha256.company, md5.company)                                                    AS company,
      COALESCE(sha256.plan_code, md5.plan_code)                                                AS license_plan_code,
      COALESCE(sha256.license_start_date, md5.license_start_date)                              AS license_starts_at,
      COALESCE(sha256.license_expire_date, md5.license_expire_date)                            AS license_expires_at,
      COALESCE(sha256.email, md5.email)                                                        AS email

    FROM usage_data
    LEFT JOIN licenses AS md5
      ON usage_data.license_md5 = md5.license_md5
    LEFT JOIN licenses AS sha256
      ON usage_data.license_sha256 = sha256.license_sha256
    WHERE
      (
        COALESCE(sha256.email, md5.email) IS NULL
        OR NOT (COALESCE(sha256.email, md5.email) LIKE '%@gitlab.com' AND LOWER(COALESCE(sha256.company, md5.company)) LIKE '%gitlab%') -- Exclude internal tests licenses.
        OR usage_data.uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'
      )

), joined AS (

    SELECT
      usage_data_with_licenses.*,
      zuora_subscriptions.subscription_status                                                  AS zuora_subscription_status,
      zuora_accounts.crm_id                                                                    AS zuora_crm_id,
      DATEDIFF('days', ping_version.release_date, usage_data_with_licenses.created_at)         AS days_after_version_release_date,
      latest_version.major_minor_version                                                       AS latest_version_available_at_ping_creation,
      latest_version.version_row_number - ping_version.version_row_number                      AS versions_behind_latest

    FROM usage_data_with_licenses
    LEFT JOIN zuora_subscriptions
      ON usage_data_with_licenses.zuora_subscription_id = zuora_subscriptions.subscription_id
    LEFT JOIN zuora_accounts
      ON zuora_subscriptions.account_id = zuora_accounts.account_id
    LEFT JOIN version_releases AS ping_version -- Join on the version of the ping itself.
      ON usage_data_with_licenses.major_minor_version = ping_version.major_minor_version
    LEFT JOIN version_releases AS latest_version -- Join the latest version released at the time of the ping.
      ON usage_data_with_licenses.created_at BETWEEN latest_version.release_date AND {{ coalesce_to_infinity('latest_version.next_version_release_date') }}

), renamed AS (

    SELECT
      joined.*,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'SaaS'
        ELSE 'Self-Managed'
      END                                                                  AS ping_source,
      CASE WHEN LOWER(edition) LIKE '%ee%' THEN 'EE'
        ELSE 'CE' END                                                      AS main_edition,
      CASE 
          WHEN edition LIKE '%CE%' THEN 'Core'
          WHEN edition LIKE '%EES%' THEN 'Starter'
          WHEN edition LIKE '%EEP%' THEN 'Premium'
          WHEN edition LIKE '%EEU%' THEN 'Ultimate'
          WHEN edition LIKE '%EE Free%' THEN 'Core'
          WHEN edition LIKE '%EE%' THEN 'Starter'
        ELSE NULL END                                                      AS edition_type,
      usage_activity_by_stage_monthly['manage']['events']                  AS monthly_active_users_last_28_days

    FROM joined

)

SELECT *
FROM renamed
