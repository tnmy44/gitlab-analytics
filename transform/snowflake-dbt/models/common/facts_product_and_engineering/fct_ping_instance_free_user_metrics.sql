{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "materialized": "incremental",
    "unique_key": "dim_ping_instance_id"
})}}

{{ simple_cte([
    ('instance_pings', 'dim_ping_instance'),
    ('map_license_account', 'map_license_subscription_account'),
    ('instance_types', 'dim_host_instance_type')
    ])

}},

/* Filtering data from tables separately on license_md5 & license_sha256 to avoid OR condition due to query performance issues */
   map_license_account_md5 AS (

    SELECT
      license_md5,
      dim_subscription_id,
      dim_crm_account_id,
      dim_parent_crm_account_id
    FROM map_license_account
    WHERE license_md5 IS NOT NULL

), map_license_account_sha256 AS (

    SELECT
      license_sha256,
      dim_subscription_id,
      dim_crm_account_id,
      dim_parent_crm_account_id
    FROM map_license_account
    WHERE license_sha256 IS NOT NULL

), core_instance_pings AS (

    SELECT
      core_instance_pings.dim_ping_instance_id,
      --core_instance_pings.dim_product_tier_id,
      core_instance_pings.ping_created_at,
      core_instance_pings.ping_created_date_28_days_earlier,
      core_instance_pings.ping_created_date_year,
      core_instance_pings.ping_created_date_month,
      core_instance_pings.ping_created_date_week,
      core_instance_pings.ping_created_date,
      core_instance_pings.raw_usage_data_id,
      core_instance_pings.raw_usage_data_payload,
      core_instance_pings.license_md5,
      core_instance_pings.license_sha256,
      --core_instance_pings.original_edition,
      --core_instance_pings.edition,
     -- core_instance_pings.main_edition,
      core_instance_pings.ping_edition,
      core_instance_pings.product_tier,
      --core_instance_pings.main_edition_product_tier,
      core_instance_pings.cleaned_version,
      core_instance_pings.version_is_prerelease,
      core_instance_pings.major_version,
      core_instance_pings.minor_version,
      core_instance_pings.major_minor_version,
      core_instance_pings.ping_source,
      core_instance_pings.is_internal,
      core_instance_pings.is_staging
      --core_instance_pings.dim_location_country_id,
      COALESCE(map_license_account_md5.dim_subscription_id, map_license_account_sha256.dim_subscription_id)             AS dim_subscription_id,
      COALESCE(map_license_account_md5.dim_crm_account_id, map_license_account_sha256.dim_crm_account_id)               AS dim_crm_account_id,
      COALESCE(map_license_account_md5.dim_parent_crm_account_id, map_license_account_sha256.dim_parent_crm_account_id) AS dim_parent_crm_account_id
    FROM instance_pings
    LEFT JOIN map_license_account_md5
    ON instance_pings.license_md5 = map_license_account_md5.license_md5
    LEFT JOIN map_license_account_sha256
    ON instance_pings.license_sha256 = map_license_account_sha256.license_sha256
    WHERE instance_pings.product_tier = 'Core'

), joined AS (

    SELECT

    {{ default_usage_ping_information() }}

    instance_types.instance_type,
    core_instance_pings.dim_subscription_id,
    core_instance_pings.dim_crm_account_id,
    core_ucore_instance_pingssage_pings.dim_parent_crm_account_id,
    core_instance_pings.dim_location_country_id,

    {{ sales_wave_2_3_metrics() }}

    FROM core_instance_pings
    LEFT JOIN instance_types
      ON core_instance_pings.raw_usage_data_payload['uuid']::VARCHAR = instance_types.instance_uuid
      AND core_instance_pings.raw_usage_data_payload['hostname']::VARCHAR = instance_types.instance_hostname
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY dim_ping_instance_id
        ORDER BY ping_created_date DESC
      ) = 1
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-01-20",
    updated_date="2023-01-20"
) }}
