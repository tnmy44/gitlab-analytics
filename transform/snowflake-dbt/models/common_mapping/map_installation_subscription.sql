{{ config(
    tags=["product", "mnpi_exception"]
) }}

WITH prep_ping_instance AS (

  SELECT
    ping_created_at,
    dim_installation_id,
    dim_subscription_id,
    licence_md5,
    license_sha256
  FROM {{ ref('prep_ping_instance') }}

),

dim_date AS (

  SELECT
    *
  FROM {{ ref('dim_date') }}

),

prep_license AS (

  SELECT
    *
  FROM {{ ref('prep_license') }}

),

base AS (

  SELECT
    prep_ping_instance.ping_created_at,
    prep_ping_instance.dim_installation_id,
    COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id)                               AS dim_subscription_id,
    LEAD(prep_ping_instance.ping_created_at) OVER (PARTITION BY prep_ping_instance.dim_installation_id ORDER BY prep_ping_instance.ping_created_at ASC)  AS next_ping_date
  FROM prep_ping_instance
  LEFT JOIN prep_license AS md5
    ON md5.licence_md5 = prep_ping_instance.licence_md5
  LEFT JOIN prep_license AS sha256
    ON sha256.license_sha256 = prep_ping_instance.license_sha256
  WHERE COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id) IS NOT NULL
  ORDER BY 
    prep_ping_instance.dim_installation_id, 
    prep_ping_instance.ping_created_at, 
    COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id)

),

ping_daily_mapping AS (

  SELECT
    dim_date.date_actual,
    base.dim_installation_id,
    base.dim_subscription_id
  FROM base
  JOIN dim_date 
    ON base.ping_created_at <= dim_date.date_actual
    AND (base.next_ping_date > dim_date.date_actual
      OR base.next_ping_date IS NULL)
  ORDER BY
    base.dim_installation_id,
    dim_date.date_actual

)

SELECT * FROM ping_daily_mapping
