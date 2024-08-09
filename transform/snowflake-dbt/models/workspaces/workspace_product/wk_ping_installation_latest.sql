{{ config(
    materialized='table',
    tags=["product"]
) }}

WITH first_ping AS (

  SELECT
    dim_installation_id,
    MIN(ping_created_at) AS first_ping_created_at
  FROM {{ ref('mart_ping_instance')}}
  GROUP BY 1

), latest_ping AS (

  SELECT
    dim_installation_id,
    dim_crm_account_id,
    latest_subscription_id,
    ping_created_at           AS latest_ping_created_at,
    major_minor_version       AS latest_major_minor_version,
    major_minor_version_num   AS latest_major_minor_version_num,
    ping_edition              AS latest_ping_edition,
    ping_product_tier         AS latest_product_tier,
    ping_edition_product_tier AS latest_ping_edition_product_tier,
    is_paid_subscription,
    is_internal               AS is_internal_installation
  FROM {{ ref('mart_ping_instance')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_installation_id ORDER BY ping_created_at DESC) = 1 --most recent record per installation

)

SELECT
  first_ping.*,
  latest_ping.* EXCLUDE dim_installation_id
FROM first_ping
INNER JOIN latest_ping
  ON first_ping.dim_installation_id = latest_ping.dim_installation_id
