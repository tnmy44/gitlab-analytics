{{ config(
    tags=["mnpi_exception"]
) }}

SELECT DISTINCT
  dim_crm_user_geo_id,
  crm_user_geo
FROM {{ ref('prep_crm_user_hierarchy') }}
