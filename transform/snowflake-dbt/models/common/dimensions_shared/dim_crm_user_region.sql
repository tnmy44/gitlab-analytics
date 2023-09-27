{{ config(
    tags=["mnpi_exception"]
) }}

SELECT DISTINCT
  dim_crm_user_region_id,
  crm_user_region
FROM {{ ref('prep_crm_user_hierarchy') }}
