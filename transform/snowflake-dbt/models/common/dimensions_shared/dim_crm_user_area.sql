{{ config(
    tags=["mnpi_exception"]
) }}

SELECT DISTINCT
  dim_crm_user_area_id,
  crm_user_area
FROM {{ ref('prep_crm_user_hierarchy') }}
