{{ config(
    tags=["mnpi_exception"]
) }}


WITH source AS (


  SELECT 
    dim_crm_user_hierarchy_id,
    dim_crm_user_hierarchy_sk,
    fiscal_year,
    crm_user_business_unit,
    dim_crm_user_business_unit_id,
    crm_user_sales_segment,
    dim_crm_user_sales_segment_id,
    crm_user_geo,
    dim_crm_user_geo_id,
    crm_user_region,
    dim_crm_user_region_id,
    crm_user_area,
    dim_crm_user_area_id,
    crm_user_sales_segment_grouped,
    crm_user_sales_segment_region_grouped,
    is_current_crm_user_hierarchy
  FROM {{ ref('prep_crm_user_hierarchy') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@mcooperDD",
    updated_by="@michellecooper",
    created_date="2021-01-05",
    updated_date="2023-03-10"
) }}
