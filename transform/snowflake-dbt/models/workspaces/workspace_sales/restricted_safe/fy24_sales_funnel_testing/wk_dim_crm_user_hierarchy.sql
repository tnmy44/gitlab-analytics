WITH source AS (


  SELECT 
    dim_crm_user_hierarchy_stamped_id,
    dim_crm_user_hierarchy_sk,
    fiscal_year,
    crm_opp_owner_business_unit_stamped,
    dim_crm_opp_owner_business_unit_stamped_id,
    crm_opp_owner_sales_segment_stamped,
    dim_crm_opp_owner_sales_segment_stamped_id,
    crm_opp_owner_geo_stamped,
    dim_crm_opp_owner_geo_stamped_id,
    crm_opp_owner_region_stamped,
    dim_crm_opp_owner_region_stamped_id,
    crm_opp_owner_area_stamped,
    dim_crm_opp_owner_area_stamped_id,
    crm_opp_owner_sales_segment_stamped_grouped,
    crm_opp_owner_sales_segment_region_stamped_grouped
  FROM {{ ref('wk_prep_crm_user_hierarchy') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-01-20",
    updated_date="2023-01-20"
) }}