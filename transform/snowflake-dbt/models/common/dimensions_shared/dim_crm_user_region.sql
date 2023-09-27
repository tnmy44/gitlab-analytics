SELECT DISTINCT
  dim_crm_user_region_id,
  crm_user_region
FROM {{ ref('dim_crm_user_hierarchy') }}
