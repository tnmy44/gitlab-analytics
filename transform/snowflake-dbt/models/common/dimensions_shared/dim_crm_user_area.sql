SELECT DISTINCT
  dim_crm_user_area_id,
  crm_user_area
FROM {{ ref('dim_crm_user_hierarchy') }}
