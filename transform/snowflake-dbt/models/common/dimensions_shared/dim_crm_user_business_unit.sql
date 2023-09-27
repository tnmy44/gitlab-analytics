SELECT DISTINCT
  dim_crm_user_business_unit_id,
  crm_user_business_unit
FROM {{ ref('dim_crm_user_hierarchy') }}
