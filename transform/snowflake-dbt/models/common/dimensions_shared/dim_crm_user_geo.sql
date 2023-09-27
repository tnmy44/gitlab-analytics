SELECT DISTINCT
  dim_crm_user_geo_id,
  crm_user_geo
FROM {{ ref('dim_crm_user_hierarchy') }}
