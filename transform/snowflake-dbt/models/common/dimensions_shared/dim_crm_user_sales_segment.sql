{{ config(
    tags=["mnpi_exception"]
) }}

SELECT DISTINCT
  dim_crm_user_sales_segment_id,
  crm_user_sales_segment
FROM {{ ref('prep_crm_user_hierarchy') }}
