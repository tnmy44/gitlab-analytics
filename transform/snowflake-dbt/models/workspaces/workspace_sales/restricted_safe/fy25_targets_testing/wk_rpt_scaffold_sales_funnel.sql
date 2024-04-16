{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('fct_sales_funnel_target_daily', 'wk_fct_sales_funnel_target_daily'),
    ('fct_sales_funnel_actual', 'wk_fct_sales_funnel_actual'),
    ('dim_sales_funnel_kpi', 'wk_dim_sales_funnel_kpi'),
    ('dim_crm_user_hierarchy', 'wk_dim_crm_user_hierarchy')
]) }}

, geo_fields AS (

    SELECT DISTINCT
      actual_date_id, 
      dim_sales_funnel_kpi_sk,
      dim_hierarchy_sk, 
      dim_order_type_id,
      dim_sales_qualified_source_id,
      geo_name, 
      region_name, 
      area_name
    FROM fct_sales_funnel_actual
    LEFT JOIN dim_sales_funnel_kpi
      ON fct_sales_funnel_actual.sales_funnel_kpi_name = dim_sales_funnel_kpi.sales_funnel_kpi_name

    UNION

    SELECT DISTINCT
      fct_sales_funnel_target_daily.target_date_id, 
      fct_sales_funnel_target_daily.dim_sales_funnel_kpi_sk,
      fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk, 
      fct_sales_funnel_target_daily.dim_order_type_id,
      fct_sales_funnel_target_daily.dim_sales_qualified_source_id,
      fct_sales_funnel_target_daily.geo_name, 
      fct_sales_funnel_target_daily.region_name, 
      fct_sales_funnel_target_daily.area_name
    FROM fct_sales_funnel_target_daily
    LEFT JOIN dim_sales_funnel_kpi
      ON fct_sales_funnel_target_daily.kpi_name = dim_sales_funnel_kpi.sales_funnel_kpi_name
)

, unioned AS (

    SELECT
    fct_sales_funnel_actual.actual_date_id                  AS date_id,
    dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk,
    fct_sales_funnel_actual.dim_hierarchy_sk,
    fct_sales_funnel_actual.dim_order_type_id,
    fct_sales_funnel_actual.dim_sales_qualified_source_id
    FROM fct_sales_funnel_actual
    LEFT JOIN dim_sales_funnel_kpi
      ON dim_sales_funnel_kpi.sales_funnel_kpi_name = fct_sales_funnel_actual.sales_funnel_kpi_name

    UNION

    SELECT 
    fct_sales_funnel_target_daily.target_date_id,
    dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk,
    fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk,
    fct_sales_funnel_target_daily.dim_order_type_id,
    fct_sales_funnel_target_daily.dim_sales_qualified_source_id
    FROM fct_sales_funnel_target_daily
    LEFT JOIN dim_sales_funnel_kpi
      ON dim_sales_funnel_kpi.sales_funnel_kpi_name = fct_sales_funnel_target_daily.kpi_name

)

, final AS (

    SELECT 
      unioned.date_id,
      unioned.dim_sales_funnel_kpi_sk,
      unioned.dim_hierarchy_sk,
      unioned.dim_order_type_id,
      unioned.dim_sales_qualified_source_id,
      geo_fields.geo_name, 
      geo_fields.region_name, 
      geo_fields.area_name
    FROM unioned
    LEFT JOIN geo_fields
      ON unioned.date_id = geo_fields.actual_date_id 
      AND unioned.dim_sales_funnel_kpi_sk = geo_fields.dim_sales_funnel_kpi_sk
      AND unioned.dim_hierarchy_sk = geo_fields.dim_hierarchy_sk
      AND unioned.dim_order_type_id = geo_fields.dim_order_type_id
      AND unioned.dim_sales_qualified_source_id = geo_fields.dim_sales_qualified_source_id
)

SELECT *
FROM final
