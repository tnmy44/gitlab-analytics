{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('fct_sales_funnel_target_daily', 'fct_sales_funnel_target_daily'),
    ('fct_sales_funnel_actual', 'fct_sales_funnel_actual'),
    ('dim_sales_funnel_kpi', 'dim_sales_funnel_kpi')
]) }}

, final AS (

    SELECT
    fct_sales_funnel_actual.actual_date_id                                              AS date_id,
    dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk,
    fct_sales_funnel_actual.dim_hierarchy_sk,
    fct_sales_funnel_actual.dim_order_type_id,
    fct_sales_funnel_actual.dim_sales_qualified_source_id,
    COALESCE(fct_sales_funnel_actual.geo_name, 'Missing geo_name')                      AS geo_name,
    COALESCE(fct_sales_funnel_actual.region_name, 'Missing region_name')                AS region_name,
    COALESCE(fct_sales_funnel_actual.area_name, 'Missing area_name')                    AS area_name,
    COALESCE(fct_sales_funnel_actual.sales_segment_name, 'Missing sales_segment_name')  AS sales_segment_name,
    COALESCE(fct_sales_funnel_actual.business_unit_name, 'Missing business_unit_name')  AS business_unit_name,
    FROM fct_sales_funnel_actual
    LEFT JOIN dim_sales_funnel_kpi
      ON dim_sales_funnel_kpi.sales_funnel_kpi_name = fct_sales_funnel_actual.sales_funnel_kpi_name

    UNION

    SELECT 
    fct_sales_funnel_target_daily.target_date_id,
    dim_sales_funnel_kpi.dim_sales_funnel_kpi_sk,
    fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk,
    fct_sales_funnel_target_daily.dim_order_type_id,
    fct_sales_funnel_target_daily.dim_sales_qualified_source_id,
    COALESCE(fct_sales_funnel_target_daily.geo_name, 'Missing geo_name')                      AS geo_name,
    COALESCE(fct_sales_funnel_target_daily.region_name, 'Missing region_name')                AS region_name,
    COALESCE(fct_sales_funnel_target_daily.area_name, 'Missing area_name')                    AS area_name,
    COALESCE(fct_sales_funnel_target_daily.sales_segment_name, 'Missing sales_segment_name')  AS sales_segment_name,
    COALESCE(fct_sales_funnel_target_daily.business_unit_name, 'Missing business_unit_name')  AS business_unit_name,
    FROM fct_sales_funnel_target_daily
    LEFT JOIN dim_sales_funnel_kpi
      ON dim_sales_funnel_kpi.sales_funnel_kpi_name = fct_sales_funnel_target_daily.kpi_name

)

SELECT *
FROM final
