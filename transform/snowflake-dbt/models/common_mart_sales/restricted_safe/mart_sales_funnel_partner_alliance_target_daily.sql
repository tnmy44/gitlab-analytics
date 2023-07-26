{{ simple_cte([
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy'),
    ('dim_order_type','dim_order_type'),
    ('fct_sales_funnel_target', 'fct_sales_funnel_partner_alliance_target'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('dim_alliance_type', 'dim_alliance_type_scd'),
    ('dim_channel_type', 'dim_channel_type'),
    ('dim_date','dim_date'),
    ('dim_partner_category', 'dim_partner_category')
]) }}

, monthly_targets AS (

    SELECT
      fct_sales_funnel_target.sales_funnel_partner_alliance_target_id,
      fct_sales_funnel_target.first_day_of_month      AS target_month,
      fct_sales_funnel_target.kpi_name,
      dim_crm_user_hierarchy.crm_user_sales_segment,
      dim_crm_user_hierarchy.crm_user_sales_segment_grouped,
      dim_crm_user_hierarchy.crm_user_business_unit,
      dim_crm_user_hierarchy.crm_user_geo,
      dim_crm_user_hierarchy.crm_user_region,
      dim_crm_user_hierarchy.crm_user_area,
      dim_crm_user_hierarchy.crm_user_sales_segment_region_grouped,
      dim_order_type.order_type_name,
      dim_order_type.order_type_grouped,
      dim_sales_qualified_source.sales_qualified_source_name,
      dim_sales_qualified_source.sqs_bucket_engagement,
      dim_channel_type.channel_type_name,
      dim_alliance_type.alliance_type_name,
      dim_alliance_type.alliance_type_short_name,
      dim_partner_category.partner_category_name,
      fct_sales_funnel_target.allocated_target
    FROM fct_sales_funnel_target
    LEFT JOIN dim_alliance_type
      ON fct_sales_funnel_target.dim_alliance_type_id = dim_alliance_type.dim_alliance_type_id
    LEFT JOIN dim_sales_qualified_source
      ON fct_sales_funnel_target.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
    LEFT JOIN dim_channel_type
      ON fct_sales_funnel_target.dim_channel_type_id = dim_channel_type.dim_channel_type_id
    LEFT JOIN dim_order_type
      ON fct_sales_funnel_target.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_user_hierarchy
      ON fct_sales_funnel_target.dim_crm_user_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
    LEFT JOIN dim_partner_category
      ON fct_sales_funnel_target.dim_partner_category_id = dim_partner_category.dim_partner_category_id

), monthly_targets_daily AS (

    SELECT
      date_day,
      monthly_targets.*,
      DATEDIFF('day', first_day_of_month, last_day_of_month) + 1  AS days_of_month,
      first_day_of_week,
      fiscal_quarter_name,
      fiscal_year,
      allocated_target / days_of_month                            AS daily_allocated_target
    FROM monthly_targets
    INNER JOIN dim_date
      ON monthly_targets.target_month = dim_date.first_day_of_month

), qtd_mtd_target AS (

    SELECT
      {{ dbt_utils.surrogate_key(['date_day', 'kpi_name', 'crm_user_sales_segment', 'crm_user_geo', 'crm_user_region',
        'crm_user_area', 'order_type_name', 'alliance_type_name', 'sales_qualified_source_name', 'crm_user_business_unit', 'partner_category_name']) }}    AS primary_key,
      date_day                                                                                                                    AS target_date,
      DATEADD('day', 1, target_date)                                                                                              AS report_target_date,
      first_day_of_week,
      target_month,
      fiscal_quarter_name,
      fiscal_year,
      kpi_name,
      crm_user_sales_segment,
      crm_user_sales_segment_grouped,
      crm_user_business_unit,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      crm_user_sales_segment_region_grouped,
      order_type_name,
      order_type_grouped,
      alliance_type_name,
      alliance_type_short_name,
      partner_category_name,
      sales_qualified_source_name,
      sqs_bucket_engagement,
      channel_type_name,
      allocated_target                                                                                                            AS monthly_allocated_target,
      daily_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, crm_user_sales_segment, crm_user_geo, crm_user_region, crm_user_business_unit,
                             crm_user_area, order_type_name, alliance_type_name, sales_qualified_source_name, first_day_of_week ORDER BY date_day)
                                                                                                                                  AS wtd_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, crm_user_sales_segment, crm_user_geo, crm_user_region, crm_user_business_unit,
                             crm_user_area, order_type_name, alliance_type_name, sales_qualified_source_name, target_month ORDER BY date_day)
                                                                                                                                  AS mtd_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, crm_user_sales_segment, crm_user_geo, crm_user_region, crm_user_business_unit,
                             crm_user_area, order_type_name, alliance_type_name, sales_qualified_source_name, fiscal_quarter_name ORDER BY date_day)
                                                                                                                                  AS qtd_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, crm_user_sales_segment, crm_user_geo, crm_user_region, crm_user_business_unit,
                             crm_user_area, order_type_name, alliance_type_name, sales_qualified_source_name, fiscal_year ORDER BY date_day)
                                                                                                                                  AS ytd_allocated_target

    FROM monthly_targets_daily

)

{{ dbt_audit(
    cte_ref="qtd_mtd_target",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-04-08",
    updated_date="2023-07-12",
  ) }}
