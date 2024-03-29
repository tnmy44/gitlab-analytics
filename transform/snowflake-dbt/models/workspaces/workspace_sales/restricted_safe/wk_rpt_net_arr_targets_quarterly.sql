{{ simple_cte([
      ('dim_crm_user_hierarchy', 'wk_dim_crm_user_hierarchy'),
      ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
      ('dim_order_type', 'dim_order_type'),
      ('daily_targets', 'wk_fct_sales_funnel_target_daily'),
      ('dim_date', 'dim_date')
])}},

date_spine AS (

     -- Filter the data down to only one snapshot every 7 days throughout each quarter.
  {{ date_spine_7th_day() }}

),

quarterly_targets AS ( 

    SELECT 
        dim_order_type_id,
        dim_sales_qualified_source_id,
        dim_crm_user_hierarchy_sk,
        fiscal_quarter_name,
        SUM(daily_allocated_target) AS total_quarter_target
    FROM daily_targets
    WHERE kpi_name = 'Net ARR'
    GROUP BY 1,2,3,4

)

SELECT 
    daily_targets.target_date,
    date_spine.day_of_fiscal_quarter,
    date_spine.last_day_of_fiscal_quarter,
    daily_targets.dim_order_type_id,
    daily_targets.dim_sales_qualified_source_id,
    daily_targets.dim_crm_user_hierarchy_sk,
    daily_targets.fiscal_quarter_name,
    dim_sales_qualified_source.sales_qualified_source_name,
    dim_order_type.order_type_name AS order_type,
    dim_order_type.order_type_grouped,
    dim_crm_user_hierarchy.crm_user_business_unit,
    dim_crm_user_hierarchy.crm_user_sales_segment,
    dim_crm_user_hierarchy.crm_user_geo,
    dim_crm_user_hierarchy.crm_user_region,
    dim_crm_user_hierarchy.crm_user_area,
    dim_crm_user_hierarchy.crm_user_role_name,
    dim_crm_user_hierarchy.crm_user_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5,
    dim_crm_user_hierarchy.crm_user_sales_segment_grouped,
    dim_crm_user_hierarchy.crm_user_sales_segment_region_grouped,
    quarterly_targets.total_quarter_target
FROM daily_targets
INNER JOIN date_spine
  ON date_spine.day_7 = daily_targets.target_date
LEFT JOIN dim_sales_qualified_source
  ON daily_targets.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
LEFT JOIN dim_order_type
  ON daily_targets.dim_order_type_id = dim_order_type.dim_order_type_id
LEFT JOIN dim_crm_user_hierarchy
  ON daily_targets.dim_crm_user_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
LEFT JOIN quarterly_targets 
  ON daily_targets.fiscal_quarter_name = quarterly_targets.fiscal_quarter_name
    AND daily_targets.dim_order_type_id = quarterly_targets.dim_order_type_id
      AND daily_targets.dim_sales_qualified_source_id = quarterly_targets.dim_sales_qualified_source_id
        AND daily_targets.dim_crm_user_hierarchy_sk = quarterly_targets.dim_crm_user_hierarchy_sk
WHERE kpi_name = 'Net ARR'
