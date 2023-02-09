{{ simple_cte([
      ('dim_date', 'dim_date'),
      ('sheetload_sales_funnel_targets_matrix_source', 'sheetload_sales_funnel_targets_matrix_source')
])}}

, fiscal_months AS (

    SELECT DISTINCT
      fiscal_month_name_fy,
      fiscal_year,
      first_day_of_month
    FROM dim_date

), final AS (

    SELECT
      sheetload_sales_funnel_targets_matrix_source.kpi_name,
      sheetload_sales_funnel_targets_matrix_source.month,
      sheetload_sales_funnel_targets_matrix_source.opportunity_source,
      sheetload_sales_funnel_targets_matrix_source.order_type,
      sheetload_sales_funnel_targets_matrix_source.area,
      sheetload_sales_funnel_targets_matrix_source.allocated_target,
      sheetload_sales_funnel_targets_matrix_source.user_segment,
      sheetload_sales_funnel_targets_matrix_source.user_geo,
      sheetload_sales_funnel_targets_matrix_source.user_region,
      sheetload_sales_funnel_targets_matrix_source.user_area,
      sheetload_sales_funnel_targets_matrix_source.user_business_unit,
      CASE
        WHEN fiscal_months.fiscal_year < 2024
          THEN CONCAT(sheetload_sales_funnel_targets_matrix_source.user_segment, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_area
                      )
        WHEN fiscal_months.fiscal_year >= 2024 AND LOWER(sheetload_sales_funnel_targets_matrix_source.user_business_unit) = 'comm'
          THEN CONCAT(sheetload_sales_funnel_targets_matrix_source.user_business_unit, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_segment, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_area
                      )
        WHEN fiscal_months.fiscal_year >= 2024 AND LOWER(sheetload_sales_funnel_targets_matrix_source.user_business_unit) = 'entg'
          THEN CONCAT(sheetload_sales_funnel_targets_matrix_source.user_business_unit, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_geo, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_region, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_area, 
                      '-',
                      sheetload_sales_funnel_targets_matrix_source.user_segment
                      )
        END                                                                                                                           AS dim_crm_user_hierarchy_sk,
        fiscal_months.fiscal_year,
        fiscal_months.first_day_of_month
    FROM sheetload_sales_funnel_targets_matrix_source
    INNER JOIN fiscal_months
      ON sheetload_sales_funnel_targets_matrix_source.month = fiscal_months.fiscal_month_name_fy

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-02-07",
    updated_date="2023-02-07"
) }}