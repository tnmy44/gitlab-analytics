{{ simple_cte([
    ('live_actuals', 'mart_crm_opportunity_stamped_hierarchy_hist'),
    ('dim_date', 'dim_date'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('dim_order_type','dim_order_type')
    ])

}},

base AS (

  SELECT DISTINCT
    current_day_name,
    current_date_actual,
    current_fiscal_year,
    current_first_day_of_fiscal_year,
    current_fiscal_quarter_name_fy,
    current_first_day_of_month,
    current_first_day_of_fiscal_quarter,
    current_day_of_month,
    current_day_of_fiscal_quarter,
    current_day_of_fiscal_year,
    current_first_day_of_week,
    current_week_of_fiscal_quarter_normalised,
    current_week_of_fiscal_quarter,
    fiscal_quarter_name_fy                       AS snapshot_fiscal_quarter_name,
    fiscal_quarter_name_fy                       AS snapshot_fiscal_quarter_name_fy,
    fiscal_quarter                               AS snapshot_fiscal_quarter,
    fiscal_year                                  AS snapshot_fiscal_year,
    first_day_of_fiscal_quarter                  AS snapshot_first_day_of_fiscal_quarter,
    last_day_of_fiscal_quarter                   AS snapshot_last_day_of_fiscal_quarter,
    first_day_of_fiscal_year                     AS snapshot_first_day_of_fiscal_year,
    last_day_of_fiscal_year                      AS snapshot_last_day_of_fiscal_year,
    first_day_of_fiscal_quarter                  AS snapshot_fiscal_quarter_date,
    fiscal_quarter_number_absolute               AS snapshot_fiscal_quarter_number_absolute,
    last_month_of_fiscal_quarter                 AS snapshot_last_month_of_fiscal_quarter,
    last_month_of_fiscal_year                    AS snapshot_last_month_of_fiscal_year,
    days_in_fiscal_quarter_count                 AS snapshot_days_in_fiscal_quarter_count,
    dim_sales_qualified_source.sales_qualified_source_name,
    dim_sales_qualified_source.sales_qualified_source_grouped,
    dim_order_type.order_type_name,
    dim_order_type.order_type_grouped
  FROM dim_date
  JOIN dim_sales_qualified_source
  JOIN dim_order_type

),

booked_arr AS (

  SELECT 
    close_fiscal_quarter_date,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    SUM(booked_net_arr)     AS total_booked_arr
  FROM live_actuals
  GROUP BY 1,2,3,4,5

),

created_arr AS (

  SELECT
    arr_created_fiscal_quarter_date,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    SUM(created_arr)        AS total_created_arr
  FROM live_actuals
  GROUP BY 1,2,3,4,5
), 

final AS (

  SELECT
    base.current_day_name,
    base.current_date_actual,
    base.current_fiscal_year,
    base.current_first_day_of_fiscal_year,
    base.current_fiscal_quarter_name_fy,
    base.current_first_day_of_month,
    base.current_first_day_of_fiscal_quarter,
    base.current_day_of_month,
    base.current_day_of_fiscal_quarter,
    base.current_day_of_fiscal_year,
    base.current_first_day_of_week,
    base.current_week_of_fiscal_quarter_normalised,
    base.current_week_of_fiscal_quarter,
    base.snapshot_fiscal_quarter,
    base.snapshot_fiscal_year,
    base.snapshot_first_day_of_fiscal_year,
    base.snapshot_last_day_of_fiscal_year,
    base.snapshot_fiscal_quarter_name,
    base.snapshot_fiscal_quarter_name_fy,
    base.snapshot_fiscal_quarter_date,
    base.snapshot_fiscal_quarter_number_absolute,
    base.snapshot_last_month_of_fiscal_quarter,
    base.snapshot_last_month_of_fiscal_year,
    base.snapshot_days_in_fiscal_quarter_count AS final_day_of_fiscal_quarter, 
    base.sales_qualified_source_name,
    base.sales_qualified_source_grouped,
    base.order_type_name,
    base.order_type_grouped,
    booked_arr.total_booked_arr,
    created_arr.total_created_arr
  FROM base 
  LEFT JOIN created_arr
    ON base.snapshot_fiscal_quarter_date = created_arr.arr_created_fiscal_quarter_date
      AND base.sales_qualified_source_name = created_arr.sales_qualified_source_name
        AND base.sales_qualified_source_grouped = created_arr.sales_qualified_source_grouped 
          AND base.order_type_name = created_arr.order_type
            AND base.order_type_grouped = created_arr.order_type_grouped
  LEFT JOIN booked_arr
    ON base.snapshot_fiscal_quarter_date = booked_arr.close_fiscal_quarter_date
      AND base.sales_qualified_source_name = booked_arr.sales_qualified_source_name
        AND base.sales_qualified_source_grouped = booked_arr.sales_qualified_source_grouped 
          AND base.order_type_name = booked_arr.order_type
            AND base.order_type_grouped = booked_arr.order_type_grouped
)

SELECT * 
FROM final
