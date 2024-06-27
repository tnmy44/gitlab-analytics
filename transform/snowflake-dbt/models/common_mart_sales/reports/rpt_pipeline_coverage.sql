{{ simple_cte([
    ('targets', 'mart_sales_funnel_target_daily'),
    ('actuals', 'mart_crm_opportunity_7th_day_weekly_snapshot'),
    ('dim_date', 'dim_date'),
    ('hierarchy', 'dim_crm_user_hierarchy')
    ])

}},

spine AS (

  {{ date_spine_7th_day() }}

),

total_targets AS (

  SELECT
    targets.fiscal_quarter_name,
    targets.fiscal_year,
    targets.dim_crm_user_hierarchy_sk,
    targets.order_type_name,
    targets.order_type_grouped,
    targets.sales_qualified_source_name,
    targets.sales_qualified_source_grouped,
    SUM(CASE WHEN kpi_name = 'Net ARR' THEN daily_allocated_target END)                  AS net_arr_total_quarter_target,
    SUM(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN daily_allocated_target END) AS pipeline_created_total_quarter_target
  FROM targets
  GROUP BY ALL

),

daily_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_date,
    actuals.snapshot_id,
    actuals.dim_crm_current_account_set_hierarchy_sk,
    actuals.sales_qualified_source_name,
    actuals.sales_qualified_source_grouped,
    actuals.order_type,
    actuals.order_type_grouped,
    SUM(booked_net_arr_in_snapshot_quarter)     AS booked_net_arr_in_snapshot_quarter,
    SUM(open_1plus_net_arr_in_snapshot_quarter) AS open_1plus_net_arr_in_snapshot_quarter,
    SUM(open_3plus_net_arr_in_snapshot_quarter) AS open_3plus_net_arr_in_snapshot_quarter,
    SUM(open_4plus_net_arr_in_snapshot_quarter) AS open_4plus_net_arr_in_snapshot_quarter

  FROM actuals
  GROUP BY ALL
),

quarterly_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.dim_crm_current_account_set_hierarchy_sk,
    actuals.sales_qualified_source_name,
    actuals.sales_qualified_source_grouped,
    actuals.order_type,
    actuals.order_type_grouped,
    SUM(actuals.booked_net_arr_in_snapshot_quarter) AS total_booked_net_arr
  FROM actuals
  WHERE snapshot_date = snapshot_last_day_of_fiscal_quarter
  GROUP BY ALL

),

combined_data AS (

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    snapshot_fiscal_quarter_name
  FROM daily_actuals

  UNION

  SELECT
    dim_crm_user_hierarchy_sk,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type_name,
    order_type_grouped,
    fiscal_quarter_name
  FROM total_targets

  UNION

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    snapshot_fiscal_quarter_name
  FROM quarterly_actuals

),

base AS (

  /*
    Cross join all dimensions (hierarchy, qualified source, order type) and
    the dates to create a comprehensive set of all possible combinations of these dimensions and dates.
    This exhaustive combination is essential for scenarios where we need to account for all possible configurations in our analysis,
    ensuring that no combination is overlooked.

    When we eventually join this set of combinations with the quarterly actuals,
    it ensures that even the newly introduced dimensions are accounted for.
  */

  SELECT
    combined_data.dim_crm_current_account_set_hierarchy_sk,
    combined_data.sales_qualified_source_name,
    combined_data.sales_qualified_source_grouped,
    combined_data.order_type,
    combined_data.order_type_grouped,
    spine.day_7 AS date_actual,
    combined_data.snapshot_fiscal_quarter_name
  FROM combined_data
  INNER JOIN spine
    ON combined_data.snapshot_fiscal_quarter_name = spine.fiscal_quarter_name

),

final AS (

  SELECT
    base.date_actual,
    base.dim_crm_current_account_set_hierarchy_sk,
    base.sales_qualified_source_name,
    base.sales_qualified_source_grouped,
    base.order_type,
    base.order_type_grouped,

    -- hierarchy fields

    hierarchy.crm_user_sales_segment                           AS crm_current_account_set_sales_segment,
    hierarchy.crm_user_geo                                     AS crm_current_account_set_geo,
    hierarchy.crm_user_region                                  AS crm_current_account_set_region,
    hierarchy.crm_user_area                                    AS crm_current_account_set_area,
    hierarchy.crm_user_business_unit                           AS crm_current_account_set_business_unit,
    hierarchy.crm_user_role_name                               AS crm_current_account_set_role_name,
    hierarchy.crm_user_role_level_1                            AS crm_current_account_set_role_level_1,
    hierarchy.crm_user_role_level_2                            AS crm_current_account_set_role_level_2,
    hierarchy.crm_user_role_level_3                            AS crm_current_account_set_role_level_3,
    hierarchy.crm_user_role_level_4                            AS crm_current_account_set_role_level_4,
    hierarchy.crm_user_role_level_5                            AS crm_current_account_set_role_level_5,

    --Dates
    dim_date.current_day_name,
    dim_date.current_date_actual,
    dim_date.current_fiscal_year,
    dim_date.current_first_day_of_fiscal_year,
    dim_date.current_fiscal_quarter_name_fy,
    dim_date.current_first_day_of_month,
    dim_date.current_first_day_of_fiscal_quarter,
    dim_date.current_day_of_month,
    dim_date.current_day_of_fiscal_quarter,
    dim_date.current_day_of_fiscal_year,
    dim_date.current_first_day_of_week,
    dim_date.current_week_of_fiscal_quarter_normalised,
    dim_date.current_week_of_fiscal_quarter,
    dim_date.snapshot_day,
    dim_date.snapshot_date,
    dim_date.snapshot_month,
    dim_date.snapshot_fiscal_year,
    dim_date.snapshot_fiscal_quarter_name,
    dim_date.snapshot_fiscal_quarter_date,
    dim_date.snapshot_day_of_fiscal_quarter_normalised,
    dim_date.snapshot_day_of_fiscal_year_normalised,
    dim_date.snapshot_day_name, 
    dim_date.snapshot_day_of_week,
    dim_date.snapshot_first_day_of_week,
    dim_date.snapshot_week_of_year,
    dim_date.snapshot_day_of_month,
    dim_date.snapshot_day_of_quarter,
    dim_date.snapshot_day_of_year,
    dim_date.snapshot_fiscal_quarter,
    dim_date.snapshot_day_of_fiscal_quarter,
    dim_date.snapshot_day_of_fiscal_year,
    dim_date.snapshot_month_name,
    dim_date.snapshot_first_day_of_month,
    dim_date.snapshot_last_day_of_month,
    dim_date.snapshot_first_day_of_year,
    dim_date.snapshot_last_day_of_year,
    dim_date.snapshot_first_day_of_quarter,
    dim_date.snapshot_last_day_of_quarter,
    dim_date.snapshot_first_day_of_fiscal_quarter,
    dim_date.snapshot_last_day_of_fiscal_quarter,
    dim_date.snapshot_first_day_of_fiscal_year,
    dim_date.snapshot_last_day_of_fiscal_year,
    dim_date.snapshot_week_of_fiscal_year,
    dim_date.snapshot_month_of_fiscal_year,
    dim_date.snapshot_last_day_of_week,
    dim_date.snapshot_quarter_name,
    dim_date.snapshot_fiscal_quarter_name_fy,
    dim_date.snapshot_fiscal_quarter_number_absolute,
    dim_date.snapshot_fiscal_month_name,
    dim_date.snapshot_fiscal_month_name_fy,
    dim_date.snapshot_holiday_desc,
    dim_date.snapshot_is_holiday,
    dim_date.snapshot_last_month_of_fiscal_quarter,
    dim_date.snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    dim_date.snapshot_last_month_of_fiscal_year,
    dim_date.snapshot_is_first_day_of_last_month_of_fiscal_year,
    dim_date.snapshot_days_in_month_count,
    dim_date.snapshot_week_of_month_normalised,
    dim_date.snapshot_week_of_fiscal_quarter_normalised,
    dim_date.snapshot_is_first_day_of_fiscal_quarter_week,
    dim_date.snapshot_days_until_last_day_of_month,
    dim_date.snapshot_week_of_fiscal_quarter,
    'targets_actuals'                                                       AS source,
    SUM(total_targets.pipeline_created_total_quarter_target)                AS pipeline_created_total_quarter_target,
    SUM(total_targets.net_arr_total_quarter_target)                         AS net_arr_total_quarter_target,
    SUM(daily_actuals.booked_net_arr_in_snapshot_quarter)                   AS coverage_booked_net_arr,
    SUM(daily_actuals.open_1plus_net_arr_in_snapshot_quarter)               AS coverage_open_1plus_net_arr,
    SUM(daily_actuals.open_3plus_net_arr_in_snapshot_quarter)               AS coverage_open_3plus_net_arr,
    SUM(daily_actuals.open_4plus_net_arr_in_snapshot_quarter)               AS coverage_open_4plus_net_arr,
    SUM(quarterly_actuals.total_booked_net_arr)                             AS total_booked_net_arr
  FROM base
  LEFT JOIN total_targets
    ON base.snapshot_fiscal_quarter_name = total_targets.fiscal_quarter_name
      AND base.sales_qualified_source_name = total_targets.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = total_targets.sales_qualified_source_grouped
      AND base.dim_crm_current_account_set_hierarchy_sk = total_targets.dim_crm_user_hierarchy_sk
      AND base.order_type = total_targets.order_type_name
      AND base.order_type_grouped = total_targets.order_type_grouped
  LEFT JOIN daily_actuals
    ON base.date_id = daily_actuals.snapshot_id
      AND base.sales_qualified_source_name = daily_actuals.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = daily_actuals.sales_qualified_source_grouped
      AND base.dim_crm_current_account_set_hierarchy_sk = daily_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.order_type = daily_actuals.order_type
      AND base.order_type_grouped = daily_actuals.order_type_grouped
  LEFT JOIN quarterly_actuals
    ON base.snapshot_fiscal_quarter_name = quarterly_actuals.snapshot_fiscal_quarter_name
      AND base.sales_qualified_source_name = quarterly_actuals.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = quarterly_actuals.sales_qualified_source_grouped
      AND base.dim_crm_current_account_set_hierarchy_sk = quarterly_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.order_type = quarterly_actuals.order_type
      AND base.order_type_grouped = quarterly_actuals.order_type_grouped
  LEFT JOIN hierarchy
    ON base.dim_crm_current_account_set_hierarchy_sk = hierarchy.dim_crm_user_hierarchy_sk 
  LEFT JOIN dim_date
    ON base.date_actual = dim_date.date_actual 
  GROUP BY ALL

)

SELECT *
FROM final