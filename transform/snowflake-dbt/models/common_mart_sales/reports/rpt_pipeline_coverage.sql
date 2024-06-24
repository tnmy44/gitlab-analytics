WITH targets AS (

  SELECT *
  FROM {{ ref('mart_sales_funnel_target_daily') }}

),

actuals AS (

  SELECT *
  FROM {{ ref('mart_crm_opportunity_7th_day_weekly_snapshot') }}

),

spine AS (

  {{ date_spine_7th_day() }}

),

total_targets AS (

  SELECT
    targets.fiscal_quarter_name,
    targets.fiscal_year,
    targets.dim_crm_user_hierarchy_sk                                                    AS dim_crm_current_account_set_hierarchy_sk,
    targets.crm_user_sales_segment,
    targets.crm_user_sales_segment_grouped,
    targets.crm_user_business_unit,
    targets.crm_user_geo,
    targets.crm_user_region,
    targets.crm_user_area,
    targets.crm_user_sales_segment_region_grouped,
    targets.order_type_name                                                              AS order_type,
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

    actuals.sales_qualified_source_name,
    actuals.sales_qualified_source_grouped,
    actuals.order_type,
    actuals.order_type_grouped,
    actuals.dim_crm_current_account_set_hierarchy_sk,
    actuals.crm_current_account_set_sales_segment,
    actuals.crm_current_account_set_geo,
    actuals.crm_current_account_set_region,
    actuals.crm_current_account_set_area,
    actuals.crm_current_account_set_business_unit,
    actuals.crm_current_account_set_role_name,
    actuals.crm_current_account_set_role_level_1,
    actuals.crm_current_account_set_role_level_2,
    actuals.crm_current_account_set_role_level_3,
    actuals.crm_current_account_set_role_level_4,
    actuals.crm_current_account_set_role_level_5,

    --Dates
    actuals.current_day_name,
    actuals.current_date_actual,
    actuals.current_fiscal_year,
    actuals.current_first_day_of_fiscal_year,
    actuals.current_fiscal_quarter_name_fy,
    actuals.current_first_day_of_month,
    actuals.current_first_day_of_fiscal_quarter,
    actuals.current_day_of_month,
    actuals.current_day_of_fiscal_quarter,
    actuals.current_day_of_fiscal_year,
    actuals.current_first_day_of_week,
    actuals.current_week_of_fiscal_quarter_normalised,
    actuals.snapshot_id,
    actuals.snapshot_day,
    actuals.snapshot_date,
    actuals.snapshot_month,
    actuals.snapshot_fiscal_year,
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.snapshot_day_of_fiscal_quarter_normalised,
    actuals.snapshot_day_of_fiscal_year_normalised,
    actuals.snapshot_day_name, 
    actuals.snapshot_day_of_week,
    actuals.snapshot_first_day_of_week,
    actuals.snapshot_week_of_year,
    actuals.snapshot_day_of_month,
    actuals.snapshot_day_of_quarter,
    actuals.snapshot_day_of_year,
    actuals.snapshot_fiscal_quarter,
    actuals.snapshot_day_of_fiscal_quarter,
    actuals.snapshot_day_of_fiscal_year,
    actuals.snapshot_month_name,
    actuals.snapshot_first_day_of_month,
    actuals.snapshot_last_day_of_month,
    actuals.snapshot_first_day_of_year,
    actuals.snapshot_last_day_of_year,
    actuals.snapshot_first_day_of_quarter,
    actuals.snapshot_last_day_of_quarter,
    actuals.snapshot_first_day_of_fiscal_quarter,
    actuals.snapshot_last_day_of_fiscal_quarter,
    actuals.snapshot_first_day_of_fiscal_year,
    actuals.snapshot_last_day_of_fiscal_year,
    actuals.snapshot_week_of_fiscal_year,
    actuals.snapshot_month_of_fiscal_year,
    actuals.snapshot_last_day_of_week,
    actuals.snapshot_quarter_name,
    actuals.snapshot_fiscal_quarter_name_fy,
    actuals.snapshot_fiscal_quarter_number_absolute,
    actuals.snapshot_fiscal_month_name,
    actuals.snapshot_fiscal_month_name_fy,
    actuals.snapshot_holiday_desc,
    actuals.snapshot_is_holiday,
    actuals.snapshot_last_month_of_fiscal_quarter,
    actuals.snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    actuals.snapshot_last_month_of_fiscal_year,
    actuals.snapshot_is_first_day_of_last_month_of_fiscal_year,
    actuals.snapshot_days_in_month_count,
    actuals.snapshot_week_of_month_normalised,
    actuals.snapshot_week_of_fiscal_quarter_normalised,
    actuals.snapshot_is_first_day_of_fiscal_quarter_week,
    actuals.snapshot_days_until_last_day_of_month,
    actuals.snapshot_week_of_fiscal_quarter,
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
    actuals.snapshot_fiscal_quarter_date,
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

  UNION

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    sales_qualified_source_name,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    snapshot_fiscal_quarter_name
  FROM daily_actuals

),

base AS (

  /*
    Cross join all dimensions (hierarchy, qualified source, order type) and
    the dates to create a comprehensive set of all possible combinations of these dimensions and dates.
    This is essential for scenarios where we need to account for all possible configurations in our analysis.

    When we eventually join this set of combinations with the quarterly actuals,
    it ensures that even the newly introduced dimensions are accounted for.
  */

  SELECT
    spine.date_id,
    spine.day_7 AS date_actual,
    combined_data.dim_crm_current_account_set_hierarchy_sk,
    combined_data.sales_qualified_source_name,
    combined_data.sales_qualified_source_grouped,
    combined_data.order_type,
    combined_data.order_type_grouped,
    combined_data.fiscal_quarter_name
  FROM combined_data
  INNER JOIN spine
    ON combined_data.fiscal_quarter_name = spine.fiscal_quarter_name

),

final AS (

  SELECT
    base.date_id,
    base.date_actual,
    base.dim_crm_current_account_set_hierarchy_sk,
    base.sales_qualified_source_name,
    base.sales_qualified_source_grouped,
    base.order_type,
    base.order_type_grouped,

    -- hierarchy fields
    daily_actuals.crm_current_account_set_sales_segment,
    daily_actuals.crm_current_account_set_geo,
    daily_actuals.crm_current_account_set_region,
    daily_actuals.crm_current_account_set_area,
    daily_actuals.crm_current_account_set_business_unit,
    daily_actuals.crm_current_account_set_role_name,
    daily_actuals.crm_current_account_set_role_level_1,
    daily_actuals.crm_current_account_set_role_level_2,
    daily_actuals.crm_current_account_set_role_level_3,
    daily_actuals.crm_current_account_set_role_level_4,
    daily_actuals.crm_current_account_set_role_level_5,

    --Dates
    daily_actuals.current_day_name,
    daily_actuals.current_date_actual,
    daily_actuals.current_fiscal_year,
    daily_actuals.current_first_day_of_fiscal_year,
    daily_actuals.current_fiscal_quarter_name_fy,
    daily_actuals.current_first_day_of_month,
    daily_actuals.current_first_day_of_fiscal_quarter,
    daily_actuals.current_day_of_month,
    daily_actuals.current_day_of_fiscal_quarter,
    daily_actuals.current_day_of_fiscal_year,
    daily_actuals.current_first_day_of_week,
    daily_actuals.current_week_of_fiscal_quarter_normalised,
    daily_actuals.snapshot_day,
    daily_actuals.snapshot_date,
    daily_actuals.snapshot_month,
    daily_actuals.snapshot_fiscal_year,
    daily_actuals.snapshot_fiscal_quarter_name,
    daily_actuals.snapshot_fiscal_quarter_date,
    daily_actuals.snapshot_day_of_fiscal_quarter_normalised,
    daily_actuals.snapshot_day_of_fiscal_year_normalised,
    daily_actuals.snapshot_day_name, 
    daily_actuals.snapshot_day_of_week,
    daily_actuals.snapshot_first_day_of_week,
    daily_actuals.snapshot_week_of_year,
    daily_actuals.snapshot_day_of_month,
    daily_actuals.snapshot_day_of_quarter,
    daily_actuals.snapshot_day_of_year,
    daily_actuals.snapshot_fiscal_quarter,
    daily_actuals.snapshot_day_of_fiscal_quarter,
    daily_actuals.snapshot_day_of_fiscal_year,
    daily_actuals.snapshot_month_name,
    daily_actuals.snapshot_first_day_of_month,
    daily_actuals.snapshot_last_day_of_month,
    daily_actuals.snapshot_first_day_of_year,
    daily_actuals.snapshot_last_day_of_year,
    daily_actuals.snapshot_first_day_of_quarter,
    daily_actuals.snapshot_last_day_of_quarter,
    daily_actuals.snapshot_first_day_of_fiscal_quarter,
    daily_actuals.snapshot_last_day_of_fiscal_quarter,
    daily_actuals.snapshot_first_day_of_fiscal_year,
    daily_actuals.snapshot_last_day_of_fiscal_year,
    daily_actuals.snapshot_week_of_fiscal_year,
    daily_actuals.snapshot_month_of_fiscal_year,
    daily_actuals.snapshot_last_day_of_week,
    daily_actuals.snapshot_quarter_name,
    daily_actuals.snapshot_fiscal_quarter_name_fy,
    daily_actuals.snapshot_fiscal_quarter_number_absolute,
    daily_actuals.snapshot_fiscal_month_name,
    daily_actuals.snapshot_fiscal_month_name_fy,
    daily_actuals.snapshot_holiday_desc,
    daily_actuals.snapshot_is_holiday,
    daily_actuals.snapshot_last_month_of_fiscal_quarter,
    daily_actuals.snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    daily_actuals.snapshot_last_month_of_fiscal_year,
    daily_actuals.snapshot_is_first_day_of_last_month_of_fiscal_year,
    daily_actuals.snapshot_days_in_month_count,
    daily_actuals.snapshot_week_of_month_normalised,
    daily_actuals.snapshot_week_of_fiscal_quarter_normalised,
    daily_actuals.snapshot_is_first_day_of_fiscal_quarter_week,
    daily_actuals.snapshot_days_until_last_day_of_month,
    daily_actuals.snapshot_week_of_fiscal_quarter,
    SUM(total_targets.pipeline_created_total_quarter_target)                AS pipeline_created_total_quarter_target,
    SUM(total_targets.net_arr_total_quarter_target)                         AS net_arr_total_quarter_target,
    SUM(daily_actuals.booked_net_arr_in_snapshot_quarter)                   AS coverage_booked_net_arr,
    SUM(daily_actuals.open_1plus_net_arr_in_snapshot_quarter)               AS coverage_open_1plus_net_arr,
    SUM(daily_actuals.open_3plus_net_arr_in_snapshot_quarter)               AS coverage_open_3plus_net_arr,
    SUM(daily_actuals.open_4plus_net_arr_in_snapshot_quarter)               AS coverage_open_4plus_net_arr,
    SUM(quarterly_actuals.total_booked_net_arr)                             AS total_booked_net_arr
  FROM base
  LEFT JOIN total_targets
    ON base.fiscal_quarter_name = total_targets.fiscal_quarter_name
      AND base.sales_qualified_source_name = total_targets.sales_qualified_source_name
      AND base.dim_crm_current_account_set_hierarchy_sk = total_targets.dim_crm_current_account_set_hierarchy_sk
      AND base.order_type = total_targets.order_type
  LEFT JOIN daily_actuals
    ON base.date_id = daily_actuals.snapshot_id
      AND base.sales_qualified_source_name = daily_actuals.sales_qualified_source_name
      AND base.dim_crm_current_account_set_hierarchy_sk = daily_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.order_type = daily_actuals.order_type
  LEFT JOIN quarterly_actuals
    ON base.fiscal_quarter_name = quarterly_actuals.snapshot_fiscal_quarter_name
      AND base.sales_qualified_source_name = quarterly_actuals.sales_qualified_source_name
      AND base.dim_crm_current_account_set_hierarchy_sk = quarterly_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.order_type = quarterly_actuals.order_type
  GROUP BY ALL

)

SELECT *
FROM final
