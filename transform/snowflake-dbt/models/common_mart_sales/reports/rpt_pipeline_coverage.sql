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
    spine.date_id,
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
    actuals.current_week_of_fiscal_quarter,
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
    'targets_actuals'                                                       AS source,
    SUM(total_targets.pipeline_created_total_quarter_target)                AS pipeline_created_total_quarter_target,
    SUM(total_targets.net_arr_total_quarter_target)                         AS net_arr_total_quarter_target,
    SUM(daily_actuals.booked_net_arr_in_snapshot_quarter)                   AS coverage_booked_net_arr,
    SUM(daily_actuals.open_1plus_net_arr_in_snapshot_quarter)               AS coverage_open_1plus_net_arr,
    SUM(daily_actuals.open_3plus_net_arr_in_snapshot_quarter)               AS coverage_open_3plus_net_arr,
    SUM(daily_actuals.open_4plus_net_arr_in_snapshot_quarter)               AS coverage_open_4plus_net_arr,
    SUM(quarterly_actuals.total_booked_net_arr)                             AS total_booked_net_arr
  FROM base
  LEFT JOIN actuals
    ON actuals.snapshot_date = base.date_actual
      AND base.sales_qualified_source_name = actuals.sales_qualified_source_name
      AND base.sales_qualified_source_grouped = actuals.sales_qualified_source_grouped
      AND base.dim_crm_current_account_set_hierarchy_sk = actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.order_type = actuals.order_type 
      AND base.order_type_grouped = actuals.order_type_grouped 
  LEFT JOIN targets
    ON targets.target_date = base.date_actual
      AND base.sales_qualified_source_grouped = targets.sales_qualified_source_grouped
      AND base.dim_crm_current_account_set_hierarchy_sk = targets.dim_crm_user_hierarchy_sk
      AND base.order_type = targets.order_type_name
      AND base.order_type_grouped = targets.order_type_grouped 
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
  GROUP BY ALL

)

SELECT *
FROM final
