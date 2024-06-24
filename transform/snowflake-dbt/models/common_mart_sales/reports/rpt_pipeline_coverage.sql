WITH targets AS (

  SELECT *
  FROM {{ ref('mart_sales_funnel_target_daily') }}

),

actuals AS (

  SELECT *
  FROM {{ ref('mart_crm_opportunity_7th_day_weekly_snapshot') }}

),

total_targets AS (

  SELECT
    targets.primary_key,
    targets.target_date,
    targets.report_target_date,
    targets.first_day_of_week,
    targets.first_day_of_month            AS target_month,
    targets.fiscal_quarter_name,
    targets.fiscal_year,
    targets.kpi_name,
    targets.targets_sk,
    targets.crm_user_sales_segment,
    targets.crm_user_sales_segment_grouped,
    targets.crm_user_business_unit,
    targets.crm_user_geo,
    targets.crm_user_region,
    targets.crm_user_area,
    targets.crm_user_sales_segment_region_grouped,
    targets.order_type_name,
    targets.order_type_grouped,
    targets.sales_qualified_source_name,
    targets.sales_qualified_source_grouped,
    SUM(CASE WHEN kpi_name = 'Net ARR' THEN daily_allocated_target END)                  AS net_arr_total_quarter_target,
    SUM(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN daily_allocated_target END) AS pipeline_created_total_quarter_target
  FROM targets
  LEFT JOIN dim_date
    ON targets.target_date_id = actuals.date_id
  {{ dbt_utils.group_by(n=5) }}

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
    actuals.current_week_of_fiscal_quarter,
    actuals.snapshot_day,
    actuals.snapshot_date,
    actuals.snapshot_month,
    actuals.snapshot_fiscal_year,
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.snapshot_day_of_fiscal_quarter_normalised,
    actuals.snapshot_day_of_fiscal_year_normalised,
    actuals.snapshot_day,
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
  {{ dbt_utils.group_by(n=7) }}
),

quarterly_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.dim_crm_current_account_set_hierarchy_sk,
    actuals.sales_qualified_source,
    actuals.sales_qualified_source_grouped,
    actuals.order_type,
    actuals.order_type_grouped,
    SUM(actuals.booked_net_arr_in_snapshot_quarter) AS total_booked_net_arr
  FROM actuals
  WHERE snapshot_date = snapshot_last_day_of_fiscal_quarter
  {{ dbt_utils.group_by(n=5) }}

),

combined_data AS (

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    targets_id,
    targets_id,
    fiscal_quarter_name,
    fiscal_quarter_date
  FROM total_targets

  UNION

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    targets_id,
    targets_id,
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date
  FROM quarterly_actuals

  UNION

  SELECT
    dim_crm_current_account_set_hierarchy_sk,
    targets_id,
    targets_id,
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date
  FROM daily_actuals

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
    combined_data.targets_id,
    combined_data.targets_id,
    spine.date_id,
    spine.day_7 AS date_actual,
    combined_data.fiscal_quarter_date,
    combined_data.fiscal_quarter_name
  FROM combined_data
  INNER JOIN spine
    ON combined_data.fiscal_quarter_name = spine.fiscal_quarter_name

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['base.date_id', 'base.dim_crm_current_account_set_hierarchy_sk', 'base.targets_id','base.targets_id']) }} AS targets_actuals_weekly_snapshot_pk,
    base.date_id,
    base.date_actual,
    base.fiscal_quarter_name,
    base.fiscal_quarter_date,
    base.dim_crm_current_account_set_hierarchy_sk,
    base.targets_id,
    base.targets_id,
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
      AND base.targets_id = total_targets.targets_id
      AND base.dim_crm_current_account_set_hierarchy_sk = total_targets.dim_crm_current_account_set_hierarchy_sk
      AND base.targets_id = total_targets.targets_id
  LEFT JOIN daily_actuals
    ON base.date_id = daily_actuals.snapshot_id
      AND base.targets_id = daily_actuals.targets_id
      AND base.dim_crm_current_account_set_hierarchy_sk = daily_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.targets_id = daily_actuals.targets_id
  LEFT JOIN quarterly_actuals
    ON base.fiscal_quarter_name = quarterly_actuals.snapshot_fiscal_quarter_name
      AND base.targets_id = quarterly_actuals.targets_id
      AND base.dim_crm_current_account_set_hierarchy_sk = quarterly_actuals.dim_crm_current_account_set_hierarchy_sk
      AND base.targets_id = quarterly_actuals.targets_id
  {{ dbt_utils.group_by(n=8) }}

)

SELECT *
FROM final
