WITH targets AS (

  SELECT *
  FROM {{ ref('wk_fct_sales_funnel_target_daily') }}

),

actuals AS (

  SELECT *
  FROM {{ ref('wk_fct_crm_opportunity_daily_snapshot') }}

),

dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}


),

spine AS (

  {{ date_spine_7th_day() }}

),

base AS (


/*
    The purpose of cross-joining all dimension tables (hierarchy, qualified source, order type) and
    the date_ids table is to create a comprehensive set of all possible combinations of these dimensions and dates.
    This exhaustive combination is essential for scenarios where we need to account for all possible configurations in our analysis,
    ensuring that no combination is overlooked.

    When we eventually join this comprehensive set of combinations with the quarterly actuals,
    it ensures that even the newly introduced dimensions are accounted for.
  */

  SELECT
    hierarchy.dim_crm_user_hierarchy_sk,
    sqs.dim_sales_qualified_source_id,
    order_type.dim_order_type_id,
    spine.date_id,
    spine.date_actual,
    spine.fiscal_quarter_name_fy AS fiscal_quarter_name,
    spine.first_day_of_fiscal_quarter AS fiscal_quarter_date
  FROM
    (
      SELECT DISTINCT dim_crm_user_hierarchy_sk FROM targets
      UNION
      SELECT DISTINCT dim_crm_current_account_set_hierarchy_sk AS dim_crm_user_hierarchy_sk FROM actuals
    ) AS hierarchy
  CROSS JOIN
    (
      SELECT DISTINCT dim_sales_qualified_source_id FROM targets
      UNION
      SELECT DISTINCT dim_sales_qualified_source_id FROM actuals
    ) AS sqs
  CROSS JOIN
    (
      SELECT DISTINCT dim_order_type_id FROM targets
      UNION
      SELECT DISTINCT dim_order_type_id FROM actuals
    ) AS order_type
  CROSS JOIN
    (
      SELECT DISTINCT target_date_id AS date_id FROM targets
      UNION
      SELECT DISTINCT snapshot_id AS date_id FROM actuals
    ) AS dates
  INNER JOIN spine
    ON dates.date_id = spine.date_id
),

total_targets AS (

  SELECT
    dim_date.fiscal_quarter_name_fy                           AS fiscal_quarter_name,
    dim_date.first_day_of_fiscal_quarter                      AS fiscal_quarter_date,
    targets.dim_crm_user_hierarchy_sk,
    targets.dim_sales_qualified_source_id,
    targets.dim_order_type_id,
    SUM(daily_allocated_target)                               AS total_quarter_target
  FROM targets
  LEFT JOIN dim_date
    ON targets.target_date_id = dim_date.date_id
  WHERE kpi_name = 'Net ARR'
  {{ dbt_utils.group_by(n=5) }}
),


daily_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.snapshot_date,
    actuals.snapshot_id,
    actuals.dim_crm_current_account_set_hierarchy_sk          AS dim_crm_user_hierarchy_sk,
    actuals.dim_sales_qualified_source_id,
    actuals.dim_order_type_id,
    
    -- attributes
    actuals.order_type,
    actuals.order_type_live,
    actuals.order_type_grouped,
    actuals.sales_qualified_source_name,
    actuals.sales_qualified_source_grouped,
    SUM(booked_net_arr_in_snapshot_quarter)                   AS booked_net_arr_in_snapshot_quarter,
  SUM(open_1plus_net_arr_in_snapshot_quarter)                 AS open_1plus_net_arr_in_snapshot_quarter
  FROM actuals
  {{ dbt_utils.group_by(n=12) }}
),

quarterly_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.dim_crm_current_account_set_hierarchy_sk          AS dim_crm_user_hierarchy_sk,
    actuals.dim_sales_qualified_source_id,
    actuals.dim_order_type_id,

    -- attributes
    actuals.order_type,
    actuals.order_type_live,
    actuals.order_type_grouped,
    actuals.sales_qualified_source_name,
    actuals.sales_qualified_source_grouped,
    SUM(actuals.booked_net_arr_in_snapshot_quarter)           AS total_booked_net_arr
  FROM actuals
  WHERE snapshot_date = snapshot_last_day_of_fiscal_quarter
  {{ dbt_utils.group_by(n=10) }}

),

final AS (

  SELECT
    base.date_id,
    base.date_actual,
    base.fiscal_quarter_name,
    base.fiscal_quarter_date,
    base.dim_crm_user_hierarchy_sk,
    base.dim_order_type_id,
    base.dim_sales_qualified_source_id,

    -- attributes
    daily_actuals.order_type,
    daily_actuals.order_type_live,
    daily_actuals.order_type_grouped,
    daily_actuals.sales_qualified_source_name,
    daily_actuals.sales_qualified_source_grouped,
    SUM(total_targets.total_quarter_target)                     AS total_quarter_target,
    SUM(daily_actuals.booked_net_arr_in_snapshot_quarter)       AS booked_net_arr_in_snapshot_quarter,
    SUM(daily_actuals.open_1plus_net_arr_in_snapshot_quarter)   AS open_1plus_net_arr_in_snapshot_quarter,
    SUM(quarterly_actuals.total_booked_net_arr)                 AS total_booked_net_arr
  FROM base
  LEFT JOIN total_targets
    ON base.fiscal_quarter_name = total_targets.fiscal_quarter_name
      AND base.dim_sales_qualified_source_id = total_targets.dim_sales_qualified_source_id
      AND base.dim_crm_user_hierarchy_sk = total_targets.dim_crm_user_hierarchy_sk
      AND base.dim_order_type_id = total_targets.dim_order_type_id
  LEFT JOIN daily_actuals
    ON base.date_id = daily_actuals.snapshot_id
      AND base.dim_sales_qualified_source_id = daily_actuals.dim_sales_qualified_source_id
      AND base.dim_crm_user_hierarchy_sk = daily_actuals.dim_crm_user_hierarchy_sk
      AND base.dim_order_type_id = daily_actuals.dim_order_type_id
  LEFT JOIN quarterly_actuals
    ON base.fiscal_quarter_name = quarterly_actuals.snapshot_fiscal_quarter_name
      AND base.dim_sales_qualified_source_id = quarterly_actuals.dim_sales_qualified_source_id
      AND base.dim_crm_user_hierarchy_sk = quarterly_actuals.dim_crm_user_hierarchy_sk
      AND base.dim_order_type_id = quarterly_actuals.dim_order_type_id
  {{ dbt_utils.group_by(n=12) }}

)

SELECT * 
FROM final
