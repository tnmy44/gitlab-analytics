{{ simple_cte([
    ('fct_crm_opportunity','fct_crm_opportunity'),
    ('dim_date','dim_date'),
    ('targets', 'fct_sales_funnel_target_daily'),
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy'),
    ('dim_order_type', 'dim_order_type'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('mart_crm_opportunity', 'mart_crm_opportunity')
]) }},

created_actuals AS (
  SELECT
    dim_crm_opportunity_id,
    dim_order_type_id,
    dim_sales_qualified_source_id,
    dim_crm_current_account_set_hierarchy_sk,
    arr_created_date AS actual_date,
    CASE WHEN is_net_arr_pipeline_created = TRUE
        AND arr_created_date IS NOT NULL
        THEN net_arr
    END              AS net_arr_pipeline_created,
    NULL             AS net_arr_gtm,
    NULL             AS new_logo_count_gtm,
    NULL             AS positive_deal_ids,
    NULL             AS negative_deal_ids,
    NULL             AS sao_net_arr,
    NULL             AS sao_opportunity_id
  FROM fct_crm_opportunity
  WHERE LEFT(actual_date, 4) BETWEEN 2019 AND 2027
),

closed_actuals AS (
  SELECT
    dim_crm_opportunity_id,
    dim_order_type_id,
    dim_sales_qualified_source_id,
    dim_crm_current_account_set_hierarchy_sk,
    close_date AS actual_date,
    NULL       AS net_arr_pipeline_created,
    CASE
      WHEN fpa_master_bookings_flag = TRUE
        THEN net_arr
    END        AS net_arr_gtm,
    CASE
      WHEN is_new_logo_first_order = TRUE
        THEN new_logo_count
    END        AS new_logo_count_gtm,
    CASE
      WHEN new_logo_count >= 0
        THEN dim_crm_opportunity_id
    END        AS positive_deal_ids,
    CASE
      WHEN new_logo_count = -1 THEN dim_crm_opportunity_id
    END        AS negative_deal_ids,
    NULL       AS sao_net_arr,
    NULL       AS sao_opportunity_id
  FROM fct_crm_opportunity
  WHERE LEFT(actual_date, 4) BETWEEN 2019 AND 2027
),

sao_actuals AS (
  SELECT
    dim_crm_opportunity_id,
    dim_order_type_id,
    dim_sales_qualified_source_id,
    dim_crm_current_account_set_hierarchy_sk,
    sales_accepted_date AS actual_date,
    NULL                AS net_arr_pipeline_created,
    NULL                AS net_arr_gtm,
    NULL                AS new_logo_count_gtm,
    NULL                AS positive_deal_ids,
    NULL                AS negative_deal_ids,
    CASE WHEN fpa_master_bookings_flag = TRUE
        THEN net_arr
    END                 AS sao_net_arr,
    CASE WHEN is_sao = TRUE
        THEN dim_crm_opportunity_id
    END                 AS sao_opportunity_id
  FROM fct_crm_opportunity
  WHERE LEFT(actual_date, 4) BETWEEN 2019 AND 2027
),

unioned_actuals AS (
  SELECT *
  FROM created_actuals

  UNION

  SELECT *
  FROM closed_actuals

  UNION

  SELECT *
  FROM sao_actuals

),

main_actuals AS (
  SELECT
    unioned_actuals.dim_order_type_id,
    unioned_actuals.dim_sales_qualified_source_id,
    unioned_actuals.dim_crm_current_account_set_hierarchy_sk,
    dim_date.date_id AS actual_date_id
  FROM unioned_actuals
  LEFT JOIN dim_date
    ON unioned_actuals.actual_date = dim_date.date_actual
  LEFT JOIN mart_crm_opportunity
    ON unioned_actuals.dim_crm_opportunity_id = mart_crm_opportunity.dim_crm_opportunity_id
  WHERE fiscal_year > 2019
  ORDER BY 1 DESC
),

--targets

targets_dates AS (
  SELECT
    targets.target_date_id,
    targets.dim_crm_user_hierarchy_sk,
    targets.dim_sales_qualified_source_id,
    targets.dim_order_type_id,
    targets.kpi_name,
    SUM(targets.daily_allocated_target) AS daily_allocated_target
  FROM targets
  LEFT JOIN dim_date
    ON targets.target_date_id = dim_date.date_id
  GROUP BY 1, 2, 3, 4, 5
),

pivoted_targets AS (
  SELECT
    target_date_id,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    "'Net ARR'"                  AS net_arr_daily_allocated_target,
    "'Deals'"                    AS deals_daily_allocated_target,
    "'New Logos'"                AS new_logos_daily_allocated_target,
    "'Stage 1 Opportunities'"    AS saos_daily_allocated_target,
    "'Net ARR Pipeline Created'" AS pipeline_created_daily_allocated_target
  FROM (
    SELECT *
    FROM targets_dates
    PIVOT (
      SUM(daily_allocated_target)
      FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
  )

),
--scaffold

scaffold AS (

  SELECT
    main_actuals.actual_date_id AS date_id,
    main_actuals.dim_crm_current_account_set_hierarchy_sk,
    main_actuals.dim_order_type_id,
    main_actuals.dim_sales_qualified_source_id
  FROM main_actuals

  UNION

  SELECT
    pivoted_targets.target_date_id,
    pivoted_targets.dim_crm_user_hierarchy_sk,
    pivoted_targets.dim_order_type_id,
    pivoted_targets.dim_sales_qualified_source_id
  FROM pivoted_targets

),

base AS (
  SELECT
    scaffold.date_id,
    scaffold.dim_crm_current_account_set_hierarchy_sk,
    scaffold.dim_order_type_id,
    scaffold.dim_sales_qualified_source_id,
    dim_crm_user_hierarchy.crm_user_geo,
    dim_crm_user_hierarchy.crm_user_region,
    dim_crm_user_hierarchy.crm_user_area,
    dim_crm_user_hierarchy.crm_user_business_unit,
    dim_crm_user_hierarchy.crm_user_sales_segment,
    dim_crm_user_hierarchy.crm_user_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5,
    dim_crm_user_hierarchy.crm_user_role_name,
    dim_order_type.order_type_grouped,
    dim_order_type.order_type_name,
    dim_sales_qualified_source.sales_qualified_source_name,
    dim_sales_qualified_source.sales_qualified_source_grouped,
    dim_date.fiscal_quarter_name_fy,
    dim_date.date_actual,
    dim_date.day_of_fiscal_quarter,
    dim_date.day_of_month,
    dim_date.day_of_fiscal_year,
    dim_date.day_of_year,
    dim_date.first_day_of_month,
    dim_date.first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_year,
    dim_date.last_day_of_month,
    dim_date.first_day_of_fiscal_year,
    dim_date.fiscal_month_name,
    dim_date.fiscal_year,
    dim_date.first_day_of_year
  FROM scaffold
  INNER JOIN dim_crm_user_hierarchy
    ON scaffold.dim_crm_current_account_set_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
  INNER JOIN dim_order_type
    ON scaffold.dim_order_type_id = dim_order_type.dim_order_type_id
  INNER JOIN dim_sales_qualified_source
    ON scaffold.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
  FULL OUTER JOIN dim_date
    ON scaffold.date_id = dim_date.date_id
)

SELECT *
FROM base
WHERE fiscal_year BETWEEN 2019 AND 2027
