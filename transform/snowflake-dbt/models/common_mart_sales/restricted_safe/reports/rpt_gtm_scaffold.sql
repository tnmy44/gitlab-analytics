with created_actuals AS (
SELECT
    DIM_CRM_OPPORTUNITY_ID,
    DIM_ORDER_TYPE_ID,
    DIM_SALES_QUALIFIED_SOURCE_ID,
    DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
    arr_created_date                as actual_date,
    CASE WHEN is_net_arr_pipeline_created = TRUE
            AND arr_created_date IS NOT NULL
        THEN NET_ARR END            AS net_arr_pipeline_created,
    NULL                            AS net_arr_gtm,
    NULL                            AS new_logo_count_gtm,
    NULL                            AS positive_deal_ids,
    NULL                            AS negative_deal_ids,
    NULL                            AS sao_net_arr,
    NULL                            AS sao_opportunity_id
FROM PROD.restricted_safe_common.fct_crm_opportunity
WHERE LEFT(actual_date,4) BETWEEN 2019 AND 2027
)

, closed_actuals as (
SELECT
    DIM_CRM_OPPORTUNITY_ID,
    DIM_ORDER_TYPE_ID,
    DIM_SALES_QUALIFIED_SOURCE_ID,
    DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
    close_date                      as actual_date,
    NULL                            as net_arr_pipeline_created,
    CASE
        WHEN FPA_MASTER_BOOKINGS_FLAG = TRUE
            THEN net_arr END        AS net_arr_gtm,
    CASE
        WHEN is_new_logo_first_order = TRUE
            THEN new_logo_count END AS new_logo_count_gtm,
    CASE
        WHEN NEW_LOGO_COUNT >=0
            THEN DIM_CRM_OPPORTUNITY_ID
        END                         AS positive_deal_ids,
    CASE
        WHEN NEW_LOGO_COUNT = -1 THEN DIM_CRM_OPPORTUNITY_ID
            END                     AS negative_deal_ids,
    NULL                            AS sao_net_arr,
    NULL                            AS sao_opportunity_id
FROM PROD.restricted_safe_common.fct_crm_opportunity
WHERE LEFT(actual_date,4) BETWEEN 2019 AND 2027 )

, sao_actuals as (
SELECT
    DIM_CRM_OPPORTUNITY_ID,
    DIM_ORDER_TYPE_ID,
    DIM_SALES_QUALIFIED_SOURCE_ID,
    DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
    sales_accepted_date as actual_date,
    NULL                            AS net_arr_pipeline_created,
    NULL                            AS net_arr_gtm,
    NULL                            AS new_logo_count_gtm,
    NULL                            AS positive_deal_ids,
    NULL                            AS negative_deal_ids,
    CASE WHEN FPA_MASTER_BOOKINGS_FLAG = TRUE
        THEN net_arr END            AS sao_net_arr,
    CASE WHEN is_sao = TRUE
        THEN DIM_CRM_OPPORTUNITY_ID END AS sao_opportunity_id
FROM PROD.restricted_safe_common.fct_crm_opportunity
WHERE LEFT(actual_date,4) BETWEEN 2019 AND 2027 )

, unioned_actuals AS (
SELECT *
FROM created_actuals

UNION

SELECT *
FROM closed_actuals

UNION

SELECT *
FROM sao_actuals

)

, main_actuals AS (
SELECT unioned_actuals.DIM_ORDER_TYPE_ID,
       unioned_actuals.DIM_SALES_QUALIFIED_SOURCE_ID,
       unioned_actuals.DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
       DIM_DATE.DATE_ID AS actual_date_id
FROM unioned_actuals
LEFT JOIN COMMON.DIM_DATE
ON unioned_actuals.actual_date = DIM_DATE.DATE_ACTUAL
LEFT JOIN PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY
ON unioned_actuals.DIM_CRM_OPPORTUNITY_ID = MART_CRM_OPPORTUNITY.DIM_CRM_OPPORTUNITY_ID
WHERE FISCAL_YEAR > 2019
ORDER BY 1 desc
)

--targets
, dim_date AS (

  SELECT *
  FROM PROD.common.dim_date


)

, targets AS (

  SELECT *
  FROM restricted_safe_common.fct_sales_funnel_target_daily

)

, targets_dates as (
    SELECT
        targets.TARGET_DATE_ID,
        targets.dim_crm_user_hierarchy_sk,
        targets.dim_sales_qualified_source_id,
        targets.dim_order_type_id,
        targets.KPI_NAME,
        SUM(daily_allocated_target)          AS daily_allocated_target
  FROM targets
  LEFT JOIN dim_date
    ON targets.target_date_id = dim_date.date_id
  GROUP BY 1,2,3,4,5
)

, pivoted_targets AS (
SELECT
    target_date_id,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    "'Net ARR'" as net_arr_daily_allocated_target,
    "'Deals'" as deals_daily_allocated_target,
    "'New Logos'" as new_logos_daily_allocated_target,
    "'Stage 1 Opportunities'" as saos_daily_allocated_target,
    "'Net ARR Pipeline Created'" as pipeline_created_daily_allocated_target
FROM (
    SELECT *
    FROM targets_dates
    PIVOT(
        SUM(daily_allocated_target)
        FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
    )

)

, main_targets AS (
SELECT *
FROM pivoted_targets
)

--scaffold

, scaffold AS (

    SELECT
      main_actuals.actual_date_id                                              AS date_id,
      main_actuals.DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
      main_actuals.dim_order_type_id,
      main_actuals.dim_sales_qualified_source_id
    FROM main_actuals

    UNION

    SELECT
      main_targets.target_date_id,
      main_targets.dim_crm_user_hierarchy_sk,
      main_targets.dim_order_type_id,
      main_targets.dim_sales_qualified_source_id
    FROM main_targets

)

, base AS (
SELECT scaffold.DATE_ID,
       scaffold.DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
       scaffold.DIM_ORDER_TYPE_ID,
       scaffold.DIM_SALES_QUALIFIED_SOURCE_ID,
       CRM_USER_GEO,
       CRM_USER_region,
       CRM_USER_AREA,
       CRM_USER_BUSINESS_UNIT,
       CRM_USER_SALES_SEGMENT,
       crm_user_role_level_1,
       CRM_USER_ROLE_LEVEL_2,
       CRM_USER_ROLE_LEVEL_3,
       CRM_USER_ROLE_LEVEL_4,
       CRM_USER_ROLE_LEVEL_5,
       CRM_USER_ROLE_NAME,
       ORDER_TYPE_GROUPED,
       ORDER_TYPE_NAME,
       SALES_QUALIFIED_SOURCE_NAME,
       SALES_QUALIFIED_SOURCE_GROUPED,
       FISCAL_QUARTER_NAME_FY,
       DATE_ACTUAL,
       dim_date.DAY_OF_FISCAL_QUARTER,
       day_of_month,
       day_of_fiscal_year,
       DAY_OF_MONTH,
       DAY_OF_YEAR,
       FIRST_DAY_OF_MONTH,
       FIRST_DAY_OF_FISCAL_QUARTER,
       LAST_DAY_OF_FISCAL_QUARTER,
       LAST_DAY_OF_FISCAL_YEAR,
       LAST_DAY_OF_MONTH,
       FIRST_DAY_OF_FISCAL_YEAR,
       FISCAL_MONTH_NAME,
       dim_date.fiscal_year,
       FIRST_DAY_OF_YEAR
FROM scaffold
INNER JOIN  PROD.common.dim_crm_user_hierarchy
ON scaffold.DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK = dim_crm_user_hierarchy.DIM_CRM_USER_HIERARCHY_SK
INNER JOIN common.DIM_ORDER_TYPE
ON scaffold.DIM_ORDER_TYPE_ID = DIM_ORDER_TYPE.DIM_ORDER_TYPE_ID
INNER JOIN common.DIM_SALES_QUALIFIED_SOURCE
ON scaffold.DIM_SALES_QUALIFIED_SOURCE_ID = DIM_SALES_QUALIFIED_SOURCE.DIM_SALES_QUALIFIED_SOURCE_ID
FULL OUTER JOIN common.dim_date
ON scaffold.DATE_ID = DIM_DATE.DATE_ID
)

SELECT *
FROM base
WHERE FISCAL_YEAR BETWEEN 2019 AND 2027
