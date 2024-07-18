with dim_date AS (

  SELECT *
  FROM PROD.common.dim_date


)

, targets AS (

  SELECT *
  FROM prod.restricted_safe_common.fct_sales_funnel_target_daily

)

 , targets_dates as (
    SELECT DISTINCT
        targets.TARGET_DATE,
        targets.TARGET_DATE_ID,
        targets.REPORT_TARGET_DATE,
        dim_date.FISCAL_QUARTER_NAME_FY,
        targets.dim_crm_user_hierarchy_sk,
        targets.dim_sales_qualified_source_id,
        targets.dim_order_type_id,
        targets.KPI_NAME,
        CONCAT(TO_VARCHAR(TARGET_DATE_ID),TO_CHAR(DIM_CRM_USER_HIERARCHY_SK),TO_CHAR(DIM_SALES_QUALIFIED_SOURCE_ID),TO_CHAR(DIM_ORDER_TYPE_ID)) as key ,
        DAILY_ALLOCATED_TARGET       AS daily_allocated_target,
        MTD_ALLOCATED_TARGET         AS mtd_allocated_target,
        QTD_ALLOCATED_TARGET         AS qtd_allocated_target,
        YTD_ALLOCATED_TARGET         AS ytd_allocated_target
  FROM targets
  LEFT JOIN dim_date
    ON targets.target_date_id = dim_date.date_id

)

, pivoted_targets AS (
SELECT
    target_date,
    target_date_id,
    REPORT_TARGET_DATE,
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    "'Net ARR'"                     as net_arr_daily_allocated_target,
    "'Deals'"                       as deals_daily_allocated_target,
    "'New Logos'"                   as new_logos_daily_allocated_target,
    "'Stage 1 Opportunities'"       as saos_daily_allocated_target,
    "'Net ARR Pipeline Created'"    as pipeline_created_daily_allocated_target
FROM (
    SELECT *
    FROM targets_dates
    PIVOT(
        SUM(daily_allocated_target)
        FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
    )

)

, mtd_pivoted_targets AS (
SELECT
    target_date,
    target_date_id,
    REPORT_TARGET_DATE,
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    key,
    "'Net ARR'"                  as mtd_net_arr_target,
    "'Deals'"                    as mtd_deals_target,
    "'New Logos'"                as mtd_new_logos_target,
    "'Stage 1 Opportunities'"    as mtd_sao_target,
    "'Net ARR Pipeline Created'" as mtd_pipeline_created_target
FROM (
    SELECT *
    FROM targets_dates
    PIVOT(
        SUM(mtd_allocated_target)
        FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
    )

)

, qtd_pivoted_targets AS (
SELECT
    target_date,
    target_date_id,
    REPORT_TARGET_DATE,
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    key,
    "'Net ARR'"                  as qtd_net_arr_target,
    "'Deals'"                    as qtd_deals_target,
    "'New Logos'"                as qtd_new_logos_target,
    "'Stage 1 Opportunities'"    as qtd_saos_target,
    "'Net ARR Pipeline Created'" as qtd_pipeline_created_target
FROM (
    SELECT *
    FROM targets_dates
    PIVOT(
        SUM(qtd_allocated_target)
        FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
    )

)

, ytd_pivoted_targets AS (
SELECT
    target_date,
    target_date_id,
    REPORT_TARGET_DATE,
    fiscal_quarter_name_fy,
    dim_crm_user_hierarchy_sk,
    dim_sales_qualified_source_id,
    dim_order_type_id,
    key,
    "'Net ARR'"                  as ytd_net_arr_target,
    "'Deals'"                    as ytd_deals_target,
    "'New Logos'"                as ytd_new_logos_target,
    "'Stage 1 Opportunities'"    as ytd_saos_target,
    "'Net ARR Pipeline Created'" as ytd_pipeline_created_target
FROM (
    SELECT *
    FROM targets_dates
    PIVOT(
        SUM(ytd_allocated_target)
        FOR kpi_name IN ('Net ARR', 'Deals', 'New Logos', 'Stage 1 Opportunities', 'Net ARR Pipeline Created')
    ) AS pvt
    )

)

, final AS ( -- need to use the MAX function in order to deal with the nulls from the various CTE's otherwise we get four of the same key per row
    SELECT DISTINCT
--dimensions
       pivoted_targets.target_date,
       pivoted_targets.target_date_id,
       pivoted_targets.fiscal_quarter_name_fy,
       pivoted_targets.dim_crm_user_hierarchy_sk,
       pivoted_targets.dim_sales_qualified_source_id,
       pivoted_targets.dim_order_type_id
       -- daily allocated targets
       , MAX(net_arr_daily_allocated_target) AS net_arr_daily_allocated_target
       , MAX(deals_daily_allocated_target) AS deals_daily_allocated_target
       , MAX(new_logos_daily_allocated_target) AS new_logos_daily_allocated_target
       , MAX(saos_daily_allocated_target) AS saos_daily_allocated_target
       , MAX(pipeline_created_daily_allocated_target) AS pipeline_created_daily_allocated_target
       --mtd targets
       , MAX(mtd_net_arr_target)             as mtd_net_arr_daily_allocated_target
       , MAX(mtd_deals_target)               as mtd_deals_daily_allocated_target
       , MAX(mtd_new_logos_target) as mtd_new_logos_daily_allocated_target
       , MAX(mtd_sao_target) as mtd_saos_daily_allocated_target
       , MAX(mtd_pipeline_created_target) as mtd_pipeline_created_daily_allocated_target
       --qtd targets
        ,MAX(qtd_net_arr_target) as qtd_net_arr_daily_allocated_target
       , MAX(qtd_deals_target)              as qtd_deals_daily_allocated_target
       , MAX(qtd_new_logos_target) as qtd_new_logos_daily_allocated_target
       , MAX(qtd_saos_target) as qtd_saos_daily_allocated_target
       , MAX(qtd_pipeline_created_target) as qtd_pipeline_created_daily_allocated_target
       -- ytd targets
       , MAX(ytd_net_arr_target)     as ytd_net_arr_daily_allocated_target
       , MAX(ytd_deals_target) as ytd_deals_daily_allocated_target
       , MAX(ytd_new_logos_target) as ytd_new_logos_daily_allocated_target
       , MAX(ytd_saos_target) as ytd_saos_daily_allocated_target
       , MAX(ytd_pipeline_created_target) as ytd_pipeline_created_daily_allocated_target
FROM pivoted_targets
LEFT JOIN mtd_pivoted_targets
ON  pivoted_targets.TARGET_DATE=                   mtd_pivoted_targets.REPORT_TARGET_DATE
    AND pivoted_targets.DIM_SALES_QUALIFIED_SOURCE_ID = mtd_pivoted_targets.DIM_SALES_QUALIFIED_SOURCE_ID
    AND pivoted_targets.DIM_ORDER_TYPE_ID             = mtd_pivoted_targets.DIM_ORDER_TYPE_ID
    AND pivoted_targets.DIM_CRM_USER_HIERARCHY_SK =     mtd_pivoted_targets.DIM_CRM_USER_HIERARCHY_SK
LEFT JOIN qtd_pivoted_targets
ON  pivoted_targets.TARGET_DATE=                 qtd_pivoted_targets.REPORT_TARGET_DATE
    AND pivoted_targets.DIM_SALES_QUALIFIED_SOURCE_ID = qtd_pivoted_targets.DIM_SALES_QUALIFIED_SOURCE_ID
    AND pivoted_targets.DIM_ORDER_TYPE_ID             = qtd_pivoted_targets.DIM_ORDER_TYPE_ID
    AND pivoted_targets.DIM_CRM_USER_HIERARCHY_SK =     qtd_pivoted_targets.DIM_CRM_USER_HIERARCHY_SK

LEFT JOIN ytd_pivoted_targets
ON  pivoted_targets.TARGET_DATE=                 ytd_pivoted_targets.REPORT_TARGET_DATE
    AND pivoted_targets.DIM_SALES_QUALIFIED_SOURCE_ID = ytd_pivoted_targets.DIM_SALES_QUALIFIED_SOURCE_ID
    AND pivoted_targets.DIM_ORDER_TYPE_ID             = ytd_pivoted_targets.DIM_ORDER_TYPE_ID
    AND pivoted_targets.DIM_CRM_USER_HIERARCHY_SK =     ytd_pivoted_targets.DIM_CRM_USER_HIERARCHY_SK
GROUP BY 1,2,3,4,5,6)

SELECT *
FROM final
