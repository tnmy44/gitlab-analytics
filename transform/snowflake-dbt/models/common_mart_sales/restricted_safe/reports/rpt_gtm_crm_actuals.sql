with created_actuals AS (
SELECT
    DIM_CRM_OPPORTUNITY_ID,
    DIM_ORDER_TYPE_ID,
    DIM_SALES_QUALIFIED_SOURCE_ID,
    DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
    arr_created_date                as actual_date,
    CASE WHEN is_net_arr_pipeline_created = TRUE
            AND arr_created_date IS NOT NULL
        THEN net_arr END            AS net_arr_pipeline_created,
    NULL                            AS net_arr_closed,
    NULL                            AS new_logo_count_closed,
    NULL                            AS deal_ids_count,
    NULL                            AS sao_net_arr,
    NULL                            AS sao_opportunity_id_count
FROM PROD.restricted_safe_common.fct_crm_opportunity
WHERE is_net_arr_pipeline_created = TRUE
    AND arr_created_date IS NOT NULL
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
            THEN BOOKED_NET_ARR END        AS net_arr_closed,
    CASE
        WHEN is_new_logo_first_order = TRUE
            THEN new_logo_count END AS new_logo_count_closed,
    CASE
        WHEN NEW_LOGO_COUNT >=0
            THEN 1
        WHEN NEW_LOGO_COUNT = -1 THEN -1
        END                         AS deal_ids_count,
    NULL                            AS sao_net_arr,
    NULL                            AS sao_opportunity_id_count
FROM PROD.restricted_safe_common.fct_crm_opportunity
WHERE FPA_MASTER_BOOKINGS_FLAG = TRUE)

, sao_actuals as (
SELECT
    DIM_CRM_OPPORTUNITY_ID,
    DIM_ORDER_TYPE_ID,
    DIM_SALES_QUALIFIED_SOURCE_ID,
    DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
    sales_accepted_date as actual_date,
    NULL                            AS net_arr_pipeline_created,
    NULL                            AS net_arr_closed,
    NULL                            AS new_logo_count_closed,
    NULL                            AS deal_ids_count,
    CASE WHEN FPA_MASTER_BOOKINGS_FLAG = TRUE
        THEN net_arr END            AS sao_net_arr,
    CASE WHEN is_sao = TRUE
        THEN 1 END AS                  sao_opportunity_id_count
FROM PROD.restricted_safe_common.fct_crm_opportunity
)

--union
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
SELECT unioned_actuals.DIM_CRM_OPPORTUNITY_ID,
       unioned_actuals.DIM_ORDER_TYPE_ID,
       unioned_actuals.DIM_SALES_QUALIFIED_SOURCE_ID,
       unioned_actuals.DIM_CRM_CURRENT_ACCOUNT_SET_HIERARCHY_SK,
       unioned_actuals.actual_date,
       unioned_actuals.net_arr_pipeline_created,
       unioned_actuals.net_arr_closed,
       unioned_actuals.new_logo_count_closed,
       unioned_actuals.deal_ids_count,
       unioned_actuals.sao_net_arr,
       unioned_actuals.sao_opportunity_id_count as sao_count,
       DIM_DATE.DATE_ID AS actual_date_id,
       MART_CRM_OPPORTUNITY.PRODUCT_CATEGORY,
       MART_CRM_OPPORTUNITY.DEAL_SIZE,
       dim_date.FISCAL_QUARTER_NAME_FY,
       dim_date.FISCAL_YEAR
    --COUNT(DISTINCT sao_opportunity_id) AS sao_count,
    --(DISTINCT positive_deal_ids) - COUNT(DISTINCT negative_deal_ids) AS deal_count
FROM unioned_actuals
LEFT JOIN COMMON.DIM_DATE
ON unioned_actuals.actual_date = DIM_DATE.DATE_ACTUAL
LEFT JOIN PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY
ON unioned_actuals.DIM_CRM_OPPORTUNITY_ID = MART_CRM_OPPORTUNITY.DIM_CRM_OPPORTUNITY_ID
WHERE FISCAL_YEAR > 2020
ORDER BY 1 desc
)

SELECT FISCAL_QUARTER_NAME_FY, SUM(deal_ids_count)
FROM main_actuals
WHERE FISCAL_YEAR > 2019
GROUP BY 1
ORDER BY 1 desc