  
  {{ config(alias='mart_sales_funnel_target_ssot') }}

    -- FY24 structure file
    WITH date_details AS (
        SELECT *
        FROM {{ ref('wk_sales_date_details') }}
        --FROM workspace_sales.date_details

  ), mart_sales_funnel_target AS (
        SELECT *
        FROM {{ref('mart_sales_funnel_target')}}
        --FROM restricted_safe_common_mart_sales.mart_sales_funnel_target

), agg_demo_keys AS (

        SELECT *
        FROM {{ ref('wk_sales_report_agg_keys_ssot') }}
        --FROM restricted_safe_workspace_sales.report_agg_keys_ssot

 ), mart_sales_funnel_target_prep AS (
    -- JK 2023-01-19: additional grouping CTE in case more keys/grains are created in the target file

    SELECT
      sales_funnel_target_id,
      target_month,
      kpi_name,
      -- business unit should show up here
      -- Business Unit (X-Ray 1st hierarchy)
      -- will be replaced with the actual field
      CASE
        WHEN LOWER(crm_user_sales_segment) IN ('large','pubsec','all')  -- "all" segment is PubSec for ROW
            THEN 'ENTG'
        WHEN LOWER(crm_user_region) IN ('latam','meta')
            OR LOWER(crm_user_geo) IN ('apac')
            THEN 'ENTG'
        WHEN LOWER(crm_user_sales_segment) IN ('mid-market','smb')
            THEN 'COMM'
        WHEN LOWER(crm_user_sales_segment) = 'jihu' THEN 'JiHu'
        ELSE 'Other'
      END                               AS crm_user_business_unit,

    CASE
        WHEN LOWER(crm_user_business_unit) = 'entg'
          THEN crm_user_geo
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND
            (
            LOWER(crm_user_sales_segment) = 'smb'
            AND LOWER(crm_user_geo) = 'amer'
            AND LOWER(crm_user_area) = 'lowtouch'
            )
          THEN 'AMER Low-Touch'
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND
            (
            LOWER(crm_user_sales_segment) = 'mid-market'
            AND (LOWER(crm_user_geo) = 'amer'
                     OR LOWER(crm_user_geo) = 'emea')
            AND LOWER(crm_user_area) != 'norben'
            AND LOWER(order_type_name) = '1. new - first order'
            )
          THEN 'MM First Orders'  --mid-market FO(?)
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND LOWER(crm_user_geo) = 'emea'
          THEN  'EMEA'
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND LOWER(crm_user_geo) = 'amer'
          THEN 'AMER'
        ELSE 'Other'
      END AS crm_user_sub_business_unit,

      -- Division (X-Ray 3rd hierarchy)
      CASE
        WHEN LOWER(crm_user_business_unit) = 'entg'
          THEN crm_user_region


        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND LOWER(crm_user_sales_segment) = 'mid-market'
          AND LOWER(crm_user_area) != 'norben'
          AND LOWER(order_type_name) = '1. new - first order'
          THEN 'MM First Orders'
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND (LOWER(crm_user_sub_business_unit) = 'amer'
                   OR LOWER(crm_user_sub_business_unit) = 'emea')
          AND (LOWER(order_type_name) != '1. new - first order'
                    OR LOWER(crm_user_area) = 'norben')
          AND LOWER(crm_user_sales_segment) = 'mid-market'
          THEN 'Mid-Market'
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND (LOWER(crm_user_sub_business_unit) = 'amer'
                OR LOWER(crm_user_sub_business_unit) = 'emea')
          AND LOWER(crm_user_sales_segment) = 'smb'
          THEN 'SMB'
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
            AND LOWER(crm_user_sales_segment) = 'smb'
          AND LOWER(crm_user_sub_business_unit) = 'amer low-touch'
          THEN 'AMER Low-Touch'
        ELSE 'Other'
      END AS crm_user_division,

       -- ASM (X-Ray 4th hierarchy): definition pending
      CASE
        WHEN
          LOWER(crm_user_business_unit) = 'entg'
          AND LOWER(crm_user_sub_business_unit) = 'amer'
          THEN crm_user_area
        WHEN
          LOWER(crm_user_business_unit) = 'entg'
          AND LOWER(crm_user_sub_business_unit) = 'emea'
          AND (
              LOWER(crm_user_division) = 'dach'
                OR LOWER(crm_user_division) = 'neur'
                OR LOWER(crm_user_division) = 'seur'
              )
          THEN crm_user_area
        WHEN
          LOWER(crm_user_business_unit) = 'entg'
          AND LOWER(crm_user_sub_business_unit) = 'emea'
          AND LOWER(crm_user_division) = 'meta'
          THEN crm_user_sales_segment
        WHEN
          LOWER(crm_user_business_unit) = 'entg'
          AND LOWER(crm_user_sub_business_unit) = 'apac'
          THEN crm_user_area
        WHEN
          LOWER(crm_user_business_unit) = 'entg'
          AND LOWER(crm_user_sub_business_unit) = 'pubsec'
          AND LOWER(crm_user_division) != 'sled'
          THEN crm_user_area
        WHEN
          LOWER(crm_user_business_unit) = 'entg'
          AND LOWER(crm_user_sub_business_unit) = 'pubsec'
          AND LOWER(crm_user_division) = 'sled'
          THEN crm_user_region

        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND (LOWER(crm_user_sub_business_unit) = 'amer'
                   OR LOWER(crm_user_sub_business_unit) = 'emea')
          THEN crm_user_area
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
          AND LOWER(crm_user_sub_business_unit) = 'mm first orders'
          THEN crm_user_geo
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
            AND LOWER(crm_user_sales_segment) = 'smb'
            AND LOWER(crm_user_sub_business_unit) = 'amer low-touch'
            AND LOWER(order_type_name) = '1. new - first order'
          THEN 'LowTouch FO'
        WHEN
          LOWER(crm_user_business_unit) = 'comm'
            AND LOWER(crm_user_sales_segment) = 'smb'
            AND LOWER(crm_user_sub_business_unit) = 'amer low-touch'
            AND LOWER(order_type_name) != '1. new - first order'
          THEN 'LowTouch Pool'
        ELSE 'Other'
      END AS crm_user_asm,

      crm_user_sales_segment,
      crm_user_sales_segment_grouped,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      crm_user_sales_segment_region_grouped,
      order_type_name,
      order_type_grouped,
      sales_qualified_source_name,
      sales_qualified_source_grouped,
      created_by,
      updated_by,
      model_created_date,
      model_updated_date,
      dbt_updated_at,
      dbt_created_at,
      SUM(allocated_target) AS allocated_target

    FROM mart_sales_funnel_target
    -- FROM prod.restricted_safe_common_mart_sales.mart_sales_funnel_target
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23

  ), mart_sales_funnel_target_expanded AS (

    SELECT
      funnel_target.*,
      -- 20220214 NF: Temporary keys, until the SFDC key is exposed,
      CASE
        WHEN funnel_target.order_type_name = '3. Growth'
            THEN '2. Growth'
        WHEN funnel_target.order_type_name = '1. New - First Order'
            THEN '1. New'
          ELSE '3. Other'
      END                                                       AS deal_group,
      COALESCE(funnel_target.sales_qualified_source_name,'NA')  AS sales_qualified_source,

    LOWER(
            CONCAT(
            funnel_target.crm_user_business_unit, '-',
            funnel_target.crm_user_sub_business_unit, '-',
            funnel_target.crm_user_division, '-',
            funnel_target.crm_user_asm, '-',
            funnel_target.crm_user_sales_segment,'-',
            funnel_target.crm_user_geo,'-',
            funnel_target.crm_user_region,'-',
            funnel_target.crm_user_area, '-',
            sales_qualified_source, '-',
            deal_group)
            ) AS report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot

    FROM mart_sales_funnel_target_prep AS funnel_target

  ), final AS (

    SELECT
      funnel_target.*,
      target_month.fiscal_quarter_name_fy           AS target_fiscal_quarter_name,
      target_month.first_day_of_fiscal_quarter      AS target_fiscal_quarter_date,
      target_month.fiscal_year                      AS target_fiscal_year,

      -- FY23 keys
      LOWER(agg_demo_keys.key_segment)                         AS key_segment,
      LOWER(agg_demo_keys.key_segment_sqs)                     AS key_segment_sqs,
      LOWER(agg_demo_keys.key_segment_ot)                      AS key_segment_ot,
      LOWER(agg_demo_keys.key_segment_geo)                     AS key_segment_geo,
      LOWER(agg_demo_keys.key_segment_geo_sqs)                 AS key_segment_geo_sqs,
      LOWER(agg_demo_keys.key_segment_geo_ot)                  AS key_segment_geo_ot,
      LOWER(agg_demo_keys.key_segment_geo_region)              AS key_segment_geo_region,
      LOWER(agg_demo_keys.key_segment_geo_region_sqs)          AS key_segment_geo_region_sqs,
      LOWER(agg_demo_keys.key_segment_geo_region_ot)           AS key_segment_geo_region_ot,
      LOWER(agg_demo_keys.key_segment_geo_region_area)         AS key_segment_geo_region_area,
      LOWER(agg_demo_keys.key_segment_geo_region_area_sqs)     AS key_segment_geo_region_area_sqs,
      LOWER(agg_demo_keys.key_segment_geo_region_area_ot)      AS key_segment_geo_region_area_ot,
      LOWER(agg_demo_keys.key_segment_geo_area)                AS key_segment_geo_area,
      LOWER(agg_demo_keys.sales_team_cro_level)                AS sales_team_cro_level,
      LOWER(agg_demo_keys.sales_team_rd_asm_level)             AS sales_team_rd_asm_level,
      LOWER(agg_demo_keys.sales_team_vp_level)                 AS sales_team_vp_level,
      LOWER(agg_demo_keys.sales_team_avp_rd_level)             AS sales_team_avp_rd_level,
      LOWER(agg_demo_keys.sales_team_asm_level)                AS sales_team_asm_level,

      -- JK 2023-02-06: FY24 keys
      LOWER(agg_demo_keys.key_sqs)                             AS key_sqs,
      LOWER(agg_demo_keys.key_ot)                              AS key_ot,
      LOWER(agg_demo_keys.key_bu)                      AS key_bu,
      LOWER(agg_demo_keys.key_bu_ot)                   AS key_bu_ot,
      LOWER(agg_demo_keys.key_bu_sqs)                  AS key_bu_sqs,
      LOWER(agg_demo_keys.key_bu_subbu)                AS key_bu_subbu,
      LOWER(agg_demo_keys.key_bu_subbu_ot)             AS key_bu_subbu_ot,
      LOWER(agg_demo_keys.key_bu_subbu_sqs)            AS key_bu_subbu_sqs,
      LOWER(agg_demo_keys.key_bu_subbu_division)       AS key_bu_subbu_division,
      LOWER(agg_demo_keys.key_bu_subbu_division_ot)    AS key_bu_subbu_division_ot,
      LOWER(agg_demo_keys.key_bu_subbu_division_sqs)   AS key_bu_subbu_division_sqs,
      LOWER(agg_demo_keys.key_bu_subbu_division_asm)   AS key_bu_subbu_division_asm

    FROM mart_sales_funnel_target_expanded funnel_target
    INNER JOIN  date_details target_month
      ON target_month.date_actual = funnel_target.target_month
    LEFT JOIN agg_demo_keys
      ON funnel_target.report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot
    WHERE LOWER(funnel_target.deal_group) LIKE ANY ('%growth%','%new%')

)

SELECT *
FROM final