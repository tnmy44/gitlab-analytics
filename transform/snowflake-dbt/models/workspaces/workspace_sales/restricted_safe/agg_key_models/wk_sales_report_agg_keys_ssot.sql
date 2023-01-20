{{ config(alias='report_agg_keys_ssot') }}
-- based on report_agg_keys_base model
-- can replace the existing wk_sales_report_agg_demo_sqs_ot_keys

-- grains include:
-- segment, geo, region, area, sqs, ot, deal_category, deal_group


WITH report_agg_keys_fy24_model AS (

    SELECT *
    FROM {{ref('wk_sales_report_agg_keys_base')}}
    -- FROM PROD.RESTRICTED_SAFE_WORKSPACE_SALES.REPORT_AGG_KEYS_FY24_MODEL

)

SELECT DISTINCT
    report_opportunity_user_segment,
    report_opportunity_user_geo,
    report_opportunity_user_region,
    report_opportunity_user_area,
    sales_qualified_source,
    order_type_stamped,
    deal_category,
    deal_group,
    report_user_segment_geo_region_area,
    report_user_segment_geo_region_area_sqs_ot,
    key_segment,
    key_sqs,
    key_ot,
    key_segment_sqs,
    key_segment_ot,
    key_segment_geo,
    key_segment_geo_sqs,
    key_segment_geo_ot,
    key_segment_geo_region,
    key_segment_geo_region_sqs,
    key_segment_geo_region_ot,
    key_segment_geo_region_area,
    key_segment_geo_region_area_sqs,
    key_segment_geo_region_area_ot,
    key_segment_geo_area,
    sales_team_cro_level,
    sales_team_rd_asm_level,
    sales_team_vp_level,
    sales_team_avp_rd_level,
    sales_team_asm_level

FROM report_agg_keys_fy24_model