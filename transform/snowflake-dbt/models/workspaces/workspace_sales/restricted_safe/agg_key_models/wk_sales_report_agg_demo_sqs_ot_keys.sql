{{ config(alias='report_agg_demo_sqs_ot_keys') }}
-- supports FY22, FY23 grains

{{ simple_cte([
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('wk_sales_report_agg_keys_base', 'wk_sales_report_agg_keys_base'),
]) }}

, fy23_segment_adjustment AS (

    SELECT
      CASE
        WHEN (opp.report_opportunity_user_segment = 'mid-market'
              OR opp.report_opportunity_user_segment = 'smb')
          AND opp.report_opportunity_user_region = 'meta'
          THEN 'large'
        WHEN (opp.report_opportunity_user_segment = 'mid-market'
              OR opp.report_opportunity_user_segment = 'smb')
          AND opp.report_opportunity_user_region = 'latam'
          THEN 'large'
        WHEN (opp.report_opportunity_user_segment = 'mid-market'
              OR opp.report_opportunity_user_segment = 'smb')
          AND opp.report_opportunity_user_geo = 'apac'
          THEN 'large'
        ELSE opp.report_opportunity_user_segment
      END AS adjusted_report_opportunity_user_segment,

      opp.report_opportunity_user_geo,
      opp.report_opportunity_user_region,
      opp.report_opportunity_user_area,
      opp.order_type                                   AS order_type_stamped,
      opp.sales_qualified_source_name                  AS sales_qualified_source,
      opp.deal_category,
      opp.deal_group,
      opp.account_owner_user_segment,
      opp.account_owner_user_geo,
      opp.account_owner_user_region,
      opp.account_owner_user_area

    FROM mart_crm_opportunity AS opp


), eligible AS (

    SELECT
      LOWER(adjusted_report_opportunity_user_segment) AS report_opportunity_user_segment,
      LOWER(report_opportunity_user_geo)              AS report_opportunity_user_geo,
      LOWER(report_opportunity_user_region)           AS report_opportunity_user_region,
      LOWER(report_opportunity_user_area)             AS report_opportunity_user_area,

      LOWER(sales_qualified_source)                   AS sales_qualified_source,
      LOWER(order_type_stamped)                       AS order_type_stamped,

      LOWER(deal_category)                            AS deal_category,
      LOWER(deal_group)                               AS deal_group,

      LOWER(
        CONCAT(
          adjusted_report_opportunity_user_segment,
          '-',report_opportunity_user_geo,
          '-',report_opportunity_user_region,
          '-',report_opportunity_user_area
        )
      ) AS report_user_segment_geo_region_area,

      LOWER(
        CONCAT(
          adjusted_report_opportunity_user_segment,
          '-',report_opportunity_user_geo,
          '-',report_opportunity_user_region,
          '-',report_opportunity_user_area,
          '-',sales_qualified_source,
          '-', order_type_stamped
        )
      ) AS report_user_segment_geo_region_area_sqs_ot
    FROM
      fy23_segment_adjustment


), valid_keys AS (
    SELECT DISTINCT 

        -- Segment
        -- Sales Qualified Source
        -- Order Type

        -- Segment - Geo
        -- Segment - Geo - Region

        -- Segment - Geo - Order Type Group 
        -- Segment - Geo - Sales Qualified Source

        -- Segment - Geo - Region - Order Type Group 
        -- Segment - Geo - Region - Sales Qualified Source
        -- Segment - Geo - Region - Area

        -- Segment - Geo - Region - Area - Order Type Group 
        -- Segment - Geo - Region - Area - Sales Qualified Source

        eligible.*,

        report_opportunity_user_segment   AS key_segment,
        sales_qualified_source                     AS key_sqs,
        deal_group                                 AS key_ot,

        report_opportunity_user_segment || '_' || sales_qualified_source             AS key_segment_sqs,
        report_opportunity_user_segment || '_' || deal_group                         AS key_segment_ot,    

        report_opportunity_user_segment || '_' || report_opportunity_user_geo                                               AS key_segment_geo,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' ||  sales_qualified_source             AS key_segment_geo_sqs,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' ||  deal_group                         AS key_segment_geo_ot,      

        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region                                     AS key_segment_geo_region,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' ||  sales_qualified_source   AS key_segment_geo_region_sqs,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' ||  deal_group               AS key_segment_geo_region_ot,   

        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area                                       AS key_segment_geo_region_area,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' ||  sales_qualified_source     AS key_segment_geo_region_area_sqs,
        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' ||  deal_group                 AS key_segment_geo_region_area_ot,

        report_opportunity_user_segment || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_area                                       AS key_segment_geo_area,

        COALESCE(report_opportunity_user_segment ,'other')                                    AS sales_team_cro_level,
     
        -- NF: This code replicates the reporting structured of FY22, to keep current tools working
        CASE 
          WHEN report_opportunity_user_segment = 'large'
            AND report_opportunity_user_geo = 'emea'
              THEN 'large_emea'
          WHEN report_opportunity_user_segment = 'mid-market'
            AND report_opportunity_user_region = 'amer'
            AND lower(report_opportunity_user_area) LIKE '%west%'
              THEN 'mid-market_west'
          WHEN report_opportunity_user_segment = 'mid-market'
            AND report_opportunity_user_region = 'amer'
            AND lower(report_opportunity_user_area) NOT LIKE '%west%'
              THEN 'mid-market_east'
          WHEN report_opportunity_user_segment = 'smb'
            AND report_opportunity_user_region = 'amer'
            AND lower(report_opportunity_user_area) LIKE '%west%'
              THEN 'smb_west'
          WHEN report_opportunity_user_segment = 'smb'
            AND report_opportunity_user_region = 'amer'
            AND lower(report_opportunity_user_area) NOT LIKE '%west%'
              THEN 'smb_east'
          WHEN report_opportunity_user_segment = 'smb'
            AND report_opportunity_user_region = 'latam'
              THEN 'smb_east'
          WHEN (report_opportunity_user_segment IS NULL
                OR report_opportunity_user_region IS NULL)
              THEN 'other'
          WHEN CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_region) like '%other%'
            THEN 'other'
          ELSE CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_region)
        END                                                                           AS sales_team_rd_asm_level,

        COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo),'other')                                                                      AS sales_team_vp_level,
        COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo,'_',report_opportunity_user_region),'other')                                   AS sales_team_avp_rd_level,
        COALESCE(CONCAT(report_opportunity_user_segment,'_',report_opportunity_user_geo,'_',report_opportunity_user_region,'_',report_opportunity_user_area),'other')  AS sales_team_asm_level

  FROM eligible

)

SELECT *
FROM valid_keys