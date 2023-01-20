{{ config(alias='report_agg_keys_base') }}
-- supports FY22, FY23 and FY24 granularity 

-- grains include:
-- segment, geo, region, area, sqs, ot, deal_category, deal_group
-- business_unit, role_type, partner_category, alliance_partner 


WITH sfdc_account_xf AS (

    SELECT *
    -- FROM PROD.restricted_safe_workspace_sales.sfdc_accounts_xf
    FROM {{ref('wk_sales_sfdc_accounts_xf')}}

), sfdc_users_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_users_xf')}}
    -- FROM prod.workspace_sales.sfdc_users_xf

), mart_crm_opportunity AS (

    SELECT *
    FROM {{ref('mart_crm_opportunity')}}

), field_for_keys AS (
    
    SELECT
        CASE report_opportunity_user_segment
            WHEN 'large' THEN 'ent-g'
            WHEN 'pubsec' THEN 'ent-g'
            WHEN 'mid-market' THEN 'comm'
            WHEN 'smb' THEN 'comm'
            WHEN 'jihu' THEN 'jihu'
            ELSE 'other'
        END AS business_unit,

        CASE
            WHEN  order_type = '1. New - First Order'
                THEN 'First Order'
            WHEN lower(account_owner.role_name) like ('pooled%')
                    AND key_segment IN ('smb','mid-market')
                    AND order_type != '1. New - First Order'
                THEN 'Pooled'
            WHEN lower(account_owner.role_name) like ('terr%')
                    AND key_segment IN ('smb','mid-market')
                    AND order_type != '1. New - First Order'
                THEN 'Territory'
            WHEN lower(account_owner.role_name) like ('named%')
                    AND key_segment IN ('smb','mid-market')
                    AND order_type != '1. New - First Order'
                THEN 'Named'
            WHEN order_type IN ('2. New - Connected','4. Contraction','6. Churn - Final','5. Churn - Partial','3. Growth')
                    AND key_segment IN ('smb','mid-market')
                THEN 'Expansion'
            ELSE 'Other'
        END AS role_type,

-- FY23 definition
        -- CASE
        --     WHEN sales_qualified_source_name = 'Channel Generated'
        --     THEN 'Channel Generated'
        --     WHEN sales_qualified_source_name != 'Channel Generated'
        --     AND NOT(LOWER(resale.account_name) LIKE ANY ('%ibm%','%google%','%gcp%','%amazon%'))
        --     THEN 'Channel Co-Sell'
        --     WHEN sales_qualified_source_name != 'Channel Generated'
        --     AND LOWER(resale.account_name) LIKE ANY ('%ibm%','%google%','%gcp%','%amazon%')
        --     THEN 'Alliance Co-Sell'
        --     ELSE 'Direct'
        -- END                                               AS partner_category,

        -- CASE
        --     WHEN LOWER(resale.account_name)LIKE '%ibm%'
        --     THEN 'IBM'
        --     WHEN LOWER(resale.account_name) LIKE ANY ('%google%','%gcp%')
        --     THEN 'GCP'
        --     WHEN LOWER(resale.account_name) LIKE '%amazon%'
        --     THEN 'AWS'
        --     WHEN LOWER(resale.account_name) IS NOT NULL
        --     THEN 'Channel'
        --     ELSE 'Direct'
        -- END                                               AS alliance_partner,


        CASE
          WHEN (sales_qualified_source_name = 'Channel Generated' OR sales_qualified_source_name = 'Partner Generated')
              THEN 'Partner Sourced'
          WHEN (sales_qualified_source_name != 'Channel Generated' AND sales_qualified_source_name != 'Partner Generated')
              AND NOT LOWER(resale.account_name) LIKE ANY ('%google%','%gcp%','%amazon%')
              THEN 'Channel Co-Sell'
          WHEN (sales_qualified_source_name != 'Channel Generated' AND sales_qualified_source_name != 'Partner Generated')
              AND LOWER(resale.account_name) LIKE ANY ('%google%','%gcp%','%amazon%')
              THEN 'Alliance Co-Sell'
          ELSE 'Direct'
        END                                               AS partner_category,

        CASE
          WHEN LOWER(resale.account_name) LIKE ANY ('%google%','%gcp%')
            THEN 'GCP'
          WHEN LOWER(resale.account_name) LIKE ANY ('%amazon%')
            THEN 'AWS'
          WHEN LOWER(resale.account_name) IS NOT NULL
            THEN 'Channel'
          ELSE 'Direct'
        END                                               AS alliance_partner,


        report_opportunity_user_segment,
        report_opportunity_user_geo,
        report_opportunity_user_region,
        report_opportunity_user_area,
        order_type AS order_type_stamped,
        sales_qualified_source_name AS sales_qualified_source,
        deal_category,
        deal_group,
        opp.account_owner_user_segment,
        opp.account_owner_user_geo,
        opp.account_owner_user_region,
        opp.account_owner_user_area

    FROM mart_crm_opportunity AS opp
    LEFT JOIN sfdc_users_xf AS account_owner
        ON opp.owner_id = account_owner.user_id
    LEFT JOIN sfdc_account_xf AS resale
        ON opp.fulfillment_partner = resale.account_id


), eligible AS (

  SELECT         
        LOWER(business_unit)                         AS business_unit,
        LOWER(report_opportunity_user_segment)       AS report_opportunity_user_segment,
        LOWER(report_opportunity_user_geo)           AS report_opportunity_user_geo,
        LOWER(report_opportunity_user_region)        AS report_opportunity_user_region,
        LOWER(report_opportunity_user_area)          AS report_opportunity_user_area,
        
        LOWER(sales_qualified_source)                AS sales_qualified_source,
        LOWER(order_type_stamped)                    AS order_type_stamped,
  
        LOWER(deal_category)                         AS deal_category,
        LOWER(deal_group)                            AS deal_group,

        LOWER(role_type)                             AS role_type,
        LOWER(partner_category)                      AS partner_category,
        LOWER(alliance_partner)                      AS alliance_partner,

        LOWER(CONCAT(report_opportunity_user_segment,'-',report_opportunity_user_geo,'-',report_opportunity_user_region,'-',report_opportunity_user_area))                                                          AS report_user_segment_geo_region_area,
        LOWER(CONCAT(report_opportunity_user_segment,'-',report_opportunity_user_geo,'-',report_opportunity_user_region,'-',report_opportunity_user_area,'-',sales_qualified_source,'-', order_type_stamped))       AS report_user_segment_geo_region_area_sqs_ot,

        LOWER(CONCAT(business_unit,'-',report_opportunity_user_segment,'-',report_opportunity_user_geo,'-',report_opportunity_user_region,'-',report_opportunity_user_area,'-',sales_qualified_source,'-',order_type_stamped,'-',role_type,'-',partner_category,'-',alliance_partner)) AS report_bu_user_segment_geo_region_area_sqs_ot_rt_pc_ap

  FROM field_for_keys
  
  UNION ALL
  
  SELECT
        LOWER(business_unit)                         AS business_unit,
        LOWER(account_owner_user_segment)            AS report_opportunity_user_segment,
        LOWER(account_owner_user_geo)                AS report_opportunity_user_geo,
        LOWER(account_owner_user_region)             AS report_opportunity_user_region,
        LOWER(account_owner_user_area)               AS report_opportunity_user_area,
        
        LOWER(sales_qualified_source)                AS sales_qualified_source,
        LOWER(order_type_stamped)                    AS order_type_stamped,
  
        LOWER(deal_category)                         AS deal_category,
        LOWER(deal_group)                            AS deal_group,

        LOWER(role_type)                             AS role_type,
        LOWER(partner_category)                      AS partner_category,
        LOWER(alliance_partner)                      AS alliance_partner,


        LOWER(CONCAT(account_owner_user_segment,'-',account_owner_user_geo,'-',account_owner_user_region,'-',account_owner_user_area))                                                          AS report_user_segment_geo_region_area,
        LOWER(CONCAT(account_owner_user_segment,'-',account_owner_user_geo,'-',account_owner_user_region,'-',account_owner_user_area, '-', sales_qualified_source, '-', order_type_stamped))    AS report_user_segment_geo_region_area_sqs_ot,

        LOWER(CONCAT(business_unit,'-',account_owner_user_segment,'-',account_owner_user_geo,'-',account_owner_user_region,'-',account_owner_user_area,'-',sales_qualified_source,'-',order_type_stamped,'-',role_type,'-',partner_category,'-',alliance_partner)) AS report_bu_user_segment_geo_region_area_sqs_ot_rt_pc_ap


  FROM field_for_keys


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

        business_unit AS key_bu,
        report_opportunity_user_segment   AS key_segment,
        sales_qualified_source            AS key_sqs,
        deal_group                        AS key_ot,
        partner_category AS key_pc,
        alliance_partner AS key_ap,

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


        -- FY24 structure keys (pending)
        business_unit || '_' || report_opportunity_user_geo AS key_bu_geo,
        partner_category || '_' || business_unit AS key_pc_bu,
        alliance_partner || '_' || business_unit AS key_ap_bu,

        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region AS key_bu_geo_region,
        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment AS key_bu_geo_segment,
        business_unit || '_' || report_opportunity_user_geo || '_' || role_type AS key_bu_geo_rt,
        partner_category || '_' || business_unit || '_' || report_opportunity_user_geo AS key_pc_bu_geo,
        alliance_partner || '_' || business_unit || '_' || report_opportunity_user_geo AS key_ap_bu_geo,

        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area AS key_bu_geo_region_area,
        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || role_type AS key_bu_geo_region_rt,
        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || report_opportunity_user_region AS key_bu_geo_segment_region,
        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || role_type AS key_bu_geo_segment_rt,

        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || report_opportunity_user_segment AS key_bu_geo_region_area_segment,
        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || role_type AS key_bu_geo_region_area_rt,
        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area AS key_bu_geo_segment_region_area,
        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || report_opportunity_user_region || '_' || role_type AS key_bu_geo_segment_region_rt,

        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || report_opportunity_user_segment || '_' || role_type AS key_bu_geo_region_area_segment_rt,
        business_unit || '_' || report_opportunity_user_geo || '_' || report_opportunity_user_segment || '_' || report_opportunity_user_region || '_' || report_opportunity_user_area || '_' || role_type AS key_bu_geo_segment_region_area_rt,


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