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
    FROM {{ref('wk_sales_mart_crm_opportunity')}}

), field_for_keys AS (
    
    SELECT
        -- NF: 20230223 FY24 GTM fields, precalculated in the user object
        account_owner_user_segment,
        account_owner_raw_user_segment,
        account_owner_user_geo,
        account_owner_user_region,
        account_owner_user_area,

        account_owner_user_business_unit,
        account_owner_user_sub_business_unit,
        account_owner_user_division,
        account_owner_user_asm,
        account_owner_user_role_type,

        -- JK 2023-02-07: these will be added to mart_crm_opportunity
        -- https://gitlab.com/gitlab-com/sales-team/field-operations/analytics/-/issues/418
        partner_category,
        alliance_partner,

        report_opportunity_user_segment,
        report_opportunity_raw_user_segment,
        report_opportunity_user_geo,
        report_opportunity_user_region,
        report_opportunity_user_area,

        report_opportunity_user_business_unit,
        report_opportunity_user_sub_business_unit,
        report_opportunity_user_division,
        report_opportunity_user_asm,
        report_opportunity_user_role_type,

        order_type AS order_type_stamped,
        sales_qualified_source_name AS sales_qualified_source,
        deal_category,
        deal_group

    FROM mart_crm_opportunity


    ), eligible AS (

    SELECT
        LOWER(report_opportunity_user_business_unit)      AS business_unit,
        LOWER(report_opportunity_user_sub_business_unit)  AS sub_business_unit,
        LOWER(report_opportunity_user_division)           AS division,
        LOWER(report_opportunity_user_asm)              AS asm,
        LOWER(report_opportunity_user_segment)          AS report_opportunity_user_segment,
        LOWER(report_opportunity_user_geo)              AS report_opportunity_user_geo,
        LOWER(report_opportunity_user_region)           AS report_opportunity_user_region,
        LOWER(report_opportunity_user_area)             AS report_opportunity_user_area,
        
        LOWER(sales_qualified_source)                   AS sales_qualified_source,
        LOWER(order_type_stamped)                       AS order_type_stamped,

        LOWER(deal_category)                            AS deal_category,
        LOWER(deal_group)                               AS deal_group,

        LOWER(report_opportunity_user_role_type)        AS role_type,
        LOWER(partner_category)                         AS partner_category,
        LOWER(alliance_partner)                         AS alliance_partner,

        LOWER(CONCAT(report_opportunity_user_segment,'-',report_opportunity_user_geo,'-',report_opportunity_user_region,'-',report_opportunity_user_area))                                                       AS report_user_segment_geo_region_area,
        LOWER(CONCAT(report_opportunity_user_segment,'-',report_opportunity_user_geo,'-',report_opportunity_user_region,'-',report_opportunity_user_area,'-',sales_qualified_source,'-', order_type_stamped))    AS report_user_segment_geo_region_area_sqs_ot,

        LOWER(
            CONCAT(
            report_opportunity_user_business_unit,
            '-',report_opportunity_raw_user_segment,
            '-',report_opportunity_user_geo,
            '-',report_opportunity_user_region,
            '-',report_opportunity_user_area,
            '-',sales_qualified_source,
            '-',order_type_stamped
            )
        ) AS report_bu_user_segment_geo_region_area_sqs_ot,

        LOWER(
            CONCAT(
            report_opportunity_user_business_unit,
            '-',report_opportunity_raw_user_segment,
            '-',report_opportunity_user_geo,
            '-',report_opportunity_user_region,
            '-',report_opportunity_user_area,
            '-',sales_qualified_source,
            '-',order_type_stamped,
            '-',report_opportunity_user_role_type,
            '-',partner_category,
            '-',alliance_partner
            )
        ) AS report_bu_user_segment_geo_region_area_sqs_ot_rt_pc_ap,

        LOWER(
            CONCAT(
            report_opportunity_user_business_unit,
            '-',report_opportunity_user_sub_business_unit,
            '-',report_opportunity_user_division,
            '-',report_opportunity_user_asm,
            '-',report_opportunity_raw_user_segment,
            '-',report_opportunity_user_geo,
            '-',report_opportunity_user_region,
            '-',report_opportunity_user_area,
            '-',sales_qualified_source,
            '-',order_type_stamped,
            '-',report_opportunity_user_role_type,
            '-',partner_category,
            '-',alliance_partner
            )
            ) AS report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap

    FROM field_for_keys

    UNION ALL

    SELECT
        LOWER(account_owner_user_business_unit)      AS business_unit,
        LOWER(account_owner_user_sub_business_unit)  AS sub_business_unit,
        LOWER(account_owner_user_division)           AS division,
        LOWER(account_owner_user_asm)                AS asm,
        LOWER(account_owner_user_segment)            AS report_opportunity_user_segment,
        LOWER(account_owner_user_geo)                AS report_opportunity_user_geo,
        LOWER(account_owner_user_region)             AS report_opportunity_user_region,
        LOWER(account_owner_user_area)               AS report_opportunity_user_area,
        
        LOWER(sales_qualified_source)                AS sales_qualified_source,
        LOWER(order_type_stamped)                    AS order_type_stamped,

        LOWER(deal_category)                         AS deal_category,
        LOWER(deal_group)                            AS deal_group,

        LOWER(account_owner_user_role_type)          AS role_type,
        LOWER(partner_category)                      AS partner_category,
        LOWER(alliance_partner)                      AS alliance_partner,

        LOWER(CONCAT(account_owner_user_segment,'-',account_owner_user_geo,'-',account_owner_user_region,'-',account_owner_user_area))                                                          AS report_user_segment_geo_region_area,
        LOWER(CONCAT(account_owner_user_segment,'-',account_owner_user_geo,'-',account_owner_user_region,'-',account_owner_user_area, '-', sales_qualified_source, '-', order_type_stamped))    AS report_user_segment_geo_region_area_sqs_ot,

        LOWER(
            CONCAT(
            account_owner_user_business_unit,
            '-',account_owner_raw_user_segment,
            '-',account_owner_user_geo,
            '-',account_owner_user_region,
            '-',account_owner_user_area,
            '-',sales_qualified_source,
            '-',order_type_stamped
            )
            ) AS report_bu_user_segment_geo_region_area_sqs_ot,

        LOWER(
            CONCAT(
            account_owner_user_business_unit,
            '-',account_owner_raw_user_segment,
            '-',account_owner_user_geo,
            '-',account_owner_user_region,
            '-',account_owner_user_area,
            '-',sales_qualified_source,
            '-',order_type_stamped,
            '-',account_owner_user_role_type,
            '-',partner_category,
            '-',alliance_partner
            )
        ) AS report_bu_user_segment_geo_region_area_sqs_ot_rt_pc_ap,

        LOWER(
            CONCAT(
            account_owner_user_business_unit,
            '-',account_owner_user_sub_business_unit,
            '-',account_owner_user_division,
            '-',account_owner_user_asm,
            '-',account_owner_raw_user_segment,
            '-',account_owner_user_geo,
            '-',account_owner_user_region,
            '-',account_owner_user_area,
            '-',sales_qualified_source,
            '-',order_type_stamped,
            '-',account_owner_user_role_type,
            '-',partner_category,
            '-',alliance_partner
            )
            ) AS report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap

  FROM field_for_keys

), valid_keys AS (

  SELECT DISTINCT 

        eligible.*,

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
        business_unit AS key_bu,
        business_unit || '_' || sub_business_unit AS key_bu_subbu,
        business_unit || '_' || sub_business_unit || '_' || division AS key_bu_subbu_division,
        business_unit || '_' || sub_business_unit || '_' || division || '_' || asm AS key_bu_subbu_division_asm,


        -- FY22 keys
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