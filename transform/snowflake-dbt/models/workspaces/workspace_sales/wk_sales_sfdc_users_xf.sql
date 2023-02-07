{{ config(alias='sfdc_users_xf') }}

WITH RECURSIVE base AS (
    SELECT
      dim_crm_user_id           AS user_id,
      user_name                 AS name,
      department,
      title,
      team,
      CASE --only expose GitLab.com email addresses of internal employees
        WHEN user_email LIKE '%gitlab.com' THEN user_email ELSE NULL
      END                       AS user_email,
      manager_name,
      manager_id,
      crm_user_geo              AS user_geo,
      crm_user_region           AS user_region,
      crm_user_sales_segment    AS user_segment,
      crm_user_area             AS user_area,
      user_role_name            AS role_name,
      user_role_type            AS role_type,
      start_date,
      is_active,
      employee_number
    FROM {{ref('dim_crm_user')}}

), managers AS (
    SELECT
      user_id,
      name,
      role_name,
      manager_name,
      manager_id,
      0 AS level,
      '' AS path
    FROM base
    WHERE role_name = 'CRO'

    UNION ALL

    SELECT
      users.user_id,
      users.name,
      users.role_name,
      users.manager_name,
      users.manager_id,
      level + 1,
      path || COALESCE(managers.role_name,'') || '::'
    FROM base users
    INNER JOIN managers
      ON users.manager_id = managers.user_id

), cro_sfdc_hierarchy AS (
    SELECT
      user_id,
      name,
      role_name,
      manager_name,
      SPLIT_PART(path, '::', 1)::VARCHAR(50) AS level_1,
      SPLIT_PART(path, '::', 2)::VARCHAR(50) AS level_2,
      SPLIT_PART(path, '::', 3)::VARCHAR(50) AS level_3,
      SPLIT_PART(path, '::', 4)::VARCHAR(50) AS level_4,
      SPLIT_PART(path, '::', 5)::VARCHAR(50) AS level_5
    FROM managers

), consolidation AS (
    SELECT
      base.user_id,
      base.name,
      base.department,
      base.title,
      base.team,
      base.user_email,
      base.manager_name,
      base.manager_id,
      base.user_geo,
      base.user_region,
      base.user_segment,
      base.user_area,
      base.role_name,
      base.role_type,
      base.start_date,
      base.is_active,
      base.employee_number,

      -- account owner hierarchies levels
      TRIM(cro.level_2)                                                                               AS sales_team_level_2,
      TRIM(cro.level_3)                                                                               AS sales_team_level_3,
      TRIM(cro.level_4)                                                                               AS sales_team_level_4,
      CASE
          WHEN TRIM(cro.level_2) IS NOT NULL
              THEN TRIM(cro.level_2)
          ELSE 'n/a'
      END                                                                                             AS sales_team_vp_level,
      CASE
          WHEN (TRIM(cro.level_3) IS NOT NULL  AND TRIM(cro.level_3) != '')
              THEN TRIM(cro.level_3)
          ELSE 'n/a'
      END                                                                                             AS sales_team_rd_level,
      CASE
          WHEN cro.level_3 LIKE 'ASM%'
              THEN cro.level_3
          WHEN cro.level_4 LIKE 'ASM%' OR cro.level_4 LIKE 'Area Sales%'
              THEN cro.level_4
          ELSE 'n/a'
      END                                                                                             AS sales_team_asm_level,
      CASE
          WHEN (cro.level_4 IS NOT NULL
          AND cro.level_4 != ''
          AND (cro.level_4 LIKE 'ASM%' OR cro.level_4 LIKE 'Area Sales%') )
              THEN cro.level_4
          WHEN (cro.level_3 IS NOT NULL AND cro.level_3 != '')
              THEN cro.level_3
          WHEN (cro.level_2 IS NOT NULL AND cro.level_2 != '')
              THEN cro.level_2
          ELSE 'n/a'
      END                                                                                             AS sales_min_hierarchy_level,
      CASE
          WHEN sales_min_hierarchy_level IN ('ASM - APAC - Japan', 'RD APAC')
              THEN 'APAC'
          WHEN sales_min_hierarchy_level IN ('ASM - Civilian','ASM - DoD - USAF+COCOMS+4th Estate'
                                          , 'ASM - NSG', 'ASM - SLED', 'ASM-DOD- Army+Navy+Marines+SI''s'
                                          , 'CD PubSec', 'RD PubSec' )
              THEN 'PUBSEC'
          WHEN sales_min_hierarchy_level IN ('ASM - EMEA - DACH', 'ASM - EMEA - North', 'ASM - MM - EMEA'
                                          , 'ASM-SMB-EMEA', 'CD EMEA', 'RD EMEA')
              THEN 'EMEA'
          WHEN sales_min_hierarchy_level IN ('ASM - MM - East','ASM - US East - Southeast', 'ASM-SMB-AMER-East'
                                          , 'Area Sales Manager - US East - Central', 'Area Sales Manager - US East - Named Accounts'
                                          , 'Area Sales Manager - US East - Northeast', 'RD US East')
              THEN 'US East'
          WHEN sales_min_hierarchy_level IN ('ASM - MM - West','ASM - US West - NorCal','ASM - US West - PacNW'
                                          ,'ASM - US West - SoCal+Rockies', 'ASM-SMB-AMER-West','RD US West')
              THEN 'US West'
          ELSE 'n/a' END                                                                            AS sales_region,
      CASE
          WHEN cro.level_2 LIKE 'VP%'
              THEN 1
          ELSE 0
      END                                                                                           AS is_lvl_2_vp_flag,
      CASE
        WHEN LOWER(title) LIKE '%strategic account%'
           OR LOWER(title) LIKE '%account executive%'
           OR LOWER(title) LIKE '%country manager%'
           OR LOWER(title) LIKE '%public sector channel manager%'
        THEN 1
        ELSE 0
      END                                                                                          AS is_rep_flag

    FROM base
    LEFT JOIN cro_sfdc_hierarchy AS cro
        ON cro.user_id = base.user_id


), user_based_reporting_keys AS (
    SELECT
      consolidation.*,

      -- Business Unit (X-Ray 1st hierarchy)
      -- will be replaced with the actual field
      CASE user_segment
        WHEN 'large' THEN 'Ent-G'
        WHEN 'pubsec' THEN 'Ent-H'
        WHEN 'mid-market' THEN 'Comm'
        WHEN 'smb' THEN 'Comm'
        WHEN 'jihu' THEN 'JiHu'
        ELSE 'Other'
      END AS business_unit,

      -- Sub-Business Unit (X-Ray 2nd hierarchy)
      /*
      JK 2023-02-06: sub-BU is used in lower hierarchy fields calculation (division & asm).
      Therefore when making changes to the field, make sure to understand implications on the whole key hierarchy
      */
      CASE
        WHEN LOWER(business_unit) = 'ent-g'
          THEN user_geo

        WHEN
          LOWER(business_unit) = 'comm'
          AND
            (
            user_segment = 'smb' 
            AND user_geo = 'amer'
            AND user_area = 'lowtouch'
            ) 
          THEN 'AMER Low-Touch'
        WHEN
          LOWER(business_unit) = 'comm'
          AND
            (
            user_segment = 'mid-market'
            AND (user_geo = 'amer' OR user_geo = 'emea')
            AND LOWER(role_type) = 'first order'
            )
          THEN 'MM First Orders'  --mid-market FO(?)
        WHEN
          LOWER(business_unit) = 'comm'
          AND user_geo = 'emea'
          AND 
            (
            user_segment != 'mid-market'
            AND LOWER(role_type) != 'first order'
            )
          THEN  'EMEA'
        WHEN
          LOWER(business_unit) = 'comm'
          AND user_geo = 'amer'
          AND
            (
            user_segment != 'mid-market'
            AND LOWER(role_type) != 'first order'
            )
          AND
            (
            user_segment != 'smb'
            AND user_area != 'lowtouch'
            )
          THEN 'AMER'
        ELSE 'Other'
      END AS sub_business_unit,

      -- Division (X-Ray 3rd hierarchy)
      CASE 
        WHEN LOWER(business_unit) = 'ent-g'
          THEN user_region

        WHEN 
          LOWER(business_unit) = 'comm'
          AND (LOWER(sub_business_unit) = 'amer' OR LOWER(sub_business_unit) = 'emea')
          AND user_segment = 'mid-market'
          THEN 'Mid-Market'
        WHEN
          LOWER(business_unit) = 'comm'
          AND (LOWER(sub_business_unit) = 'amer' OR LOWER(sub_business_unit) = 'emea')
          AND user_segment = 'smb'
          THEN 'SMB'
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(sub_business_unit) = 'mm first orders'
          THEN 'MM First Orders'
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(sub_business_unit) = 'amer low-touch'
          THEN 'AMER Low-Touch'
        ELSE 'Other'
      END AS division,

      -- ASM (X-Ray 4th hierarchy): definition pending
      CASE
        WHEN 
          LOWER(business_unit) = 'ent-g'
          AND LOWER(sub_business_unit) = 'amer'
          THEN user_area

        WHEN 
          LOWER(business_unit) = 'ent-g'
          AND LOWER(sub_business_unit) = 'emea'
          AND (LOWER(division) = 'dach' OR LOWER(division) = 'neur' OR LOWER(division) = 'seur')
          THEN user_area
        WHEN
          LOWER(business_unit) = 'ent-g'
          AND LOWER(sub_business_unit) = 'emea'
          AND LOWER(division) = 'meta'
          THEN user_segment --- pending/ waiting for Meri?
        WHEN 
          LOWER(business_unit) = 'ent-g'
          AND (LOWER(sub_business_unit) = 'apac' OR LOWER(sub_business_unit) = 'pubsec')
          THEN user_area

        WHEN
          LOWER(business_unit) = 'comm'
          AND (LOWER(sub_business_unit) = 'amer' OR LOWER(sub_business_unit) = 'emea')
          THEN user_area
        WHEN 
          LOWER(business_unit) = 'comm'
          AND LOWER(sub_business_unit) = 'mm first orders'
          THEN user_geo
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(sub_business_unit) = 'amer low-touch'
          AND LOWER(role_type) = 'first order'
          THEN 'LowTouch FO'
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(sub_business_unit) = 'amer low-touch'
          AND LOWER(role_type) != 'first order'
          THEN 'LowTouch Pool'
        ELSE 'Other'
      END AS asm

    FROM consolidation

)


SELECT *
FROM user_based_reporting_keys
-- FROM consolidation