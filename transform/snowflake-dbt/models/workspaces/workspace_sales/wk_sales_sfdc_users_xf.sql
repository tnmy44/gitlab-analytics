{{ config(alias='sfdc_users_xf') }}

WITH base AS (
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
      IFNULL(crm_user_geo, 'Other')                    AS user_geo,
      IFNULL(crm_user_region, 'Other')                 AS user_region,
      IFNULL(crm_user_sales_segment, 'Other')          AS user_segment,
      IFNULL(crm_user_area, 'Other')                   AS user_area,
      IFNULL(user_role_name, 'Other')                  AS role_name,
      IFNULL(user_role_type, 'Other')                  AS role_type,
      start_date,
      is_active,
      employee_number
    FROM {{ref('dim_crm_user')}}

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
     
      CASE
        WHEN LOWER(title) LIKE '%strategic account%'
           OR LOWER(title) LIKE '%account executive%'
           OR LOWER(title) LIKE '%country manager%'
           OR LOWER(title) LIKE '%public sector channel manager%'
        THEN 1
        ELSE 0
      END                                                                                          AS is_rep_flag

    FROM base

), user_based_reporting_keys AS (
    SELECT
      consolidation.*,

      -- Business Unit (X-Ray 1st hierarchy)
      -- will be replaced with the actual field
      CASE LOWER(user_segment)
        WHEN 'large' THEN 'ENTG'
        WHEN 'pubsec' THEN 'ENTG'
        WHEN 'mid-market' THEN 'COMM'
        WHEN 'smb' THEN 'COMM'
        WHEN 'jihu' THEN 'JiHu'
        ELSE 'Other'
      END AS business_unit,

      -- Sub-Business Unit (X-Ray 2nd hierarchy)
      /*
      JK 2023-02-06: sub-BU is used in lower hierarchy fields calculation (division & asm).
      Therefore when making changes to the field, make sure to understand implications on the whole key hierarchy
      */
      CASE
        WHEN LOWER(business_unit) = 'entg'
          THEN user_geo

        WHEN
          LOWER(business_unit) = 'comm'
          AND
            (
            LOWER(user_segment) = 'smb'
            AND LOWER(user_geo) = 'amer'
            AND LOWER(user_area) = 'lowtouch'
            ) 
          THEN 'AMER Low-Touch'
        WHEN
          LOWER(business_unit) = 'comm'
          AND
            (
            LOWER(user_segment) = 'mid-market'
            AND (LOWER(user_geo) = 'amer' OR LOWER(user_geo) = 'emea')
            AND LOWER(role_type) = 'first order'
            )
          THEN 'MM First Orders'  --mid-market FO(?)
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(user_geo) = 'emea'
          AND 
            (
            LOWER(user_segment) != 'mid-market'
            AND LOWER(role_type) != 'first order'
            )
          THEN  'EMEA'
        WHEN
          LOWER(business_unit) = 'comm'
          AND LOWER(user_geo) = 'amer'
          AND
            (
            LOWER(user_segment) != 'mid-market'
            AND LOWER(role_type) != 'first order'
            )
          AND
            (
            LOWER(user_segment) != 'smb'
            AND LOWER(user_area) != 'lowtouch'
            )
          THEN 'AMER'
        ELSE 'Other'
      END AS sub_business_unit,

      -- Division (X-Ray 3rd hierarchy)
      CASE 
        WHEN LOWER(business_unit) = 'entg'
          THEN user_region

        WHEN 
          LOWER(business_unit) = 'comm'
          AND (LOWER(sub_business_unit) = 'amer' OR LOWER(sub_business_unit) = 'emea')
          AND LOWER(user_segment) = 'mid-market'
          THEN 'Mid-Market'
        WHEN
          LOWER(business_unit) = 'comm'
          AND (LOWER(sub_business_unit) = 'amer' OR LOWER(sub_business_unit) = 'emea')
          AND LOWER(user_segment) = 'smb'
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
          LOWER(business_unit) = 'entg'
          AND LOWER(sub_business_unit) = 'amer'
          THEN user_area
        WHEN 
          LOWER(business_unit) = 'entg'
          AND LOWER(sub_business_unit) = 'emea'
          AND (LOWER(division) = 'dach' OR LOWER(division) = 'neur' OR LOWER(division) = 'seur')
          THEN user_area
        WHEN
          LOWER(business_unit) = 'entg'
          AND LOWER(sub_business_unit) = 'emea'
          AND LOWER(division) = 'meta'
          THEN user_segment --- pending/ waiting for Meri?
        WHEN 
          LOWER(business_unit) = 'entg'
          AND LOWER(sub_business_unit) = 'apac'
          THEN user_area
        WHEN
          LOWER(business_unit) = 'entg'
          AND LOWER(sub_business_unit) = 'pubsec'
          AND LOWER(division) != 'sled'
          THEN user_area
        WHEN
          LOWER(business_unit) = 'entg'
          AND LOWER(sub_business_unit) = 'pubsec'
          AND LOWER(division) = 'sled'
          THEN user_region

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