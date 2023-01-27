{{ simple_cte([
    ('sfdc_user_roles_source','sfdc_user_roles_source'),
    ('dim_date','dim_date'),
    ('sfdc_users_source', 'sfdc_users_source'),
    ('fy24_mock_crm_user_snapshot_source', 'fy24_mock_crm_user_snapshot_source'),
]) }}

, sheetload_mapping_sdr_sfdc_bamboohr_source AS (

    SELECT *
    FROM {{ ref('sheetload_mapping_sdr_sfdc_bamboohr_source') }}

), sfdc_users AS (

    SELECT 
      *,
      NULL AS user_business_unit
    FROM sfdc_users_source

    UNION ALL

    SELECT
      user_id,
      name,
      user_email,
      employee_number,
      title,
      team,
      department,
      manager_id,
      manager_name,
      is_active,
      user_role_id,
      user_role_type,
      start_date,
      ramping_quota,
      user_segment,
      user_geo,
      user_region,
      user_area,
      user_segment_geo_region_area,
      user_segment_grouped,
      user_segment_region_grouped,
      created_by_id,
      created_date,
      last_modified_id,
      last_modified_date,
      systemmodstamp,
      _last_dbt_run,
      user_business_unit
    FROM fy24_mock_crm_user_snapshot_source

), final AS (

    SELECT
      sfdc_users.user_id                                                                                                              AS dim_crm_user_id,
      sfdc_users.employee_number,
      sfdc_users.name                                                                                                                 AS user_name,
      sfdc_users.title,
      sfdc_users.department,
      sfdc_users.team,
      sfdc_users.manager_id,
      sfdc_users.manager_name,
      sfdc_users.user_email,
      sfdc_users.is_active,
      sfdc_users.start_date,
      sfdc_users.ramping_quota,
      sfdc_users.user_role_id,
      sfdc_users.user_role_type,
      sfdc_user_roles_source.name                                                                                                     AS user_role_name,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_segment']) }}                                                                      AS dim_crm_user_sales_segment_id,
      sfdc_users.user_segment                                                                                                         AS crm_user_sales_segment,
      sfdc_users.user_segment_grouped                                                                                                 AS crm_user_sales_segment_grouped,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_geo']) }}                                                                          AS dim_crm_user_geo_id,
      sfdc_users.user_geo                                                                                                             AS crm_user_geo,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_region']) }}                                                                       AS dim_crm_user_region_id,
      sfdc_users.user_region                                                                                                          AS crm_user_region,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_area']) }}                                                                         AS dim_crm_user_area_id,
      sfdc_users.user_area                                                                                                            AS crm_user_area,
      sfdc_users.user_business_unit                                                                                                   AS crm_user_business_unit,
      {{ dbt_utils.surrogate_key(['sfdc_users.user_business_unit']) }}                                                                AS dim_crm_user_business_unit_id,
      COALESCE(
               sfdc_users.user_segment_geo_region_area,
               CONCAT(sfdc_users.user_segment,'-' , sfdc_users.user_geo, '-', sfdc_users.user_region, '-', sfdc_users.user_area)
               )                                                                                                                      AS crm_user_sales_segment_geo_region_area,
      sfdc_users.user_segment_region_grouped                                                                                          AS crm_user_sales_segment_region_grouped,
      sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_segment                                                                          AS sdr_sales_segment,
      sheetload_mapping_sdr_sfdc_bamboohr_source.sdr_region,
      sfdc_users.created_date,
      CASE
        WHEN LOWER(sfdc_users.user_business_unit) = 'comm'
          THEN CONCAT(sfdc_users.user_business_unit, 
                      '-',
                      sfdc_users.user_geo, 
                      '-',
                      sfdc_users.user_region, 
                      '-',
                      sfdc_users.user_segment, 
                      '-',
                      sfdc_users.user_area
                      )
        WHEN LOWER(sfdc_users.user_business_unit) = 'entg'
          THEN CONCAT(sfdc_users.user_business_unit, 
                      '-',
                      sfdc_users.user_geo, 
                      '-',
                      sfdc_users.user_region, 
                      '-',
                      sfdc_users.user_area, 
                      '-',
                      sfdc_users.user_segment
                      )
          ELSE CONCAT(sfdc_users.user_segment, 
                      '-',
                      sfdc_users.user_geo, 
                      '-',
                      sfdc_users.user_region, 
                      '-',
                      sfdc_users.user_area
                      )
        END                                                                                                                           AS dim_crm_user_hierarchy_sk
    FROM sfdc_users
    LEFT JOIN sfdc_user_roles_source
      ON sfdc_users.user_role_id = sfdc_user_roles_source.id
    LEFT JOIN sheetload_mapping_sdr_sfdc_bamboohr_source
      ON sfdc_users.user_id = sheetload_mapping_sdr_sfdc_bamboohr_source.user_id

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-01-20",
    updated_date="2023-01-20"
  ) }}
