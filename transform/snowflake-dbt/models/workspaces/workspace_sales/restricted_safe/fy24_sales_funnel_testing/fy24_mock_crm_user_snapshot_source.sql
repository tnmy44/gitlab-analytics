WITH source AS (

    SELECT *
    FROM {{ ref('fy24_mock_crm_users') }}

), renamed AS(

    SELECT 

      --ids
      user_id::VARCHAR AS user_id,
      name::VARCHAR AS name,
      user_email::VARCHAR AS user_email,
      employee_number::VARCHAR AS employee_number,

      -- info
      title::VARCHAR AS title,
      team::VARCHAR AS team,
      department::VARCHAR AS department,
      manager_id::VARCHAR AS manager_id,
      manager_name::VARCHAR AS manager_name,
      is_active::VARCHAR AS is_active,
      user_role_id::VARCHAR AS user_role_id,
      user_role_type::VARCHAR AS user_role_type,
      start_date::VARCHAR AS start_date,
      ramping_quota::VARCHAR AS ramping_quota,
      user_segment::VARCHAR AS user_segment,
      user_geo::VARCHAR AS user_geo,
      user_region::VARCHAR AS user_region,
      user_area::VARCHAR AS user_area,
      user_segment_geo_region_area::VARCHAR AS user_segment_geo_region_area,
      user_segment_grouped::VARCHAR AS user_segment_grouped,
      {{ sales_segment_region_grouped('user_segment', 'user_geo', 'user_region') }}
                                                                        AS user_segment_region_grouped,

      --metadata
      created_by_id::VARCHAR AS created_by_id,
      created_date::VARCHAR AS created_date,
      last_modified_id::VARCHAR AS last_modified_id,
      last_modified_date::VARCHAR AS last_modified_date,
      systemmodstamp::VARCHAR AS systemmodstamp,

      --dbt last run
      _last_dbt_run::VARCHAR AS _last_dbt_run,
      dbt_scd_id::VARCHAR AS dbt_scd_id,
      dbt_updated_at::VARCHAR AS dbt_updated_at,
      dbt_valid_from::VARCHAR AS dbt_valid_from,
      dbt_valid_to::VARCHAR AS dbt_valid_to,

      user_business_unit::VARCHAR AS user_business_unit

    FROM source

)

SELECT *
FROM renamed