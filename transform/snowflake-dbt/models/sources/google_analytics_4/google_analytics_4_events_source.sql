WITH source AS (

  SELECT *
  FROM {{ source('google_analytics_4_bigquery','events') }}

),

flattened AS (

  SELECT

    value['date_part']::NUMBER                                                                                                                                                        AS date_part_nodash,
    value['device']['category']::VARCHAR                                                                                                                                              AS device_category,
    value['device']['is_limited_ad_tracking']::VARCHAR                                                                                                                                AS device_is_limited_ad_tracking,
    value['device']['language']::VARCHAR                                                                                                                                              AS device_language,
    value['device']['operating_system_version']::VARCHAR                                                                                                                              AS device_operating_system_version,
    value['device']['web_info']['browser']::VARCHAR                                                                                                                                   AS device_web_info_browser,
    value['device']['web_info']['browser_version']::VARCHAR                                                                                                                           AS device_web_info_browser_version,
    value['device']['web_info']['hostname']::VARCHAR                                                                                                                                  AS device_web_info_hostname,
    value['event_bundle_sequence_id']::NUMBER                                                                                                                                         AS event_bundle_sequence_id,
    value['event_date']::NUMBER                                                                                                                                                       AS event_date,
    value['event_name']::VARCHAR                                                                                                                                                      AS event_name,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])::VARIANT                               AS variant__event_params,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['batch_ordering_id']::VARCHAR          AS batch_ordering_id,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['batch_page_id']::VARCHAR              AS batch_page_id,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['campaign']::VARCHAR                   AS campaign,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['engaged_session_event']::VARCHAR      AS engaged_session_event,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['engagement_time_msec']::VARCHAR       AS engagement_time_msec,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['ga_session_id']::VARCHAR              AS ga_session_id,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['ga_session_number']::VARCHAR          AS ga_session_number,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['medium']::VARCHAR                     AS medium,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['page_exclude_localization']::VARCHAR  AS page_exclude_localization,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['page_location']::VARCHAR              AS page_location,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['page_referrer']::VARCHAR              AS page_referrer,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['page_title']::VARCHAR                 AS page_title,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['session_engaged']::VARCHAR            AS session_engaged,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['source']::VARCHAR                     AS source,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['event_params'])['term']::VARCHAR                       AS term,
    TO_TIMESTAMP(value['event_timestamp']::VARCHAR)                                                                                                                                   AS event_timestamp,
    TO_TIMESTAMP(value['gcs_export_time']::VARCHAR)                                                                                                                                   AS gcs_export_time,
    value['geo']['city']::VARCHAR                                                                                                                                                     AS geo_city,
    value['geo']['continent']::VARCHAR                                                                                                                                                AS geo_continent,
    value['geo']['country']::VARCHAR                                                                                                                                                  AS geo_country,
    value['geo']['metro']::VARCHAR                                                                                                                                                    AS geo_metro,
    value['geo']['region']::VARCHAR                                                                                                                                                   AS geo_region,
    value['geo']['sub_continent']::VARCHAR                                                                                                                                            AS geo_sub_continent,
    value['is_active_user']::BOOLEAN                                                                                                                                                  AS is_active_user,
    value['items'][0]::VARIANT                                                                                                                                                        AS variant__items,
    value['platform']::VARCHAR                                                                                                                                                        AS platform,
    value['privacy_info']::VARIANT                                                                                                                                                    AS variant__privacy_info,
    value['stream_id']::NUMBER                                                                                                                                                        AS stream_id,
    value['traffic_source']['medium']::VARCHAR                                                                                                                                        AS traffic_source_medium,
    value['traffic_source']['name']::VARCHAR                                                                                                                                          AS traffic_source_name,
    value['traffic_source']['source']::VARCHAR                                                                                                                                        AS traffic_source_source,
    TO_TIMESTAMP(value['user_first_touch_timestamp']::VARCHAR)                                                                                                                        AS user_first_touch_timestamp,
    value['user_ltv']['currency']::VARCHAR                                                                                                                                            AS user_ltv_currency,
    value['user_ltv']['revenue']::VARCHAR                                                                                                                                             AS user_ltv_revenue,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])                                     AS variant__user_properties,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['ssense_employee_range']::VARCHAR   AS ssense_employee_range,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['ssense_confidence']::VARCHAR       AS ssense_confidence,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['ssense_country']::VARCHAR          AS ssense_country,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['ssense_blacklisted']::VARCHAR      AS ssense_blacklisted,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['ssense_sales_segment']::VARCHAR    AS ssense_sales_segment,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['ssense_company']::VARCHAR          AS ssense_company,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['ssense_industry']::VARCHAR         AS ssense_industry,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['ssense_revenue_range']::VARCHAR    AS ssense_revenue_range,
    {{generate_database_name(env_var('SNOWFLAKE_PREP_DATABASE'))}}.{{target.schema | trim}}.parse_bigquery_object_array(value['user_properties'])['browser_width_height']::VARCHAR    AS browser_width_height,    
    value['user_pseudo_id']::VARCHAR                                                                                                                                                  AS user_pseudo_id,
    date_part::DATE                                                                                                                                                                   AS date_part

  FROM source

  {% if is_incremental() %}

    WHERE date_part >= (SELECT MAX(date_part) FROM {{ this }})

  {% endif %}

)

SELECT *
FROM flattened