{{ config({
    "materialized": "incremental",
    })
}}

WITH source AS (

  SELECT *
  FROM {{ ref('google_analytics_4_events_source') }}

),

flattened AS (

  SELECT
    date_part_nodash,
    device_category,
    device_is_limited_ad_tracking,
    device_language,
    device_operating_system_version,
    device_web_info_browser,
    device_web_info_browser_version,
    device_web_info_hostname,
    event_bundle_sequence_id,
    event_date,
    event_name,
    preparation.parse_bigquery_object_array(variant__event_params)                                        AS variant__event_params,
    preparation.parse_bigquery_object_array(variant__event_params)['batch_ordering_id']                   AS batch_ordering_id,
    preparation.parse_bigquery_object_array(variant__event_params)['batch_page_id']                       AS batch_page_id,
    preparation.parse_bigquery_object_array(variant__event_params)['campaign']                            AS campaign,
    preparation.parse_bigquery_object_array(variant__event_params)['engaged_session_event']               AS engaged_session_event,
    preparation.parse_bigquery_object_array(variant__event_params)['engagement_time_msec']                AS engagement_time_msec,
    preparation.parse_bigquery_object_array(variant__event_params)['ga_session_id']                       AS ga_session_id,
    preparation.parse_bigquery_object_array(variant__event_params)['ga_session_number']                   AS ga_session_number,
    preparation.parse_bigquery_object_array(variant__event_params)['medium']                              AS medium,
    preparation.parse_bigquery_object_array(variant__event_params)['page_exclude_localization']           AS page_exclude_localization,
    preparation.parse_bigquery_object_array(variant__event_params)['page_location']                       AS page_location,
    preparation.parse_bigquery_object_array(variant__event_params)['page_referrer']                       AS page_referrer,
    preparation.parse_bigquery_object_array(variant__event_params)['page_title']                          AS page_title,
    preparation.parse_bigquery_object_array(variant__event_params)['session_engaged']                     AS session_engaged,
    preparation.parse_bigquery_object_array(variant__event_params)['source']                              AS source,
    preparation.parse_bigquery_object_array(variant__event_params)['term']                                AS term,
    event_timestamp,
    gcs_export_time,
    geo_city,
    geo_continent,
    geo_country,
    geo_metro,
    geo_region,
    geo_sub_continent,
    is_active_user,
    variant__items,
    platform,
    variant__privacy_info,
    stream_id,
    traffic_source_medium,
    traffic_source_name,
    traffic_source_source,
    user_first_touch_timestamp,
    user_ltv_currency,
    user_ltv_revenue,
    preparation.parse_bigquery_object_array(variant__user_properties)                                     AS variant__user_properties,
    preparation.parse_bigquery_object_array(variant__user_properties)['ssense_employee_range']::VARCHAR   AS ssense_employee_range,
    preparation.parse_bigquery_object_array(variant__user_properties)['ssense_confidence']::VARCHAR       AS ssense_confidence,
    preparation.parse_bigquery_object_array(variant__user_properties)['ssense_country']::VARCHAR          AS ssense_country,
    preparation.parse_bigquery_object_array(variant__user_properties)['ssense_blacklisted']::VARCHAR      AS ssense_blacklisted,
    preparation.parse_bigquery_object_array(variant__user_properties)['ssense_sales_segment']::VARCHAR    AS ssense_sales_segment,
    preparation.parse_bigquery_object_array(variant__user_properties)['ssense_company']::VARCHAR          AS ssense_company,
    preparation.parse_bigquery_object_array(variant__user_properties)['ssense_industry']::VARCHAR         AS ssense_industry,
    preparation.parse_bigquery_object_array(variant__user_properties)['ssense_revenue_range']::VARCHAR    AS ssense_revenue_range,
    preparation.parse_bigquery_object_array(variant__user_properties)['browser_width_height']::VARCHAR    AS browser_width_height,
    user_pseudo_id
    date_part
    
  FROM source

  {% if is_incremental() %}

    WHERE date_part >= (SELECT MAX(date_part) FROM {{ this }})

  {% endif %}

)

SELECT *
FROM flattened