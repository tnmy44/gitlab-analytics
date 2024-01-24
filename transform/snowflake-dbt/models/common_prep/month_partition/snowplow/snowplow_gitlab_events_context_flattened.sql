{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(1)).strftime('%Y')) | int %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(1)).strftime('%m')) | int %}
{% set start_date = modules.datetime.datetime(year_value, month_value, 1) %}
{% set end_date = (start_date + modules.datetime.timedelta(days=31)).strftime('%Y-%m-01') %}

{{config({
    "unique_key":"event_id",
    "cluster_by":['derived_tstamp_date']
  })
}}

WITH filtered_source as (

    SELECT
        event_id,
        derived_tstamp::DATE AS derived_tstamp_date,
        contexts
    {% if target.name not in ("prod") -%}

    FROM {{ ref('snowplow_gitlab_good_events_sample_source') }}

    {%- else %}

    FROM {{ ref('snowplow_gitlab_good_events_source') }}

    {%- endif %}

    WHERE TRY_TO_TIMESTAMP(derived_tstamp) IS NOT NULL
      AND derived_tstamp >= '{{ start_date }}'
      AND derived_tstamp < '{{ end_date }}'
)

, base AS (
  
    SELECT DISTINCT * 
    FROM filtered_source

), events_with_context_flattened AS (

    SELECT 
      base.*,
      f.value['schema']::VARCHAR                                                                                                                              AS context_data_schema,
      f.value['data']                                                                                                                                         AS context_data, 
      -- GitLab Standard Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_standard/jsonschema/%',
          context_name='gitlab_standard',
          field_alias_datatype_list=[
            {'field':'environment'},
            {'field':'extra', 'formula':"TRY_PARSE_JSON(context_data['extra'])", 'data_type':'variant'},
            {'field':'namespace_id', 'data_type':'number'},
            {'field':'plan'},
            {'field':'google_analytics_id'},
            {'field':'project_id', 'data_type':'number'},
            {'field':'pseudonymized_user_id'},
            {'field':'source'}
            ]
        )
      }},
      IFF(google_analytics_id = '', NULL,
          SPLIT_PART(google_analytics_id, '.', 3) || '.' ||
          SPLIT_PART(google_analytics_id, '.', 4))::VARCHAR                                                                                                   AS google_analytics_client_id,

      -- Web Page Context Columns
       {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0',
          context_name='web_page',
          field_alias_datatype_list=[
            {'field':'id', 'alias':'web_page_id'}
            ]
        )
      }},

      -- GitLab Experiment Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_experiment/jsonschema/%',
          context_name='gitlab_experiment',
          field_alias_datatype_list=[
            {'field':'experiment', 'alias':'experiment_name'},
            {'field':'key', 'alias':'experiment_context_key'},
            {'field':'variant', 'alias':'experiment_variant'},
            {'field':'migration_keys', 'formula':"ARRAY_TO_STRING(context_data['migration_keys']::VARIANT, ', ')", 'alias':'experiment_migration_keys'}
            ]
        )
      }},

      -- Code Suggestions Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/code_suggestions_context/jsonschema/%',
          context_name='code_suggestions',
          field_alias_datatype_list=[
            {'field':'model_engine'}, 
            {'field':'model_name'}, 
            {'field':'prefix_length', 'data_type':'int'},
            {'field':'suffix_length', 'data_type':'int'},
            {'field':'language'},
            {'field':'user_agent'},
            {'field':'gitlab_realm'},
            {'field':'api_status_code', 'data_type':'int'},
            {'field':'gitlab_saas_namespace_ids', 'alias':'namespace_ids'},
            {'field':'gitlab_instance_id', 'alias':'instance_id'},
            {'field':'gitlab_host_name', 'alias':'host_name'}
            ]
        )
      }},
      CASE
        WHEN gitlab_realm IN ('SaaS','saas') 
          THEN 'SaaS'
        WHEN gitlab_realm IN ('Self-Managed','self-managed') 
          THEN 'Self-Managed'
        WHEN gitlab_realm IS NULL 
          THEN NULL
        ELSE gitlab_realm
      END                                                                                                                                                     AS delivery_type,

      -- IDE Extension Version Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/ide_extension_version/jsonschema/%',
          context_name='ide_extension_version',
          field_alias_datatype_list=[
            {'field':'extension_name'},
            {'field':'extension_version'},
            {'field':'ide_name'}, 
            {'field':'ide_vendor'}, 
            {'field':'ide_version'},
            {'field':'language_server_version'}
            ]
        )
      }},

      -- Service Ping Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_service_ping/jsonschema/%',
          context_name='gitlab_service_ping',
          field_alias_datatype_list=[
            {'field':'event_name', 'alias':'redis_event_name'},
            {'field':'key_path'}, 
            {'field':'data_source'}
            ]
        )
      }},

      -- Performance Timing Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:org.w3/PerformanceTiming/jsonschema/1-0-0',
          context_name='performance_timing',
          field_alias_datatype_list=[
            {'field':'connectEnd', 'alias':'connect_end'},
            {'field':'connectStart', 'alias':'connect_start'},
            {'field':'domComplete', 'alias':'dom_complete'},
            {'field':'domContentLoadedEventEnd', 'alias':'dom_content_loaded_event_end'},
            {'field':'domContentLoadedEventStart', 'alias':'dom_content_loaded_event_start'},
            {'field':'domInteractive', 'alias':'dom_interactive'},
            {'field':'domLoading', 'alias':'dom_loading'},
            {'field':'domainLookupEnd', 'alias':'domain_lookup_end'},
            {'field':'domainLookupStart', 'alias':'domain_lookup_start'},
            {'field':'fetchStart', 'alias':'fetch_start'},
            {'field':'loadEventEnd', 'alias':'load_event_end'},
            {'field':'loadEventStart', 'alias':'load_event_start'},
            {'field':'navigationStart', 'alias':'navigation_start'},
            {'field':'redirectEnd', 'alias':'redirect_end'},
            {'field':'redirectStart', 'alias':'redirect_start'},
            {'field':'requestStart', 'alias':'request_start'},
            {'field':'responseEnd', 'alias':'response_end'},
            {'field':'responseStart', 'alias':'response_start'},
            {'field':'secureConnectionStart', 'alias':'secure_connection_start'},
            {'field':'unloadEventEnd', 'alias':'unload_event_end'},
            {'field':'unloadEventStart', 'alias':'unload_event_start'}
            ]
        )
      }}      

    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

)

SELECT 
   
  events_with_context_flattened.event_id,
  events_with_context_flattened.derived_tstamp_date,

  MODE(events_with_context_flattened.gitlab_standard_context)               AS gitlab_standard_context,
  MODE(events_with_context_flattened.gitlab_standard_context_schema)        AS gitlab_standard_context_schema,
  MODE(events_with_context_flattened.has_gitlab_standard_context)           AS has_gitlab_standard_context,
  MODE(events_with_context_flattened.environment)                           AS environment,
  MODE(events_with_context_flattened.extra)                                 AS extra,
  MODE(events_with_context_flattened.namespace_id)                          AS namespace_id,
  MODE(events_with_context_flattened.plan)                                  AS plan,
  MODE(events_with_context_flattened.google_analytics_client_id)            AS google_analytics_client_id,
  MODE(events_with_context_flattened.project_id)                            AS project_id,
  MODE(events_with_context_flattened.pseudonymized_user_id)                 AS pseudonymized_user_id,
  MODE(events_with_context_flattened.source)                                AS source,

  MODE(events_with_context_flattened.web_page_context)                      AS web_page_context,
  MODE(events_with_context_flattened.web_page_context_schema)               AS web_page_context_schema,
  MODE(events_with_context_flattened.has_web_page_context)                  AS has_web_page_context,
  MODE(events_with_context_flattened.web_page_id)                           AS web_page_id,

  MODE(events_with_context_flattened.gitlab_experiment_context)             AS gitlab_experiment_context,
  MODE(events_with_context_flattened.gitlab_experiment_context_schema)      AS gitlab_experiment_context_schema,
  MODE(events_with_context_flattened.has_gitlab_experiment_context)         AS has_gitlab_experiment_context,
  MODE(events_with_context_flattened.experiment_name)                       AS experiment_name,
  MODE(events_with_context_flattened.experiment_context_key)                AS experiment_context_key,
  MODE(events_with_context_flattened.experiment_variant)                    AS experiment_variant,
  MODE(events_with_context_flattened.experiment_migration_keys)             AS experiment_migration_keys,

  MODE(events_with_context_flattened.code_suggestions_context)              AS code_suggestions_context,
  MODE(events_with_context_flattened.code_suggestions_context_schema)       AS code_suggestions_context_schema,
  MODE(events_with_context_flattened.has_code_suggestions_context)          AS has_code_suggestions_context,
  MODE(events_with_context_flattened.model_engine)                          AS model_engine,
  MODE(events_with_context_flattened.model_name)                            AS model_name,
  MODE(events_with_context_flattened.prefix_length)                         AS prefix_length,
  MODE(events_with_context_flattened.suffix_length)                         AS suffix_length,
  MODE(events_with_context_flattened.language)                              AS language,
  MODE(events_with_context_flattened.user_agent)                            AS user_agent,
  MODE(events_with_context_flattened.delivery_type)                         AS delivery_type,
  MODE(events_with_context_flattened.api_status_code)                       AS api_status_code,
  MODE(events_with_context_flattened.namespace_ids)                         AS namespace_ids,
  MODE(events_with_context_flattened.instance_id)                           AS instance_id,
  MODE(events_with_context_flattened.host_name)                             AS host_name,


  MODE(events_with_context_flattened.ide_extension_version_context)         AS ide_extension_version_context,
  MODE(events_with_context_flattened.ide_extension_version_context_schema)  AS ide_extension_version_context_schema,
  MODE(events_with_context_flattened.has_ide_extension_version_context)     AS has_ide_extension_version_context,
  MODE(events_with_context_flattened.extension_name)                        AS extension_name,
  MODE(events_with_context_flattened.extension_version)                     AS extension_version,
  MODE(events_with_context_flattened.ide_name)                              AS ide_name,
  MODE(events_with_context_flattened.ide_vendor)                            AS ide_vendor,
  MODE(events_with_context_flattened.ide_version)                           AS ide_version,
  MODE(events_with_context_flattened.language_server_version)               AS language_server_version,

  MODE(events_with_context_flattened.gitlab_service_ping_context)           AS gitlab_service_ping_context,
  MODE(events_with_context_flattened.gitlab_service_ping_context_schema)    AS gitlab_service_ping_context_schema,
  MODE(events_with_context_flattened.has_gitlab_service_ping_context)       AS has_gitlab_service_ping_context,
  MODE(events_with_context_flattened.redis_event_name)                      AS redis_event_name,
  MODE(events_with_context_flattened.key_path)                              AS key_path,
  MODE(events_with_context_flattened.data_source)                           AS data_source,

  MODE(events_with_context_flattened.performance_timing_context)            AS performance_timing_context,
  MODE(events_with_context_flattened.performance_timing_context_schema)     AS performance_timing_context_schema,
  MODE(events_with_context_flattened.has_performance_timing_context)        AS has_performance_timing_context,
  MODE(events_with_context_flattened.connect_end)                           AS connect_end,
  MODE(events_with_context_flattened.connect_start)                         AS connect_start,
  MODE(events_with_context_flattened.dom_complete)                          AS dom_complete,
  MODE(events_with_context_flattened.dom_content_loaded_event_end)          AS dom_content_loaded_event_end,
  MODE(events_with_context_flattened.dom_content_loaded_event_start)        AS dom_content_loaded_event_start,
  MODE(events_with_context_flattened.dom_interactive)                       AS dom_interactive,
  MODE(events_with_context_flattened.dom_loading)                           AS dom_loading,
  MODE(events_with_context_flattened.domain_lookup_end)                     AS domain_lookup_end,
  MODE(events_with_context_flattened.domain_lookup_start)                   AS domain_lookup_start,
  MODE(events_with_context_flattened.fetch_start)                           AS fetch_start,
  MODE(events_with_context_flattened.load_event_end)                        AS load_event_end,
  MODE(events_with_context_flattened.load_event_start)                      AS load_event_start,
  MODE(events_with_context_flattened.navigation_start)                      AS navigation_start,
  MODE(events_with_context_flattened.redirect_end)                          AS redirect_end,
  MODE(events_with_context_flattened.redirect_start)                        AS redirect_start,
  MODE(events_with_context_flattened.request_start)                         AS request_start,
  MODE(events_with_context_flattened.response_end)                          AS response_end,
  MODE(events_with_context_flattened.response_start)                        AS response_start,
  MODE(events_with_context_flattened.secure_connection_start)               AS secure_connection_start,
  MODE(events_with_context_flattened.unload_event_end)                      AS unload_event_end,
  MODE(events_with_context_flattened.unload_event_start)                    AS unload_event_start

FROM events_with_context_flattened
GROUP BY 1,2

