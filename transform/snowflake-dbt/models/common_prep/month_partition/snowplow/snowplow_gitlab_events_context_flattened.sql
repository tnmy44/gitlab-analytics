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

 -- Gitlab Standard Context Columns
       {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_standard/jsonschema/%',
          context_name='gitlab_standard',
          field_alias_datatype_list=[
            ('environment','','',''), 
            ('extra',"TRY_PARSE_JSON(context_data['extra'])", 'variant', ''), 
            ('namespace_id','','number', ''), 
            ('plan','', '', ''), 
            ('google_analytics_id','', '', ''),
            ('project_id','', 'number', ''),
            ('pseudonymized_user_id','', '', ''),
            ('source','', '', '')
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
            ('id','','','web_page_id')
            ]
        )
      }},

      -- Gitlab Experiment Context Columns
       {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_experiment/jsonschema/%',
          context_name='experiment_context',
          field_alias_datatype_list=[
            ('experiment','','','experiment_name'), 
            ('key','', '', 'experiment_context_key'), 
            ('variant','','', 'experiment_variant'), 
            ('migration_keys',"ARRAY_TO_STRING(context_data['migration_keys']::VARIANT, ', ')", '', 'experiment_migration_keys')
            ]
        )
      }},


      -- Code Suggestions Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/code_suggestions_context/jsonschema/%',
          context_name='code_suggestions',
          field_alias_datatype_list=[
            ('model_engine','','',''), 
            ('model_name','', '', ''), 
            ('prefix_length','','int', ''), 
            ('suffix_length','', 'int', ''), 
            ('language','', '', ''),
            ('user_agent','', '', ''),
            ('gitlab_realm','', '', ''),
            ('api_status_code','', 'int', ''),
            ('gitlab_saas_namespace_ids','', '', 'namespace_ids'),
            ('gitlab_instance_id','', '', 'instance_id'),
            ('gitlab_host_name','', '', 'host_name')
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
            ('extension_name','','',''), 
            ('extension_version','', '', ''), 
            ('ide_name','','', ''), 
            ('ide_vendor','', '', ''), 
            ('ide_version','', '', ''),
            ('language_server_version','', '', '')
            ]
        )
      }},

      -- Service Ping Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_service_ping/jsonschema/%',
          context_name='service_ping',
          field_alias_datatype_list=[
            ('event_name','','','redis_event_name'), 
            ('key_path','', '', ''), 
            ('data_source','','', '')
            ]
        )
      }}

    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

)

SELECT *                                                                                                                                
FROM events_with_context_flattened
