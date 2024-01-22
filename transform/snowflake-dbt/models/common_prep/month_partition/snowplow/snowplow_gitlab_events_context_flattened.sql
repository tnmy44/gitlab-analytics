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
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data, NULL)                                                        AS gitlab_standard_context,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data_schema, NULL)                                                 AS gitlab_standard_context_schema,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data['environment']::VARCHAR, NULL)                                AS environment,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', TRY_PARSE_JSON(context_data['extra'])::VARIANT, NULL)                      AS extra,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data['namespace_id']::NUMBER, NULL)                                AS namespace_id,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data['plan']::VARCHAR, NULL)                                       AS plan,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data['google_analytics_id']::VARCHAR, NULL)                        AS google_analytics_id,
      IFF(google_analytics_id = '', NULL,
          SPLIT_PART(google_analytics_id, '.', 3) || '.' ||
          SPLIT_PART(google_analytics_id, '.', 4))::VARCHAR                                                                                                   AS google_analytics_client_id,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data['project_id']::NUMBER, NULL)                                  AS project_id,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data['user_id']::VARCHAR, NULL)                                    AS pseudonymized_user_id,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%', context_data['source']::VARCHAR, NULL)                                     AS source,

      -- Web Page Context Columns
      IFF(context_data_schema = 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0', context_data, NULL)                                          AS web_page_context,
      IFF(context_data_schema = 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0', context_data_schema, NULL)                                   AS web_page_context_schema,
      IFF(context_data_schema = 'iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0', context_data['id']::TEXT, NULL)                              AS web_page_id,

      -- Gitlab Experiment Context Columns
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%', context_data, NULL)                                                      AS experiment_context,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%', context_data_schema, NULL)                                               AS experiment_context_schema,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%', context_data['experiment']::VARCHAR, NULL)                               AS experiment_name,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%', context_data['key']::VARCHAR, NULL)                                      AS experiment_context_key,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%', context_data['variant']::VARCHAR, NULL)                                  AS experiment_variant,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_experiment/jsonschema/%', ARRAY_TO_STRING(context_data['migration_keys']::VARIANT, ', '), NULL)    AS experiment_migration_keys,


      -- Code Suggestions Context Columns
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data, NULL)                                               AS code_suggestions_context,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data_schema, NULL)                                        AS code_suggestions_context_schema,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['model_engine']::VARCHAR, NULL)                      AS model_engine, 
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['model_name']::VARCHAR, NULL)                        AS model_name,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['prefix_length']::INT, NULL)                         AS prefix_length,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['suffix_length']::INT, NULL)                         AS suffix_length,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['language']::VARCHAR, NULL)                          AS language,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['user_agent']::VARCHAR, NULL)                        AS user_agent,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['gitlab_realm']::VARCHAR, NULL)                      AS gitlab_realm,
      CASE
        WHEN gitlab_realm IN ('SaaS','saas') 
          THEN 'SaaS'
        WHEN gitlab_realm IN ('Self-Managed','self-managed') 
          THEN 'Self-Managed'
        WHEN gitlab_realm IS NULL 
          THEN NULL
        ELSE gitlab_realm
      END                                                                                                                                                     AS delivery_type,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['api_status_code']::INT, NULL)                       AS api_status_code,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['gitlab_saas_namespace_ids']::VARCHAR, NULL)         AS namespace_ids,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['gitlab_instance_id']::VARCHAR, NULL)                AS instance_id,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%', context_data['gitlab_host_name']::VARCHAR, NULL)                  AS host_name,

      -- IDE Extension Version Context Columns
      IFF(context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%', context_data, NULL)                                                  AS ide_extension_version_context,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%', context_data_schema, NULL)                                           AS ide_extension_version_context_schema,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%', context_data['extension_name']::VARCHAR, NULL)                       AS extension_name,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%', context_data['extension_version']::VARCHAR, NULL)                    AS extension_version,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%', context_data['ide_name']::VARCHAR, NULL)                             AS ide_name,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%', context_data['ide_vendor']::VARCHAR, NULL)                           AS ide_vendor,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%', context_data['ide_version']::VARCHAR, NULL)                          AS ide_version,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%', context_data['language_server_version']::VARCHAR, NULL)              AS language_server_version,

      -- Service Ping Context Columns
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_service_ping/jsonschema/%', context_data, NULL)                                                    AS service_ping_context,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_service_ping/jsonschema/%', context_data_schema, NULL)                                             AS service_ping_context_schema,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_service_ping/jsonschema/%', context_data['event_name']::VARCHAR, NULL)                             AS redis_event_name,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_service_ping/jsonschema/%', context_data['key_path']::VARCHAR, NULL)                               AS key_path,
      IFF(context_data_schema LIKE 'iglu:com.gitlab/gitlab_service_ping/jsonschema/%', context_data['data_source']::VARCHAR, NULL)                            AS data_source
    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

)

SELECT
  events_with_context_flattened.event_id::VARCHAR                                                                                                             AS event_id,
  events_with_context_flattened.derived_tstamp_date,
  MODE(gitlab_standard_context)                                                                                                                               AS gitlab_standard_context,
  MODE(gitlab_standard_context_schema)                                                                                                                        AS gitlab_standard_context_schema,
  MODE(environment)                                                                                                                                           AS environment,
  MODE(extra)                                                                                                                                                 AS extra,
  MODE(namespace_id)                                                                                                                                          AS namespace_id,
  MODE(plan)                                                                                                                                                  AS plan,
  MODE(google_analytics_id)                                                                                                                                   AS google_analytics_id,
  MODE(google_analytics_client_id)                                                                                                                            AS google_analytics_client_id,
  MODE(project_id)                                                                                                                                            AS project_id,
  MODE(pseudonymized_user_id)                                                                                                                                 AS pseudonymized_user_id,
  MODE(source)                                                                                                                                                AS source,
  MODE(web_page_context)                                                                                                                                      AS web_page_context,
  MODE(web_page_context_schema)                                                                                                                               AS web_page_context_schema,
  MODE(web_page_id)                                                                                                                                           AS web_page_id,
  MODE(experiment_context)                                                                                                                                    AS experiment_context,
  MODE(experiment_context_schema)                                                                                                                             AS experiment_context_schema,
  MODE(experiment_name)                                                                                                                                       AS experiment_name,
  MODE(experiment_context_key)                                                                                                                                AS experiment_context_key,
  MODE(experiment_variant)                                                                                                                                    AS experiment_variant,
  MODE(experiment_migration_keys)                                                                                                                             AS experiment_migration_keys,
  MODE(code_suggestions_context)                                                                                                                              AS code_suggestions_context,
  MODE(code_suggestions_context_schema)                                                                                                                       AS code_suggestions_context_schema,
  MODE(model_engine)                                                                                                                                          AS model_engine, 
  MODE(model_name)                                                                                                                                            AS model_name,
  MODE(prefix_length)                                                                                                                                         AS prefix_length,
  MODE(suffix_length)                                                                                                                                         AS suffix_length,
  MODE(language)                                                                                                                                              AS language,
  MODE(user_agent)                                                                                                                                            AS user_agent,
  MODE(delivery_type)                                                                                                                                         AS delivery_type,
  MODE(api_status_code)                                                                                                                                       AS api_status_code,
  MODE(namespace_ids)                                                                                                                                         AS namespace_ids,
  MODE(instance_id)                                                                                                                                           AS instance_id,
  MODE(host_name)                                                                                                                                             AS host_name,
  MODE(ide_extension_version_context)                                                                                                                         AS ide_extension_version_context,
  MODE(ide_extension_version_context_schema)                                                                                                                  AS ide_extension_version_context_schema,
  MODE(extension_name)                                                                                                                                        AS extension_name,
  MODE(extension_version)                                                                                                                                     AS extension_version,
  MODE(ide_name)                                                                                                                                              AS ide_name,
  MODE(ide_vendor)                                                                                                                                            AS ide_vendor,
  MODE(ide_version)                                                                                                                                           AS ide_version,
  MODE(language_server_version)                                                                                                                               AS language_server_version,
  MODE(service_ping_context)                                                                                                                                  AS service_ping_context,
  MODE(service_ping_context_schema)                                                                                                                           AS service_ping_context_schema,
  MODE(redis_event_name)                                                                                                                                      AS redis_event_name,
  MODE(key_path)                                                                                                                                              AS key_path,
  MODE(data_source)                                                                                                                                           AS data_source

FROM events_with_context_flattened
GROUP BY 1,2
