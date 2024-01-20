{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(1)).strftime('%Y')) %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(1)).strftime('%m')) %}

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

    WHERE DATE_PART(month, TRY_TO_TIMESTAMP(derived_tstamp)) = '{{ month_value }}'
      AND DATE_PART(year, TRY_TO_TIMESTAMP(derived_tstamp)) = '{{ year_value }}'
      AND TRY_TO_TIMESTAMP(derived_tstamp) IS NOT NULL
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
  MAX(gitlab_standard_context)                                                                                                                                AS gitlab_standard_context,
  MAX(gitlab_standard_context_schema)                                                                                                                         AS gitlab_standard_context_schema,
  MAX(environment)                                                                                                                                            AS environment,
  MAX(extra)                                                                                                                                                  AS extra,
  MAX(namespace_id)                                                                                                                                           AS namespace_id,
  MAX(plan)                                                                                                                                                   AS plan,
  MAX(google_analytics_id)                                                                                                                                    AS google_analytics_id,
  MAX(google_analytics_client_id)                                                                                                                             AS google_analytics_client_id,
  MAX(project_id)                                                                                                                                             AS project_id,
  MAX(pseudonymized_user_id)                                                                                                                                  AS pseudonymized_user_id,
  MAX(source)                                                                                                                                                 AS source,
  MAX(web_page_context)                                                                                                                                       AS web_page_context,
  MAX(web_page_context_schema)                                                                                                                                AS web_page_context_schema,
  MAX(web_page_id)                                                                                                                                            AS web_page_id,
  MAX(experiment_context)                                                                                                                                     AS experiment_context,
  MAX(experiment_context_schema)                                                                                                                              AS experiment_context_schema,
  MAX(experiment_name)                                                                                                                                        AS experiment_name,
  MAX(experiment_context_key)                                                                                                                                 AS experiment_context_key,
  MAX(experiment_variant)                                                                                                                                     AS experiment_variant,
  MAX(experiment_migration_keys)                                                                                                                              AS experiment_migration_keys,
  MAX(code_suggestions_context)                                                                                                                               AS code_suggestions_context,
  MAX(code_suggestions_context_schema)                                                                                                                        AS code_suggestions_context_schema,
  MAX(model_engine)                                                                                                                                           AS model_engine, 
  MAX(model_name)                                                                                                                                             AS model_name,
  MAX(prefix_length)                                                                                                                                          AS prefix_length,
  MAX(suffix_length)                                                                                                                                          AS suffix_length,
  MAX(language)                                                                                                                                               AS language,
  MAX(user_agent)                                                                                                                                             AS user_agent,
  MAX(delivery_type)                                                                                                                                          AS delivery_type,
  MAX(api_status_code)                                                                                                                                        AS api_status_code,
  MAX(namespace_ids)                                                                                                                                          AS namespace_ids,
  MAX(instance_id)                                                                                                                                            AS instance_id,
  MAX(host_name)                                                                                                                                              AS host_name,
  MAX(ide_extension_version_context)                                                                                                                          AS ide_extension_version_context,
  MAX(ide_extension_version_context_schema)                                                                                                                   AS ide_extension_version_context_schema,
  MAX(extension_name)                                                                                                                                         AS extension_name,
  MAX(extension_version)                                                                                                                                      AS extension_version,
  MAX(ide_name)                                                                                                                                               AS ide_name,
  MAX(ide_vendor)                                                                                                                                             AS ide_vendor,
  MAX(ide_version)                                                                                                                                            AS ide_version,
  MAX(language_server_version)                                                                                                                                AS language_server_version,
  MAX(service_ping_context)                                                                                                                                   AS service_ping_context,
  MAX(service_ping_context_schema)                                                                                                                            AS service_ping_context_schema,
  MAX(redis_event_name)                                                                                                                                       AS redis_event_name,
  MAX(key_path)                                                                                                                                               AS key_path,
  MAX(data_source)                                                                                                                                            AS data_source

FROM events_with_context_flattened
GROUP BY 1,2
