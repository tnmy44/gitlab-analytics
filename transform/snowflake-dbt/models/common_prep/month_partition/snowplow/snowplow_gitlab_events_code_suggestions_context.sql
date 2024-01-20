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
        derived_tstamp,
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
      f.value['schema']::VARCHAR  AS context_data_schema,
      f.value['data']             AS context_data
    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

)

SELECT
  events_with_context_flattened.event_id::VARCHAR                                 AS event_id,
  events_with_context_flattened.derived_tstamp::DATE                              AS derived_tstamp_date,
  context_data                                                                    AS code_suggestions_context,
  context_data_schema                                                             AS code_suggestions_context_schema,
  context_data['model_engine']::VARCHAR                                           AS model_engine, 
  context_data['model_name']::VARCHAR                                             AS model_name,
  context_data['prefix_length']::INT                                              AS prefix_length,
  context_data['suffix_length']::INT                                              AS suffix_length,
  context_data['language']::VARCHAR                                               AS language,
  context_data['user_agent']::VARCHAR                                             AS user_agent,
  CASE
    WHEN context_data['gitlab_realm']::VARCHAR IN ('SaaS','saas') 
      THEN 'SaaS'
    WHEN context_data['gitlab_realm']::VARCHAR IN ('Self-Managed','self-managed') 
      THEN 'Self-Managed'
    WHEN context_data['gitlab_realm']::VARCHAR IS NULL 
      THEN NULL
    ELSE context_data['gitlab_realm']::VARCHAR
  END                                                                             AS delivery_type,
  context_data['api_status_code']::INT                                            AS api_status_code,
  context_data['gitlab_saas_namespace_ids']::VARCHAR                              AS namespace_ids,
  context_data['gitlab_instance_id']::VARCHAR                                     AS instance_id,
  context_data['gitlab_host_name']::VARCHAR                                       AS host_name
FROM events_with_context_flattened
WHERE code_suggestions_context_schema LIKE 'iglu:com.gitlab/code_suggestions_context/jsonschema/%'