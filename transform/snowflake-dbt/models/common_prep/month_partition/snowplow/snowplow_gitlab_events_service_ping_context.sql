{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(1)).strftime('%Y')) %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(1)).strftime('%m')) %}

{{config({
    "unique_key":"event_id",
    "cluster_by":['derived_tstamp::DATE']
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
    events_with_context_flattened.event_id::VARCHAR        AS event_id,
    events_with_context_flattened.derived_tstamp,
    context_data                                           AS service_ping_version_context,
    context_data_schema                                    AS service_ping_context_schema,
    context_data['event_name']::VARCHAR                    AS event_name,
    context_data['key_path']::VARCHAR                      AS key_path,
    context_data['data_source']::VARCHAR                   AS data_source
FROM events_with_context_flattened
WHERE service_ping_context_schema LIKE 'iglu:com.gitlab/gitlab_service_ping/jsonschema/%'