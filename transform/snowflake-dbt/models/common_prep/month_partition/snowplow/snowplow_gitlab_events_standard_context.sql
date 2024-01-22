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
        derived_tstamp,
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
    /*
    we need to extract the GitLab standard context fields from the contexts JSON provided in the raw events
    A contexts json look like a list of context attached to an event:

    The GitLab standard context which we are looking for is defined by schema at:
    https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/gitlab_standard/jsonschema/1-0-5

    To in this CTE for any event, we use LATERAL FLATTEN to create one row per context per event.
    We then extract the context schema and the context data
    */
    SELECT 
      base.*,
      f.value['schema']::VARCHAR  AS context_data_schema,
      f.value['data']             AS context_data
    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

)

/*
in this CTE we take the results from the previous CTE and isolate the only context we are interested in:
the gitlab standard context, which has this context schema: iglu:com.gitlab/gitlab_standard/jsonschema/1-0-5
Then we extract the id from the context_data column
*/
SELECT
    events_with_context_flattened.event_id::VARCHAR        AS event_id,
    events_with_context_flattened.derived_tstamp::DATE     AS derived_tstamp_date,
    context_data                                           AS gitlab_standard_context,
    context_data_schema                                    AS gitlab_standard_context_schema,
    context_data['environment']::VARCHAR                   AS environment,
    TRY_PARSE_JSON(context_data['extra'])::VARIANT         AS extra,
    context_data['namespace_id']::NUMBER                   AS namespace_id,
    context_data['plan']::VARCHAR                          AS plan,
    context_data['google_analytics_id']::VARCHAR           AS google_analytics_id,
    IFF(google_analytics_id = '', NULL,
        SPLIT_PART(google_analytics_id, '.', 3) || '.' ||
        SPLIT_PART(google_analytics_id, '.', 4))::VARCHAR  AS google_analytics_client_id,
    context_data['project_id']::NUMBER                     AS project_id,
    context_data['user_id']::VARCHAR                       AS pseudonymized_user_id,
    context_data['source']::VARCHAR                        AS source
FROM events_with_context_flattened
WHERE context_data_schema LIKE 'iglu:com.gitlab/gitlab_standard/jsonschema/%'
