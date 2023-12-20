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

    WHERE app_id IS NOT NULL
      AND DATE_PART(month, TRY_TO_TIMESTAMP(derived_tstamp)) = '{{ month_value }}'
      AND DATE_PART(year, TRY_TO_TIMESTAMP(derived_tstamp)) = '{{ year_value }}'
      AND TRY_TO_TIMESTAMP(derived_tstamp) is not null
)

, base AS (
  
    SELECT DISTINCT * 
    FROM filtered_source

)

/*
    we need to extract the GitLab standard context fields from the contexts JSON provided in the raw events
    A contexts json look like a list of context attached to an event:

    The GitLab standard context which we are looking for is defined by schema at:
    https://gitlab.com/gitlab-org/iglu/-/blob/master/public/schemas/com.gitlab/gitlab_standard/jsonschema/1-0-5

    To in this CTE for any event, we use LATERAL FLATTEN to create one row per context per event.
    We then extract the context schema and the context data

    We take the results from the previous CTE and isolate the only context we are interested in:
    the gitlab standard context, which has this context schema: iglu:com.gitlab/gitlab_standard/jsonschema/1-0-5
    Then we extract the id from the context_data column
*/

SELECT 
  base.event_id,
  base.derived_tstamp,
  f.value['schema']::VARCHAR                          AS ide_extension_version_context_data_schema,
  f.value['data']                                     AS ide_extension_version_context,
  f.value['data']['extension_name']::VARCHAR          AS extension_name,
  f.value['data']['extension_version']::VARCHAR       AS extension_version,
  f.value['data']['ide_name']::VARCHAR                AS ide_name,
  f.value['data']['ide_vendor']::VARCHAR              AS ide_vendor,
  f.value['data']['ide_version']::VARCHAR             AS ide_version,
  f.value['data']['language_server_version']::VARCHAR AS language_server_version
FROM base,
lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f
WHERE ide_extension_version_context_data_schema LIKE 'iglu:com.gitlab/ide_extension_version/jsonschema/%'


