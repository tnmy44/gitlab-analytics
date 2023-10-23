{{ config(

    materialized='incremental',
    unique_key='suggestion_id',
    tags=['product'],
    on_schema_change='sync_all_columns'
  ) 

}}

WITH gitlab_ide_extension_events AS (

  SELECT
    {{ dbt_utils.star(from=ref('wk_mart_behavior_structured_event_code_suggestion'), except=["CREATED_BY", 
    "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM {{ ref('wk_mart_behavior_structured_event_code_suggestion') }}
  WHERE app_id = 'gitlab_ide_extension' --events that can be used to calculate suggestion outcome
    AND event_label IS NOT NULL --required field in order to stitch the events together

  --TBD/NEEDS BUSINESS VALIDATION: In the event that there are multiple events per event_label and event_action, use the first one
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_label, event_action --remove duplicate events
      ORDER BY behavior_at ASC) = 1

),

--Visual with event sequence here: https://gitlab.com/gitlab-org/modelops/applied-ml/code-suggestions/ai-assist/-/issues/256#note_1549346766

requested AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_requested'

),

loaded AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_loaded'

),

shown AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_shown'

),

accepted AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_accepted'

),

rejected AS (

  SELECT gitlab_ide_extension_events.*
  FROM gitlab_ide_extension_events
  LEFT JOIN accepted
    ON gitlab_ide_extension_events.event_label = accepted.event_label
  WHERE gitlab_ide_extension_events.event_action = 'suggestion_rejected'
    AND accepted.event_label IS NULL --suggestion cannot be accepted and rejected, default to accepted if both present: https://gitlab.com/gitlab-data/product-analytics/-/issues/1410#note_1581747408

),

cancelled AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_cancelled'

),

not_provided AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_not_provided'

),

error AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_error'

),

suggestion_level AS (

  SELECT
    requested.event_label AS suggestion_id,
    --Edge cases where language on the requested event is NULL or '', fall back to loaded event
    IFF(requested.language IS NULL OR requested.language = '', loaded.language, requested.language) AS language,
    requested.delivery_type,
    requested.prefix_length,
    requested.suffix_length,
    requested.extension_name,
    requested.extension_version,
    requested.ide_name,
    requested.ide_vendor,
    requested.ide_version,
    --model_engine and model_name not available on requested event. Default to loaded event, fall back to cancelled to cover a handful of edge cases
    COALESCE(loaded.model_engine, cancelled.model_engine) AS model_engine,
    COALESCE(loaded.model_name, cancelled.model_engine) AS model_name,
    requested.behavior_at AS requested_at,
    loaded.behavior_at AS loaded_at,
    shown.behavior_at AS shown_at,
    accepted.behavior_at AS accepted_at,
    rejected.behavior_at AS rejected_at,
    cancelled.behavior_at AS cancelled_at,
    not_provided.behavior_at AS not_provided_at,
    error.behavior_at AS error_at,
    DATEDIFF('milliseconds', requested_at, loaded_at) AS load_time_in_ms,
    DATEDIFF('milliseconds', shown_at, IFNULL(accepted_at, rejected_at)) AS display_time_in_ms,
    --Outcome order: accepted, rejected, cancelled, not_provided, shown, loaded, error, requested
    COALESCE(accepted.event_action, rejected.event_action, 
             cancelled.event_action, not_provided.event_action, 
             shown.event_action, loaded.event_action,  
             error.event_action, requested.event_action) AS suggestion_outcome,
    IFF(loaded.event_label IS NOT NULL, TRUE, FALSE) AS was_loaded,
    IFF(shown.event_label IS NOT NULL, TRUE, FALSE) AS was_shown,
    IFF(accepted.event_label IS NOT NULL, TRUE, FALSE) AS was_accepted,
    IFF(rejected.event_label IS NOT NULL, TRUE, FALSE) AS was_rejected,
    IFF(cancelled.event_label IS NOT NULL, TRUE, FALSE) AS was_cancelled,
    IFF(not_provided.event_label IS NOT NULL, TRUE, FALSE) AS was_not_provided,
    IFF(error.event_label IS NOT NULL, TRUE, FALSE) AS was_error
  FROM requested
  LEFT JOIN loaded
    ON requested.event_label = loaded.event_label
  LEFT JOIN shown
    ON requested.event_label = shown.event_label
  LEFT JOIN accepted
    ON requested.event_label = accepted.event_label
  LEFT JOIN rejected
    ON requested.event_label = rejected.event_label
  LEFT JOIN cancelled
    ON requested.event_label = cancelled.event_label
  LEFT JOIN not_provided
    ON requested.event_label = not_provided.event_label
  LEFT JOIN error
    ON requested.event_label = error.event_label
  {% if is_incremental() %}
    
  WHERE requested.behavior_at >= (SELECT MAX(requested_at) FROM {{ this }})
    
  {% endif %}

)

{{ dbt_audit(
    cte_ref="suggestion_level",
    created_by="@cbraza",
    updated_by="@cbraza",
    created_date="2023-10-20",
    updated_date="2023-10-20"
) }}
