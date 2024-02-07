{{ config(
        materialized = "incremental",
        unique_key = "behavior_structured_event_pk",
        on_schema_change='sync_all_columns',
        cluster_by=['experiment_name'],
        tags=['product']
) }}

{{ 
    simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('snowplow_gitlab_events_experiment_contexts', 'prep_snowplow_gitlab_events_experiment_contexts_all')

    ])
}}

, final AS (

    SELECT

      {{ 
      dbt_utils.star(from=ref('fct_behavior_structured_event'),
      relation_alias='fct_behavior_structured_event',
      except=[
        'CREATED_BY',
        'UPDATED_BY',
        'MODEL_CREATED_DATE',
        'MODEL_UPDATED_DATE',
        'DBT_CREATED_AT',
        'DBT_UPDATED_AT'
        ]) 
      }}
      
    FROM fct_behavior_structured_event
    INNER JOIN snowplow_gitlab_events_experiment_contexts
      ON fct_behavior_structured_event.behavior_structured_event_pk = snowplow_gitlab_events_experiment_contexts.event_id
    WHERE fct_behavior_structured_event.has_gitlab_experiment_context = TRUE

    {% if is_incremental() %}

    WHERE behavior_at > (SELECT MAX(behavior_at) FROM {{this}})

    {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-09-01",
    updated_date="2024-02-07"
) }}