{{ config(
        materialized = "incremental",
        unique_key = "dim_behavior_event_sk",
        tags=['product']
) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unnested_events_all')
    ])
}}

, final AS (

    SELECT
      dim_behavior_event_sk,
      event,
      event_name,
      platform,
      environment,
      event_category,
      event_action,
      event_label,
      event_property,
      MAX(behavior_at)   AS max_timestamp
    FROM events
    WHERE is_staging_event = FALSE

    {% if is_incremental() %}
    
    AND behavior_at > (SELECT MAX(max_timestamp) FROM {{this}})
    
    {% endif %}

    {{ dbt_utils.group_by(n=9) }}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@chrissharp",
    updated_by="@utkarsh060",
    created_date="2022-09-20",
    updated_date="2024-03-26"
) }}
