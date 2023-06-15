
{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}


SELECT
    id
    created_at,
    updated_at,
    relative_position,
    start_event_identifier,
    end_event_identifier,
    group_id,
    start_event_label_id,
    end_event_label_id,
    hidden,
    custom,
    group_value_stream_id,
    _uploaded_at
FROM {{ source('gitlab_dotcom', 'analytics_cycle_analytics_group_stages') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
