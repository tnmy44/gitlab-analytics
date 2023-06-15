
{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}


SELECT 
    id,
    namespace_id,
    created_at,
    updated_at,
    _uploaded_at
FROM {{ source('gitlab_dotcom', 'audit_events_external_audit_event_destinations') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1
