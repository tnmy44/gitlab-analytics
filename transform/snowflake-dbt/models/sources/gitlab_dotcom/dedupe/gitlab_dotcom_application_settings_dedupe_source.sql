{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

SELECT
    group_id,
    default_projects_limit,
    signup_enabled,
    created_at,
    updated_at,
    shared_runners_enabled,
    usage_ping_enabled,
    shared_runners_minutes,
    repository_size_limit,
    _uploaded_at
FROM {{ source('gitlab_dotcom', 'application_settings') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
