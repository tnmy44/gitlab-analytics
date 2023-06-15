
{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}


SELECT
    id,
    created_at,
    updated_at,
    project_id,
    created_by_user_id,
    _uploaded_at
FROM {{ source('gitlab_dotcom', 'cluster_agents') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
