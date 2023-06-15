
{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}


SELECT
    ID,
    created_at,
    updated_at,
    project_id,
    approvals_required,
    rule_type,
    report_type,
    _uploaded_at
FROM {{ source('gitlab_dotcom', 'approval_project_rules') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
