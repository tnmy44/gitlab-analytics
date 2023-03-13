WITH source AS (

    SELECT *
    FROM {{ ref('thanos_stage_group_error_budget_completeness_source') }}
    WHERE is_success = true
)
SELECT *
FROM source
