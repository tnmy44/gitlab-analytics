WITH source AS (

  SELECT
    -- primary key
    dim_requirement_sk,

    -- natural key
    requirement_id,

    -- foreign keys
    dim_project_id,
    ultimate_parent_namespace_id,
    dim_plan_id,
    author_id,
    created_date_id,

    -- metadata
    requirement_internal_id,
    requirement_state,
    created_at,
    updated_at
  FROM {{ ref('prep_requirement') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-07-25",
    updated_date="2023-07-25"
) }}
