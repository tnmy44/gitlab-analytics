{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_integration_sk"
    })
}}

WITH final AS (

    SELECT 
      -- Surrogate Key
      dim_integration_sk,

      -- Natural Key
      integration_id,

      -- Foreign Keys
      dim_project_id,
      ultimate_parent_namespace_id,
      dim_plan_id,
      created_date_id,

      -- Dimensional Contexts
      is_active,
      created_at,
      updated_at
    FROM {{ ref('prep_integration') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-07-27",
    updated_date="2023-07-27"
) }}