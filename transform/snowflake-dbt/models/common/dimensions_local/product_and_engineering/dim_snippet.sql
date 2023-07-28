{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_snippet_sk"
    })
}}

WITH final AS (

    SELECT
      dim_snippet_sk,
      snippet_id,
      author_id,
      dim_project_id,
      ultimate_parent_namespace_id,
      dim_plan_id,
      snippet_type,
      created_date_id,
      created_at,
      updated_at
    FROM {{ ref('prep_snippet') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-07-28",
    updated_date="2023-07-28"
) }}
