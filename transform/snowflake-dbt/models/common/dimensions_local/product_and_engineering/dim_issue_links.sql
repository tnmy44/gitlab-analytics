WITH prep_issue_links AS (

    SELECT 
      dim_issue_link_id,
      -- FOREIGN KEYS
      -- joins to dim_issue_sk in dim_issue table
      dim_issue_sk_source,
      dim_issue_sk_target,
      --
      created_at,
      updated_at,
      valid_from
    FROM {{ ref('prep_issue_links') }}

)

{{ dbt_audit(
    cte_ref="prep_issue_links",
    created_by="@dtownsend",
    updated_by="@michellecooper",
    created_date="2021-08-04",
    updated_date="2023-09-29"
) }}