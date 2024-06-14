{{ config(
    materialized='table'
) }}

WITH source AS

(

    SELECT {{ hash_sensitive_columns('commonroom_community_members_source') }}
    FROM {{ ref('commonroom_community_members_source') }}

), final AS (

    SELECT *
    FROM source
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rbacovic",
    updated_by="@rbacovic",
    created_date="2024-06-14",
    updated_date="2024-06-14",
  ) }}