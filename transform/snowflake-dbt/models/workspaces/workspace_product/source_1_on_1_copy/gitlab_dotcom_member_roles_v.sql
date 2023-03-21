{{ config(
    materialized='view',
    tags=["mnpi_exception", "product"]
) }}

WITH final AS (

    SELECT 
    *
    FROM {{ ref('gitlab_dotcom_member_roles_source') }}

)



{{ dbt_audit(
    cte_ref="final",
    created_by="@mpetersen",
    updated_by="@mpetersen",
    created_date="2023-03-20",
    updated_date="2023-03-20"
) }}
