{{ config(
    materialized='view',
    tags=["product"]
) }}

WITH final AS (

    SELECT 
    r.id::INT AS id,
    r.namespace_id::INT AS namespace_id,
    r.created_at::TIMESTAMP AS created_at,
    r.updated_at::TIMESTAMP AS updated_at,
    r.base_access_level::INT,
    r.download_code::BOOLEAN,
    r.read_code::BOOLEAN,
    _uploaded_at::FLOAT
    FROM {{ ref('gitlab_dotcom_member_roles_dedupe_source') }} r

)



{{ dbt_audit(
    cte_ref="final",
    created_by="@mpetersen",
    updated_by="@mpetersen",
    created_date="2023-03-20",
    updated_date="2023-03-20"
) }}
