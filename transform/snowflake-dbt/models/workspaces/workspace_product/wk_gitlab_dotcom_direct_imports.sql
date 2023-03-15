{{ config(
    materialized='view',
    tags=["mnpi_exception", "product"]
) }}

WITH final AS (
    SELECT
    bi.id AS bulk_import_id,
    bi.created_at AS bulk_import_created_at,
    bi.user_id,
    e.namespace_id,
    CASE WHEN MAX(e.status) = 2 THEN 'Completed' ELSE 'Failed' END AS status,
    MAX(e.created_at) AS latest_bulk_import,
    MIN(e.created_at) AS first_bulk_import
    FROM
    {{ ref( 'gitlab_dotcom_bulk_import_entities_dedupe_source') }} e
    LEFT JOIN 
    {{ ref( 'gitlab_dotcom_bulk_imports_dedupe_source') }} bi ON e.bulk_import_id = bi.id
    GROUP BY 
    1,2,3,4
    )


{{ dbt_audit(
    cte_ref="final",
    created_by="@mpetersen",
    updated_by="@mpetersen",
    created_date="2023-03-16",
    updated_date="2023-03-16"
) }}
