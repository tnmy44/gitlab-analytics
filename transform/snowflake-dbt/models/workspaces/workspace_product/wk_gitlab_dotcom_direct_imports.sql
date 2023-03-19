{{ config(
    materialized='view',
    tags=["mnpi_exception", "product"]
) }}

WITH final AS (
WITH final AS (
    SELECT
    bi.user_id,
    e.namespace_id, -- This attribute is only populated when source_type = 0 (groups). If the status is failed, probably the value will be NULL since the migration failed
    e.project_id,   -- This attribute is only populated when source_type = 1 (project). If the status is failed, probably the value will be NULL since the migration failed
    COALESCE(e.namespace_id,e.project_id) AS dim_namespace_id,
    e.created_at AS entity_created_at,
    e.updated_at AS entity_updated_at,   -- This date can be used as the date the migration finished, failed, or timed out as we update the date along with the final status
    bi.created_at AS bulk_import_created_at,
    e.has_failures,
    CASE 
      WHEN e.source_type = 0 THEN 'group' 
      WHEN e.source_type = 1 THEN 'project'
      ELSE 'unknown'                        -- in case a different source_type is added
    END AS entity_type,
    CASE
      WHEN e.status = 0 THEN 'created'    -- temporary status
      WHEN e.status = 1 THEN 'started'    -- temporary status
      WHEN e.status = 2 THEN 'finished'   -- final status
      WHEN e.status = 3 THEN 'timeout'    -- final status
      WHEN e.status = -1 THEN 'failed'    -- final status
      ELSE 'unknown'                      -- in case a different status is added
    END AS status
    FROM
    {{ ref( 'gitlab_dotcom_bulk_import_entities_dedupe_source') }} e
    LEFT JOIN 
    {{ ref( 'gitlab_dotcom_bulk_imports_dedupe_source') }} bi ON e.bulk_import_id = bi.id

    )


{{ dbt_audit(
    cte_ref="final",
    created_by="@mpetersen",
    updated_by="@mpetersen",
    created_date="2023-03-16",
    updated_date="2023-03-16"
) }}
