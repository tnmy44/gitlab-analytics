WITH source AS (
  SELECT *
  FROM {{ source('tableau_cloud','permissions') }}
),

renamed AS (
  SELECT 
    "Grantee Type"::VARCHAR AS grantee_type,
    "Grantee LUID"::VARCHAR AS grantee_luid,
    "Grantee Name"::VARCHAR AS grantee_name,
    "Item Hyperlink"::VARCHAR AS item_hyperlink,
    "Item LUID"::VARCHAR AS item_luid,
    "Item Name"::VARCHAR AS item_name,
    "Item Type"::VARCHAR AS item_type,
    "Item Parent Project Name"::VARCHAR AS item_parent_project_name,
    "Top Parent Project Name"::VARCHAR AS top_parent_project_name,
    "Controlling Permissions Project Name"::VARCHAR AS controlling_permissions_project_name,
    "Capability Type"::VARCHAR AS capability_type,
    "Permission Value"::NUMBER AS permission_value,
    "Permission Description"::VARCHAR AS permission_description,
    "Site Name"::VARCHAR AS site_name,
    "Site LUID"::VARCHAR AS site_luid,
    NULLIF("User Email"::VARCHAR,'NA') AS user_email,
    NULLIF("User LUID"::VARCHAR,'NA') AS user_luid,
    NULLIF("User Site Role"::VARCHAR,'NA') AS user_site_role,
    "Admin Insights Published At"::TIMESTAMP AS admin_insights_published_at,
    "Has Permission?"::BOOLEAN AS has_permission,
    uploaded_at::TIMESTAMP AS uploaded_at
  FROM source
)

SELECT *
FROM renamed