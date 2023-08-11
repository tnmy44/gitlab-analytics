WITH project_ancestory AS (

  SELECT
    source_primary_key,
    LISTAGG(folder_name, '/') WITHIN GROUP (ORDER BY hierarchy_level DESC) AS full_path
  FROM {{ ref('gcp_billing_export_project_ancestry') }}
  WHERE uploaded_at >= '2022-01-01' AND hierarchy_level > 1

  GROUP BY 1
),

export AS (

  SELECT * FROM {{ ref('gcp_billing_export_xf') }}
  WHERE invoice_month >= '2022-01-01'


),

renamed AS (
  SELECT DISTINCT
    b.uploaded_at,
    b.project_id AS gcp_project_id,
    a.full_path
  FROM project_ancestory AS a
  INNER JOIN export AS b ON a.source_primary_key = b.source_primary_key
)

SELECT
  gcp_project_id,
  full_path,
  MIN(uploaded_at) AS first_created_at,
  MAX(uploaded_at) AS last_updated_at
FROM renamed
GROUP BY 1, 2
