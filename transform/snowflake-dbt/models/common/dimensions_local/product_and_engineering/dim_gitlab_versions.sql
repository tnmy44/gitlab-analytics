
WITH versions AS (

    SELECT *
    FROM {{ ref('version_versions_source') }}

), calculated AS (

    SELECT
      *,
      SPLIT_PART(version, '.', 1)::NUMBER   AS major_version,
      SPLIT_PART(version, '.', 2)::NUMBER   AS minor_version,
      SPLIT_PART(version, '.', 3)::NUMBER   AS patch_number,
      IFF(patch_number = 0, TRUE, FALSE)    AS is_monthly_release,
      created_at::DATE                      AS created_date,
      updated_at::DATE                      AS updated_date
    FROM versions  

), renamed AS (

    SELECT
      id AS version_id,
      version,
      major_version,
      minor_version,
      patch_number,
      is_monthly_release,
      is_vulnerable,
      created_date,
      updated_date
    FROM calculated  
)

, final AS (

    SELECT
      version_id as dim_version_id,
      version,
      major_version,
      minor_version,
      patch_number,
      is_monthly_release,
      is_vulnerable,
      CASE WHEN is_vulnerable IN ('0','f') THEN 'not_vulnerable'
           WHEN is_vulnerable = '1' THEN 'non_critical'
           WHEN is_vulnerable = '2' THEN 'critical'
           WHEN is_vulnerable = 't' THEN 'vulnerable'
      ELSE NULL END AS vulnerability_type_desc,
      created_date,
      updated_date
    FROM renamed  

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@derekatwood",
    updated_by="@snalamaru",
    created_date="2020-08-06",
    updated_date="2023-03-15"
) }}

