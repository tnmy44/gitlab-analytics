{{ config(
    tags=["mnpi_exception"]
) }}

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

) , final AS (

    SELECT

     --surrogate_key
      {{ dbt_utils.surrogate_key(['id']) }} AS dim_gitlab_versions_sk,

      --natural key
      id as dim_version_id,

     --attributes
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

    FROM calculated  

UNION ALL

SELECT 

     --surrogate_key
      {{ dbt_utils.surrogate_key(['-1']) }} AS dim_gitlab_versions_sk,

      --natural key
      -1 as dim_version_id,
    
     --attributes
     'Missing version'                      AS version,
      -1                                    AS major_version,
      -1                                    AS minor_version,
      -1                                    AS patch_number,
      0                                     AS is_monthly_release,
      'Missing is_vulnerable'               AS is_vulnerable,
      'Missing vulnerability_type_desc'     AS vulnerability_type_desc,
      '9999-12-31'                          AS created_date,
      '9999-12-31'                          AS updated_date

)    

{{ dbt_audit(
    cte_ref="final",
    created_by="@derekatwood",
    updated_by="@snalamaru",
    created_date="2020-08-06",
    updated_date="2023-03-21"
) }}

