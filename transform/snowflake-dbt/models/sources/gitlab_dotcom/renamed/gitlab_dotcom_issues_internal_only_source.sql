    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issues_internal_only_dedupe_source') }}
  WHERE created_at::VARCHAR NOT IN ('0001-01-01 12:00:00','1000-01-01 12:00:00','10000-01-01 12:00:00')
    AND LEFT(created_at::VARCHAR , 10) != '1970-01-01'
  
), renamed AS (

  SELECT
    id::NUMBER                      AS internal_issue_id,
    iid::NUMBER                     AS internal_issue_iid,
    title::VARCHAR                  AS issue_title,
    description::VARCHAR            AS issue_description,
    project_id::NUMBER              AS project_id,
    _uploaded_at::TIMESTAMP         AS uploaded_at,
    updated_at::TIMESTAMP           AS updated_at

  FROM source

)

SELECT *
FROM renamed
