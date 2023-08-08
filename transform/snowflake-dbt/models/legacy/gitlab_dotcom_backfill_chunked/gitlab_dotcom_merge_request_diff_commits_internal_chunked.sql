{{ config(
  materialized="incremental"
) }}

WITH merge_request_diffs_internal AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_diffs_internal') }}
),

merge_request_diff_commits_chunked AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_diffs') }}
  WHERE
    updated_at >= '{{ var("start_date") }}'
    AND updated_at < '{{ var("end_date") }}'
),

merge_request_diff_commits_internal_chunked AS (
  SELECT merge_request_diff_commits_chunked.* FROM merge_request_diffs_internal
  INNER JOIN merge_request_diff_commits_chunked
    ON merge_request_diffs_internal.merge_request_diff_id
      = merge_request_diff_commits_chunked.merge_request_diff_id

)

SELECT * FROM merge_request_diff_commits_internal_chunked
