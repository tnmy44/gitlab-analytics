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
    and updated_at < '{{ var("end_date") }}'
),
merge_request_diff_commits_internal_chunked AS (
  select merge_request_diff_commits_chunked.* from merge_request_diffs_internal
  join merge_request_diff_commits_chunked
  on merge_request_diffs_internal.id =
  merge_request_diff_commits_chunked.merge_request_diff_id

)

SELECT * FROM merge_request_diff_commits_internal_chunked
