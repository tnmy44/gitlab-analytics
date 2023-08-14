{{ config(
  materialized="incremental",
  full_refresh= only_force_full_refresh()
) }}

WITH merge_request_diffs_internal AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_diffs_internal') }}
),

merge_request_diff_commits_chunked AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_diffs') }}

  {% if is_incremental() %}
    WHERE
      updated_at >= (SELECT MAX('{{ var("start_date", "updated_at" ) }}') FROM {{ this }})
      AND updated_at < (SELECT MAX('{{ var("end_date", "2999-12-31" ) }}') FROM {{ this }})
  {% endif %}
),

merge_request_diff_commits_internal AS (
  SELECT merge_request_diff_commits_chunked.* FROM merge_request_diffs_internal
  INNER JOIN merge_request_diff_commits_chunked
    ON merge_request_diffs_internal.merge_request_diff_id
      = merge_request_diff_commits_chunked.merge_request_diff_id

)

SELECT * FROM merge_request_diff_commits_internal
