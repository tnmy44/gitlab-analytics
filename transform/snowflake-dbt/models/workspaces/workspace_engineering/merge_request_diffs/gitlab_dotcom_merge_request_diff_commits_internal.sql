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
  FROM {{ source('gitlab_dotcom', 'merge_request_diff_commits') }}
  {% if is_incremental() %}
    WHERE
    /*
    If there is a passed in backfill_start_id, use that
    Else, handle like a regular incremental model using merge_request_diff_id
    */
      merge_request_diff_id >= (
        SELECT
          {% if var('backfill_start_id', false) != false %}
          '{{ var("backfill_start_id") }}'
        {% else %}
            MAX(merge_request_diff_id) + 1
          FROM {{ this }}
          {% endif %}
      )
      AND
      merge_request_diff_id
      <= (
        SELECT '{{ var("backfill_end_id", 9999999999999) }}'
      )
  {% endif %}
),

merge_request_diff_commits_internal AS (
  SELECT merge_request_diff_commits_chunked.* FROM merge_request_diffs_internal
  INNER JOIN merge_request_diff_commits_chunked
    ON merge_request_diffs_internal.merge_request_diff_id
      = merge_request_diff_commits_chunked.merge_request_diff_id

)

SELECT * FROM merge_request_diff_commits_internal
