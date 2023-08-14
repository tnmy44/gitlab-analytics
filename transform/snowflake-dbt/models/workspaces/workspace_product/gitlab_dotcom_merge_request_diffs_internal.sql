{{ config(
  materialized="table"
) }}

WITH dim_project_internal AS (

  SELECT *
  FROM {{ ref('dim_project') }}
  WHERE namespace_is_internal = TRUE
),

merge_requests_internal AS (
  SELECT mr.*
  FROM
    {{ ref('gitlab_dotcom_merge_requests') }} mr
  INNER JOIN dim_project_internal ON mr.target_project_id = dim_project_internal.dim_project_id
),

merge_request_diffs_internal AS (
  SELECT mrd.*
  FROM
    merge_requests_internal mr
  INNER JOIN {{ ref('gitlab_dotcom_merge_request_diffs') }} mrd ON mr.merge_request_id = mrd.merge_request_id
  WHERE
    -- corresponds to created_at=2021-07-01 00:00:01.446
    mr.merge_request_diff_id >= 208751592
)

SELECT * FROM merge_request_diffs_internal
