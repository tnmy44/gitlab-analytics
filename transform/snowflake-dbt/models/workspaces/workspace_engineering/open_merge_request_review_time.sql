WITH product_mrs AS (

  SELECT *
  FROM {{ ref('engineering_merge_requests') }} AS mrs
  WHERE (
    (merge_request_state = 'opened' AND mrs.merged_at IS NULL)
    OR (merge_request_state = 'merged' AND mrs.merged_at IS NOT NULL)
  )
  AND YEAR(created_at) >= 2022

),

/*when mr author reassigns to someone other than themself, we recognize this as a review request*/
first_non_author_assignment AS (

  SELECT
    mrs.merge_request_id,
    0                    AS review_requests,
    0                    AS reviewer_count,
    MIN(note_created_at) AS first_review_date
  FROM product_mrs AS mrs
  INNER JOIN {{ ref('gitlab_dotcom_merge_request_assignment_events') }} AS asevs ON mrs.merge_request_id = asevs.merge_request_id AND mrs.author_id != asevs.event_user_id
  WHERE asevs.event IN ('assigned', 'reassigned')
  GROUP BY 1

),

/*retrieve review dates from notes table*/
notes AS (

  SELECT
    created_at  AS review_requested_at,
    noteable_id AS merge_request_id,
    note
  FROM {{ ref('internal_notes') }} AS notes
  WHERE notes.noteable_type = 'MergeRequest'
--     and array_contains('reviewer'::variant, notes.action_type_array)

),

extracted_usernames AS (
  SELECT
    review_requested_at,
    merge_request_id,
    REPLACE(REGEXP_SUBSTR(TRIM(value), '@([^\\s,]+)'), '@', '') AS username
  FROM notes,
    LATERAL SPLIT_TO_TABLE(note, ',')
  WHERE notes.note LIKE '%requested review from%'

),

agg AS (

  SELECT
    mrs.merge_request_id,
    GREATEST(COUNT(DISTINCT username), MAX(first_non_author_assignment.reviewer_count))                         AS reviewer_count, --extracted from notes
    GREATEST(COUNT(DISTINCT review_requested_at || username), MAX(first_non_author_assignment.review_requests)) AS review_requests, --extracted from notes
    LEAST(MIN(extracted_usernames.review_requested_at), MIN(first_non_author_assignment.first_review_date))     AS first_review_date -- first assignment event
  FROM product_mrs AS mrs
  LEFT JOIN extracted_usernames ON mrs.merge_request_id = extracted_usernames.merge_request_id
  LEFT JOIN first_non_author_assignment ON mrs.merge_request_id = first_non_author_assignment.merge_request_id
  GROUP BY 1

),
/*
agg AS (

  SELECT
    mrs.merge_request_id,
    COUNT(DISTINCT username)                        AS reviewer_count, --extracted from notes
    COUNT(DISTINCT review_requested_at || username) AS review_requests, --extracted from notes
    MIN(review_requested_at)                        AS first_review_date -- first assignment event
  FROM extracted_usernames
  INNER JOIN product_mrs AS mrs ON extracted_usernames.merge_request_id = mrs.merge_request_id
  GROUP BY 1
),

combined AS (
  SELECT
    merge_request_id,
    MAX(reviewer_count)    AS reviewer_count,
    MAX(review_requests)   AS review_requests,
    MIN(first_review_date) AS first_review_date
  FROM (
    SELECT * FROM first_non_author_assignment
    UNION
    SELECT * FROM agg
  )
  GROUP BY 1

),
*/
first_review_date AS (

  SELECT DISTINCT
    mrs.*,
    reviewer_count,
    review_requests,
    first_review_date
  FROM product_mrs AS mrs
  INNER JOIN agg ON mrs.merge_request_id = agg.merge_request_id

),

date_spine AS (

  SELECT date_actual
  FROM {{ ref('dim_date') }}
  WHERE date_actual BETWEEN DATEADD(MONTH, -18, CURRENT_DATE) AND CURRENT_DATE

),

/*add flag for mrs older than 365 days*/
add_old_flag AS (

  SELECT
    DATE_TRUNC('day', date_spine.date_actual)                                                         AS day,
    first_review_date.*,
    CASE WHEN DATEADD('day', -365, date_actual) >= created_at AND merged_at IS NULL THEN 1 ELSE 0 END AS old_1yr_flag,
    DATEDIFF('day', created_at, date_spine.date_actual)                                               AS days_open,
    ROUND(DATEDIFF('day', created_at, first_review_date), 2)                                          AS days_to_review,
    ROUND(DATEDIFF('day', first_review_date, date_spine.date_actual), 2)                              AS days_in_review,
    PERCENT_RANK() OVER (PARTITION BY day ORDER BY days_open)                                         AS days_open_p95
  FROM date_spine
  INNER JOIN first_review_date ON date_spine.date_actual BETWEEN first_review_date.first_review_date AND
    COALESCE(merged_at, CURRENT_DATE)::DATE

)


SELECT DISTINCT
  mr.*,
  TRY_CAST(sizes.product_merge_request_files_changed AS INTEGER)               AS files_changed,
  TRY_CAST(sizes.product_merge_request_lines_added AS INTEGER)                 AS added_lines,
  TRY_CAST(sizes.product_merge_request_lines_removed AS INTEGER)               AS removed_lines,
  MAX(notes.review_requested_at) OVER (PARTITION BY mr.merge_request_id)::DATE AS last_note_date
FROM add_old_flag AS mr
LEFT JOIN {{ ref('sizes_part_of_product_merge_requests') }} AS sizes ON mr.merge_request_iid = sizes.product_merge_request_iid AND mr.target_project_id = sizes.product_merge_request_project_id
LEFT JOIN notes ON mr.merge_request_id = notes.merge_request_id
WHERE mr.merge_request_id NOT IN (SELECT deleted_merge_request_id FROM {{ ref('sheetload_deleted_mrs') }})
--AND old_1yr_flag = 0
-- and day = dateadd('day',-1,current_date)
-- and reviewer_count is null
