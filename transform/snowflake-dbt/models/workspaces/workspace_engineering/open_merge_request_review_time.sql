WITH product_mrs AS (

    SELECT *
    FROM {{ref('engineering_merge_requests')}} mrs
    WHERE (
        (merge_request_state = 'opened' AND mrs.merged_at IS NULL)
        OR (merge_request_state = 'merged' AND mrs.merged_at IS NOT NULL)
        )
        AND year(merge_request_created_at) >= 2022
    
)

/*When MR author reassigns to someone other than themself, we recognize this as a review request*/
, first_non_author_assignment AS (
    SELECT mrs.merge_request_id
    , 0 as review_requests
    , 0 as reviewer_count
    , min(NOTE_CREATED_AT) as first_review_date
    FROM product_mrs mrs
    JOIN {{ref('gitlab_dotcom_merge_request_assignment_events')}} asevs ON mrs.merge_request_id = asevs.merge_request_id AND mrs.author_id <> asevs.EVENT_USER_ID
    WHERE asevs.event in ('assigned', 'reassigned')
    GROUP BY 1
)

/*Retrieve review dates from notes table*/
, notes AS (
    SELECT created_at as review_requested_at
    , noteable_id as merge_request_id
    , note AS input_string
    FROM {{ref('internal_notes')}}  notes 
    WHERE notes.noteable_type = 'MergeRequest'
--     AND ARRAY_CONTAINS('reviewer'::variant, notes.action_type_array)
)

, extracted_usernames AS (
    SELECT review_requested_at
    , merge_request_id
    , REPLACE(REGEXP_SUBSTR(TRIM(VALUE), '@([^\\s,]+)'), '@', '') AS username
    FROM notes,
    LATERAL SPLIT_TO_TABLE(input_string, ',')
    where notes.note LIKE '%requested review from%'
)

, agg AS (
  SELECT mrs.merge_request_id
  , count(distinct username) AS reviewer_count
  , count(distinct review_requested_at||username) as review_requests
  , min(review_requested_at) as first_review_date
  FROM extracted_usernames
  JOIN product_mrs mrs ON extracted_usernames.merge_request_id = mrs.merge_request_id
  GROUP BY 1
)

, combined AS (
  SELECT merge_request_id
  , max(reviewer_count) as reviewer_count
  , max(review_requests) as review_requests
  , min(first_review_date) as first_review_date
  FROM (SELECT * FROM first_non_author_assignment
        UNION
        SELECT * FROM agg)
  GROUP BY 1
)

, first_review_date AS (
    SELECT DISTINCT mrs.*
    , reviewer_count
    , review_requests
    , first_review_date
  FROM product_mrs mrs
  JOIN combined on mrs.merge_request_id = combined.merge_request_id
)

, date_spine AS (
    SELECT date_actual
    FROM {{ref('dim_date')}}
    WHERE date_actual BETWEEN DATEADD(month, -18, CURRENT_DATE) AND CURRENT_DATE
)

/*Add flag for MRs older than 365 days*/
, add_old_flag AS (
    SELECT date_trunc('day',date_spine.date_actual) as day
    , first_review_date.* 
    , case when dateadd('day',-365,date_actual) >= merge_request_created_at and merged_at is null then 1 else 0 end as old_1yr_flag
    , datediff('day',merge_request_created_at,date_spine.date_actual) as days_open
    , round(datediff('day',merge_request_created_at,first_review_date),2) as days_to_review
    , round(datediff('day',first_review_date,date_spine.date_actual),2) as days_in_review
    , PERCENT_RANK() OVER (PARTITION BY day ORDER BY days_open) AS days_open_p95
    FROM date_spine
    JOIN  first_review_date ON date_spine.date_actual BETWEEN first_review_date.first_review_date  AND
 coalesce(merged_at,current_date):: DATE

)



  SELECT distinct mr.*
  , TRY_CAST(sizes.PRODUCT_MERGE_REQUEST_FILES_CHANGED as integer) as FILES_CHANGED
  , TRY_CAST(sizes.PRODUCT_MERGE_REQUEST_LINES_ADDED as integer) as added_lines
  , TRY_CAST(sizes.PRODUCT_MERGE_REQUEST_LINES_REMOVED as integer) as removed_lines
  , max(notes.created_at)::date AS last_note_date
  FROM add_old_flag mr
  LEFT JOIN {{ref('SIZES_PART_OF_PRODUCT_MERGE_REQUESTS')}} sizes on mr.merge_request_iid = sizes.product_merge_request_iid and mr.target_project_id = sizes.product_merge_request_project_id
  LEFT JOIN notes ON mr.merge_request_id = notes.noteable_id
  WHERE mr.merge_request_id NOT IN (SELECT deleted_merge_request_id FROM {{ref('sheetload_deleted_mrs')}})       
AND old_1yr_flag = 0
-- and day = dateadd('day',-1,current_date)
-- and reviewer_count is null
  