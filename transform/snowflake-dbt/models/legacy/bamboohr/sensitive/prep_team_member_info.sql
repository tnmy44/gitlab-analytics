WITH all_workers AS (

  SELECT *
  FROM {{ ref('all_workers_source') }}

),

gitlab_usernames AS (

  SELECT *
  FROM {{ ref('gitlab_usernames_source') }}

),


gitlab_usernames_no_gaps AS (

  SELECT
    t1.employee_id,
    t1.gitlab_username,
    t1.date_time_completed                                AS valid_from,
    COALESCE(MIN(t2.date_time_completed), NULL)           AS valid_to
  FROM gitlab_usernames t1
  LEFT JOIN gitlab_usernames t2
    ON t1.employee_id = t2.employee_id
    AND t1.date_time_completed < t2.date_time_completed
  {{ dbt_utils.group_by(n=3)}}

),

final AS (

  SELECT
    all_workers.employee_id,
    all_workers.nationality,
    all_workers.ethnicity,
    all_workers.preferred_first_name,
    all_workers.preferred_last_name,
    all_workers.gender,
    all_workers.work_email,
    all_workers.date_of_birth,
    gitlab_usernames.gitlab_username,
    gitlab_usernames.valid_from,
    gitlab_usernames.valid_to
  FROM all_workers 
  LEFT JOIN gitlab_usernames_no_gaps AS gitlab_usernames 
    ON gitlab_usernames.employee_id = all_workers.employee_id

)

SELECT *
FROM final
