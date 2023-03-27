WITH all_team_members AS (

  SELECT * 
  FROM {{ref('all_workers_source')}}

),

blended_dates AS (

  SELECT * 
  FROM {{ref('blended_dates')}}

),

key_talent AS (

  SELECT * 
  FROM {{ref('assess_talent_source')}}

),

gitlab_usernames AS (

  SELECT * 
  FROM {{ref('gitlab_usernames_source')}}
),

performance_growth_potential AS (

  SELECT * 
  FROM {{ref('performance_growth_potential_source')}}

), 

staffing_history AS (

  SELECT * 
  FROM {{ref('staffing_history_approved_source')}}

),

final AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['blended_dates.employee_id', 'blended_dates.valid_from']) }}                AS dim_team_member_sk,
    blended_dates.employee_id                                                                               AS employee_id,
    blended_dates.business_process                                                                          AS business_process,
    all_team_members.nationality                                                                            AS nationality,
    all_team_members.ethnicity                                                                              AS ethnicity,
    all_team_members.preferred_first_name                                                                   AS first_name,
    all_team_members.preferred_last_name                                                                    AS last_name,
    all_team_members.gender                                                                                 AS gender,
    all_team_members.work_email                                                                             AS work_email,
    all_team_members.date_of_birth                                                                          AS date_of_birth,
    key_talent.key_talent                                                                                   AS key_talent_status,
    gitlab_usernames.gitlab_username                                                                        AS gitlab_username,
    performance_growth_potential.growth_potential_rating                                                    AS growth_potential_rating,
    performance_growth_potential.performance_rating                                                         AS performance_rating,
    staffing_history.current_country                                                                        AS country,
    staffing_history.current_region                                                                         AS region,
    staffing_history.hire_date                                                                              AS hire_date,
    staffing_history.termination_date                                                                       AS termination_date,
    blended_dates.valid_from                                                                                AS valid_from,
    blended_dates.valid_to                                                                                  AS valid_to,
    blended_dates.row_num                                                                                   AS event_sequence
  FROM blended_dates
  INNER JOIN key_talent
      ON key_talent.employee_id = blended_dates.employee_id 
      AND key_talent.effective_date = blended_dates.key_talent_valid_from
  INNER JOIN gitlab_usernames
    ON gitlab_usernames.employee_id = blended_dates.employee_id 
    AND gitlab_usernames.date_time_completed = blended_dates.usernames_valid_from
  INNER JOIN performance_growth_potential
    ON performance_growth_potential.employee_id = blended_dates.employee_id 
    AND performance_growth_potential.review_period_end_date = blended_dates.performance_growth_potential_valid_from
  INNER JOIN staffing_history
    ON staffing_history.employee_id = blended_dates.employee_id 
    AND staffing_history.effective_date = blended_dates.staffing_history_valid_from
  INNER JOIN all_team_members
    ON all_team_members.employee_id = blended_dates.employee_id 

)

SELECT *
FROM final
