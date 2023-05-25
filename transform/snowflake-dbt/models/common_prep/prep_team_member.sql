WITH all_team_members AS (

  SELECT * 
  FROM {{ref('all_workers_source')}}

),

key_talent AS (

  SELECT
    employee_id,
    key_talent,
    effective_date AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}})  OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('assess_talent_source')}}

),

gitlab_usernames AS (

  SELECT
    employee_id,
    gitlab_username,
    effective_date AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}})  OVER (PARTITION BY employee_id ORDER BY valid_from, date_time_completed) AS valid_to
  FROM {{ref('gitlab_usernames_source')}}

),

performance_growth_potential AS (

  SELECT
    employee_id,
    growth_potential_rating,
    performance_rating,
    review_period_end_date AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}})  OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('performance_growth_potential_source')}}

), 

staffing_history AS (

  SELECT
    employee_id,
    business_process_type,
    hire_date,
    termination_date,
    LAST_VALUE(hire_date IGNORE NULLS) OVER (PARTITION BY employee_id ORDER BY effective_date ROWS UNBOUNDED PRECEDING) AS most_recent_hire_date,
    current_country AS country,
    current_region AS region,
    IFF(termination_date IS NULL, TRUE, FALSE) AS is_current_team_member,
    IFF(COUNT(hire_date) OVER (PARTITION BY employee_id ORDER BY effective_date ASC ROWS UNBOUNDED PRECEDING) > 1, TRUE, FALSE) AS is_rehire, -- team member is a rehire if they have more than 1 hire_date event
    effective_date AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('staffing_history_approved_source')}}
  WHERE business_process_type = 'Hire' OR business_process_type = 'Termination' OR business_process_type = 'Contract Contingent Worker'

),

unioned AS (

  /*
    We union all valid_from dates from each type 2 SCD table (except the type 1 SCD - all_team_members)
    to create a date spine that we can then use to join our events into
  */

  SELECT 
    employee_id,
    valid_from AS unioned_dates
  FROM key_talent

  UNION

  SELECT 
    employee_id,
    valid_from
  FROM gitlab_usernames

  UNION 

  SELECT 
    employee_id,
    valid_from
  FROM performance_growth_potential

  UNION

  SELECT 
    employee_id,
    valid_from
  FROM staffing_history

),

date_range AS (

  SELECT 
    employee_id,
    unioned_dates AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to,
    IFF(valid_to = {{var('tomorrow')}}, TRUE, FALSE) AS is_current
  FROM unioned
  
),

final AS (

 SELECT 
    {{ dbt_utils.surrogate_key(['all_team_members.employee_id'])}}                                          AS dim_team_member_sk,
    all_team_members.employee_id                                                                            AS employee_id,
    COALESCE(all_team_members.nationality, 'Unknown Nationality')                                           AS nationality,
    COALESCE(all_team_members.ethnicity, 'Unknown Ethnicity')                                               AS ethnicity,
    COALESCE(all_team_members.preferred_first_name, 'Unknown First Name')                                   AS first_name,
    COALESCE(all_team_members.preferred_last_name, 'Unknown Last Name')                                     AS last_name,
    COALESCE(all_team_members.gender, 'Unknown Gender')                                                     AS gender,
    COALESCE(all_team_members.work_email, 'Unknown Work Email')                                             AS work_email,
    all_team_members.date_of_birth                                                                          AS date_of_birth,
    COALESCE(key_talent.key_talent, 'Unknown Yes/No Status')                                                AS key_talent_status,
    COALESCE(gitlab_usernames.gitlab_username, 'Unknown Username')                                          AS gitlab_username,
    COALESCE(performance_growth_potential.growth_potential_rating, 'Unknown Rating')                        AS growth_potential_rating,
    COALESCE(performance_growth_potential.performance_rating, 'Unknown Rating')                             AS performance_rating,
    COALESCE(staffing_history.country, 'Unknown Country')                                                   AS country,
    COALESCE(staffing_history.region, 'Unknown Region')                                                     AS region,
    staffing_history.most_recent_hire_date                                                                  AS hire_date,
    staffing_history.termination_date                                                                       AS termination_date,
    staffing_history.is_current_team_member                                                                 AS is_current_team_member,
    staffing_history.is_rehire                                                                              AS is_rehire,
    date_range.valid_from                                                                                   AS valid_from,
    date_range.valid_to                                                                                     AS valid_to,
    date_range.is_current                                                                                   AS is_current
    FROM all_team_members
    INNER JOIN date_range
      ON date_range.employee_id = all_team_members.employee_id 

    /*
      A team member event is matched to a date interval if there is any overlap between the two, 
      this happens when the team member event begins before the date range ends 
      and the team member event ends after the range begins 
    */

    LEFT JOIN key_talent
      ON key_talent.employee_id = date_range.employee_id 
        AND (
          CASE
            WHEN date_range.valid_from >= key_talent.valid_from AND date_range.valid_from < key_talent.valid_to THEN TRUE
            WHEN date_range.valid_to > key_talent.valid_from AND date_range.valid_to <= key_talent.valid_to THEN TRUE
            WHEN key_talent.valid_from >= date_range.valid_from AND key_talent.valid_from < date_range.valid_to THEN TRUE
            ELSE FALSE
          END) = TRUE
    LEFT JOIN gitlab_usernames
      ON gitlab_usernames.employee_id = date_range.employee_id 
        AND (
          CASE
            WHEN date_range.valid_from >= gitlab_usernames.valid_from AND date_range.valid_from < gitlab_usernames.valid_to THEN TRUE
            WHEN date_range.valid_to > gitlab_usernames.valid_from AND date_range.valid_to <= gitlab_usernames.valid_to THEN TRUE
            WHEN gitlab_usernames.valid_from >= date_range.valid_from AND gitlab_usernames.valid_from < date_range.valid_to THEN TRUE
            ELSE FALSE
          END) = TRUE
        AND gitlab_usernames.valid_from != gitlab_usernames.valid_to
    LEFT JOIN performance_growth_potential
      ON performance_growth_potential.employee_id = date_range.employee_id 
        AND (
          CASE
            WHEN date_range.valid_from >= performance_growth_potential.valid_from AND date_range.valid_from < performance_growth_potential.valid_to THEN TRUE
            WHEN date_range.valid_to > performance_growth_potential.valid_from AND date_range.valid_to <= performance_growth_potential.valid_to THEN TRUE
            WHEN performance_growth_potential.valid_from >= date_range.valid_from AND performance_growth_potential.valid_from < date_range.valid_to THEN TRUE
            ELSE FALSE
          END) = TRUE
    LEFT JOIN staffing_history
      ON staffing_history.employee_id = date_range.employee_id 
        AND (
          CASE
            WHEN date_range.valid_from >= staffing_history.valid_from AND date_range.valid_from < staffing_history.valid_to THEN TRUE
            WHEN date_range.valid_to > staffing_history.valid_from AND date_range.valid_to <= staffing_history.valid_to THEN TRUE
            WHEN staffing_history.valid_from >= date_range.valid_from AND staffing_history.valid_from < date_range.valid_to THEN TRUE
            ELSE FALSE
          END) = TRUE

)

SELECT *
FROM final
