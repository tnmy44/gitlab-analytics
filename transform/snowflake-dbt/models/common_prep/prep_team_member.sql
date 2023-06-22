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

team_member_groups AS (

  /*
  We need to identify and isolate groups of consecutive records that share the same country, region or team (islands)
  and the gaps between those groups

  We have used the LAG and CONDITIONAL_TRUE_EVENT window functions to assign a group number to each island and gap 
  */
  SELECT
    employee_id,
    team_id_current AS team_id,
    country_current AS country,
    region_current AS region,
    COALESCE(region, '')                                                                                                                       AS no_null_region,
    LAG(no_null_region, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                    AS lag_region,
    CONDITIONAL_TRUE_EVENT(no_null_region != lag_region) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)          AS region_group,
    COALESCE(country, '')                                                                                                                      AS no_null_country,
    LAG(no_null_country, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                   AS lag_country,
    CONDITIONAL_TRUE_EVENT(no_null_country != lag_country) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)        AS country_group,
    COALESCE(team_id, '')                                                                                                                      AS no_null_team_id,
    LAG(no_null_team_id, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                   AS lag_team_id,
    CONDITIONAL_TRUE_EVENT(no_null_team_id != lag_team_id) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)        AS team_id_group,
    effective_date                                                                                                                             AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from)                                               AS valid_to
  FROM {{ref('staffing_history_approved_source')}}

),

team_member_changes AS (

  /*
  This CTE finds the valid_from and valid_to for the team_id, region, and country group changes
  */

  SELECT 
    employee_id,
    team_id, 
    team_id_group,
    country,
    country_group,
    region,
    region_group,
    MIN(VALID_FROM) AS valid_from,
    MAX(valid_to) AS valid_to
  FROM team_member_groups
  {{ dbt_utils.group_by(n=7)}}

),

staffing_history AS (

  /*
  This CTE pulls the remaining fields we need from staffing_history_approved_source
  */

  SELECT
    employee_id,
    business_process_type,
    hire_date,
    termination_date,
    LAST_VALUE(hire_date IGNORE NULLS) OVER (PARTITION BY employee_id ORDER BY effective_date ROWS UNBOUNDED PRECEDING) AS most_recent_hire_date,
    IFF(termination_date IS NULL, TRUE, FALSE) AS is_current_team_member,
    IFF(COUNT(hire_date) OVER (PARTITION BY employee_id ORDER BY effective_date ASC ROWS UNBOUNDED PRECEDING) > 1, TRUE, FALSE) AS is_rehire, -- team member is a rehire if they have more than 1 hire_date event
    date_time_initiated,
    effective_date AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('staffing_history_approved_source')}}
  WHERE business_process_type = 'Hire' OR business_process_type = 'Termination' OR business_process_type = 'Contract Contingent Worker'

),

history_combined AS (

  /*
  This CTE combines the fields from staffing history and the fields we want to keep track of (country, region, team_id) from 
  the team_member_changes CTE
  */

  SELECT
    staffing_history.employee_id                                                AS employee_id,
    staffing_history.hire_date                                                  AS hire_date,
    staffing_history.termination_date                                           AS termination_date,
    staffing_history.most_recent_hire_date                                      AS most_recent_hire_date,
    team_member_changes.team_id                                                 AS team_id,
    team_member_changes.country                                                 AS country,
    team_member_changes.region                                                  AS region,
    staffing_history.is_current_team_member                                     AS is_current_team_member,
    staffing_history.is_rehire                                                  AS is_rehire, 
    staffing_history.date_time_initiated                                        AS date_time_initiated,
    GREATEST(team_member_changes.valid_from, staffing_history.valid_from)       AS valid_from,
    LEAST(team_member_changes.valid_to, staffing_history.valid_to)              AS valid_to
  FROM staffing_history
  LEFT JOIN team_member_changes
    ON team_member_changes.employee_id = staffing_history.employee_id 
      AND NOT (team_member_changes.valid_to <= staffing_history.valid_from
        OR team_member_changes.valid_from >= staffing_history.valid_to)

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
  FROM history_combined

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
    COALESCE(history_combined.country, 'Unknown Country')                                                   AS country,
    COALESCE(history_combined.region, 'Unknown Region')                                                     AS region,
    history_combined.team_id                                                                                AS team_id,
    history_combined.most_recent_hire_date                                                                  AS hire_date,
    history_combined.termination_date                                                                       AS termination_date,
    history_combined.is_current_team_member                                                                 AS is_current_team_member,
    history_combined.is_rehire                                                                              AS is_rehire,
    history_combined.date_time_initiated                                                                    AS date_time_initiated,
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
        AND NOT (key_talent.valid_to <= date_range.valid_from
          OR key_talent.valid_from >= date_range.valid_to)
    LEFT JOIN gitlab_usernames
      ON gitlab_usernames.employee_id = date_range.employee_id 
        AND NOT (gitlab_usernames.valid_to <= date_range.valid_from
          OR gitlab_usernames.valid_from >= date_range.valid_to)
            AND gitlab_usernames.valid_from != gitlab_usernames.valid_to
    LEFT JOIN performance_growth_potential
      ON performance_growth_potential.employee_id = date_range.employee_id 
        AND NOT (performance_growth_potential.valid_to <= date_range.valid_from
          OR performance_growth_potential.valid_from >= date_range.valid_to)
    LEFT JOIN history_combined
      ON history_combined.employee_id = date_range.employee_id 
        AND NOT (history_combined.valid_to <= date_range.valid_from
          OR history_combined.valid_from >= date_range.valid_to)

)

SELECT *
FROM final
