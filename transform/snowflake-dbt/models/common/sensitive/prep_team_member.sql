WITH all_team_members AS (

  SELECT * 
  FROM {{ref('all_workers_source')}}

),

key_talent AS (

  SELECT
    employee_id,
    key_talent,
    effective_date              AS valid_from,
    COALESCE(LEAD(valid_from) OVER (PARTITION BY employee_id ORDER BY valid_from),DATEADD('day',1,CURRENT_DATE())) AS valid_to
  FROM {{ref('assess_talent_source')}}

),

gitlab_usernames AS (


  SELECT
    employee_id,
    gitlab_username,
    effective_date              AS valid_from,
    COALESCE(LEAD(valid_from) OVER (PARTITION BY employee_id ORDER BY valid_from),DATEADD('day',1,CURRENT_DATE())) AS valid_to
  FROM {{ref('gitlab_usernames_source')}}

),

performance_growth_potential AS (

  SELECT
    employee_id,
    growth_potential_rating,
    performance_rating,
    review_period_end_date      AS valid_from,
    COALESCE(LEAD(valid_from) OVER (PARTITION BY employee_id ORDER BY valid_from),DATEADD('day',1,CURRENT_DATE())) AS valid_to
  FROM {{ref('performance_growth_potential_source')}}

), 

staffing_history AS (

  SELECT
    employee_id,
    business_process_type,
    hire_date,
    termination_date,
    current_country             AS country,
    current_region              AS region,
    effective_date              AS valid_from,
    COALESCE(LEAD(valid_from) OVER (PARTITION BY employee_id ORDER BY valid_from),DATEADD('day',1,CURRENT_DATE())) AS valid_to
  FROM {{ref('staffing_history_approved_source')}}

),

unioned AS (

  /*
    We union all valid_from and valid_to dates from each type 2 SCD table (except the type 1 SCD - all_team_members)
    to create a date spine that we can then use to join our events into
  */

  SELECT 
    employee_id AS unioned_dates,
    valid_from
  FROM key_talent

  UNION

  SELECT 
    employee_id,
    valid_to
  FROM key_talent

  UNION

  SELECT 
    employee_id,
    valid_from
  FROM gitlab_usernames

  UNION 

  SELECT 
    employee_id,
    valid_to
  FROM gitlab_usernames

  UNION

  SELECT 
    employee_id,
    valid_from
  FROM performance_growth_potential

  UNION 

  SELECT 
    employee_id,
    valid_to
  FROM performance_growth_potential

  UNION

  SELECT 
    employee_id,
    valid_from
  FROM staffing_history

  UNION

  SELECT 
    employee_id,
    valid_to
  FROM staffing_history

),

date_range AS (

  SELECT 
    employee_id,
    unioned_dates AS valid_from,
    lead(valid_from,1) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM unioned
  
),


final AS (

 SELECT 
    -- surrogate key
    {{ dbt_utils.surrogate_key(['all_team_members.employee_id', 'date_range.valid_from']) }}                AS dim_team_member_sk,
    -- The table should return at most one row per employee and date range

    all_team_members.employee_id                                                                            AS employee_id,
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
    staffing_history.business_process_type                                                                  AS business_process_type,
    staffing_history.country                                                                                AS country,
    staffing_history.region                                                                                 AS region,
    staffing_history.hire_date                                                                              AS hire_date,
    staffing_history.termination_date                                                                       AS termination_date,
    date_range.valid_from,
    date_range.valid_to
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
      AND key_talent.valid_to > date_range.valid_from
      AND key_talent.valid_from < date_range.valid_to
    LEFT JOIN gitlab_usernames
      ON gitlab_usernames.employee_id = date_range.employee_id 
      AND gitlab_usernames.valid_to > date_range.valid_from
      AND gitlab_usernames.valid_from < date_range.valid_to
    LEFT JOIN performance_growth_potential
      ON performance_growth_potential.employee_id = date_range.employee_id 
      AND performance_growth_potential.valid_to > date_range.valid_from        
      AND performance_growth_potential.valid_from < date_range.valid_to
    LEFT JOIN staffing_history
      ON staffing_history.employee_id = date_range.employee_id 
      AND staffing_history.valid_to > date_range.valid_from
      AND staffing_history.valid_from < date_range.valid_to
    WHERE date_range.valid_to IS NOT NULL

)

SELECT *
FROM final
