WITH key_talent AS (

  SELECT
    employee_id,
    effective_date AS valid_from,
    LEAD(valid_from, 1, DATEADD('day',1,CURRENT_DATE())) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('assess_talent_source')}}
  
),

gitlab_usernames AS (

   SELECT
    employee_id,
    date_time_completed AS valid_from,
    LEAD(valid_from, 1, DATEADD('day',1,CURRENT_DATE())) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('gitlab_usernames_source')}}

),

performance_growth_potential AS (

  SELECT
    employee_id,
    review_period_end_date AS valid_from,
    LEAD(valid_from, 1, DATEADD('day',1,CURRENT_DATE())) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('performance_growth_potential_source')}}

), 

staffing_history AS (

  SELECT
    employee_id,
    effective_date AS valid_from,
    LEAD(valid_from, 1, DATEADD('day',1,CURRENT_DATE())) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('staffing_history_approved_source')}}

),

unioned AS (

  SELECT 
    employee_id,
    valid_from
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

final AS (

  SELECT 
    unioned.employee_id                                                                                                         AS employee_id,
    key_talent.valid_from                                                                                                       AS key_talent_valid_from,
    key_talent.valid_to                                                                                                         AS key_talent_valid_to,
    gitlab_usernames.valid_from                                                                                                 AS usernames_valid_from,
    gitlab_usernames.valid_to                                                                                                   AS usernames_valid_to,
    performance_growth_potential.valid_from                                                                                     AS performance_growth_potential_valid_from,
    performance_growth_potential.valid_to                                                                                       AS performance_growth_potential_valid_to,
    staffing_history.valid_from                                                                                                 AS staffing_history_valid_from,
    staffing_history.valid_to                                                                                                   AS staffing_history_valid_to,
    unioned.valid_from                                                                                                          AS valid_from,
    COALESCE(LEAD(unioned.valid_from) OVER (PARTITION BY unioned.employee_id ORDER BY unioned.valid_from), {{var('tomorrow')}}) AS valid_to,
    ROW_NUMBER() OVER (PARTITION BY unioned.employee_id ORDER BY unioned.valid_from)                                            AS row_num
    FROM unioned
    INNER JOIN key_talent
      ON key_talent.employee_id = unioned.employee_id 
      AND key_talent.valid_from <= unioned.valid_from
      AND key_talent.valid_to > unioned.valid_from
    INNER JOIN gitlab_usernames
      ON gitlab_usernames.employee_id = unioned.employee_id 
      AND gitlab_usernames.valid_from <= unioned.valid_from
      AND gitlab_usernames.valid_to > unioned.valid_from
    INNER JOIN performance_growth_potential
      ON performance_growth_potential.employee_id = unioned.employee_id 
      AND performance_growth_potential.valid_from <= unioned.valid_from
      AND performance_growth_potential.valid_to > unioned.valid_from
    INNER JOIN staffing_history
      ON staffing_history.employee_id = unioned.employee_id 
      AND staffing_history.valid_from <= unioned.valid_from
      AND staffing_history.valid_to > unioned.valid_from

)

SELECT *
FROM final
