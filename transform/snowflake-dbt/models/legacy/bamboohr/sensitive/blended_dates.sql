WITH key_talent AS (

  SELECT
    t1.employee_id,
    t1.effective_date AS valid_from,
    COALESCE(MIN(t2.effective_date), {{ var('tomorrow') }}) AS valid_to
  FROM {{ref('assess_talent_source')}} t1
  LEFT JOIN {{ref('assess_talent_source')}} t2
    ON t1.employee_id = t2.employee_id
    AND t1.effective_date < t2.effective_date
  GROUP BY 1, 2

),

gitlab_usernames AS (

  SELECT
    t1.employee_id,
    t1.date_time_completed AS valid_from,
    COALESCE(MIN(t2.date_time_completed), {{ var('tomorrow') }}) AS valid_to
  FROM {{ref('gitlab_usernames_source')}} t1
  LEFT JOIN {{ref('gitlab_usernames_source')}} t2
    ON t1.employee_id = t2.employee_id
    AND t1.date_time_completed < t2.date_time_completed
  GROUP BY 1, 2

),

performance_growth_potential AS (

  SELECT
    t1.employee_id,
    t1.review_period_end_date AS valid_from,
    COALESCE(MIN(t2.review_period_end_date), {{ var('tomorrow') }}) AS valid_to
  FROM {{ref('performance_growth_potential_source')}} t1
  LEFT JOIN {{ref('performance_growth_potential_source')}} t2
    ON t1.employee_id = t2.employee_id
    AND t1.review_period_end_date < t2.review_period_end_date
  GROUP BY 1, 2

), 

staffing_history AS (

  SELECT
    t1.employee_id,
    t1.business_process_type,
    t1.effective_date AS valid_from,
    COALESCE(MIN(t2.effective_date), {{ var('tomorrow') }}) AS valid_to
  FROM {{ref('staffing_history_approved_source')}} t1
  LEFT JOIN {{ref('staffing_history_approved_source')}} t2
    ON t1.employee_id = t2.employee_id
    AND t1.effective_date < t2.effective_date
    AND t1.business_process_type = t2.business_process_type
  GROUP BY 1, 2, 3

),

unioned AS (

  SELECT 
    employee_id,
    'Key Talent Assessment' AS business_process,
    valid_from
  FROM key_talent

  UNION

  SELECT 
    employee_id,
    'Change GitLab Username' AS business_process,
    valid_from
  FROM gitlab_usernames

  UNION 

  SELECT 
    employee_id,
    'Key Talent Assessment' AS business_process,
    valid_from
  FROM performance_growth_potential

  UNION 

  SELECT 
    employee_id,
    business_process_type AS business_process,
    valid_from
  FROM staffing_history

),

final AS (

  SELECT 
    unioned.employee_id                          AS employee_id,
    unioned.business_process                     AS business_process,
    key_talent.valid_from                        AS key_talent_valid_from,
    key_talent.valid_to                          AS key_talent_valid_to,
    gitlab_usernames.valid_from                  AS usernames_valid_from,
    gitlab_usernames.valid_to                    AS usernames_valid_to,
    performance_growth_potential.valid_from      AS performance_growth_potential_valid_from,
    performance_growth_potential.valid_to        AS performance_growth_potential_valid_to,
    staffing_history.valid_from                  AS staffing_history_valid_from,
    staffing_history.valid_to                    AS staffing_history_valid_to,
    unioned.valid_from                           AS valid_from,
    COALESCE(LEAD(unioned.valid_from) OVER (PARTITION BY unioned.employee_id, unioned.business_process ORDER BY unioned.valid_from), {{var('tomorrow')}}) AS valid_to,
    ROW_NUMBER() OVER (PARTITION BY unioned.employee_id, unioned.business_process ORDER BY unioned.valid_from) AS row_num
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
