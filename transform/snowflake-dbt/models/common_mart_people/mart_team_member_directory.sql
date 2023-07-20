WITH team_member_mapping AS (

  SELECT 
    employee_id,
    cost_center,
    age,
    CASE 
      WHEN age BETWEEN 18 AND 24  THEN '18-24'
      WHEN age BETWEEN 25 AND 29  THEN '25-29'
      WHEN age BETWEEN 30 AND 34  THEN '30-34'
      WHEN age BETWEEN 35 AND 39  THEN '35-39'
      WHEN age BETWEEN 40 AND 44  THEN '40-44'
      WHEN age BETWEEN 44 AND 49  THEN '44-49'
      WHEN age BETWEEN 50 AND 54  THEN '50-54'
      WHEN age BETWEEN 55 AND 59  THEN '55-59'
      WHEN age >= 60              THEN '60+'
      WHEN age IS NULL            THEN 'Unreported'
      WHEN age = -1               THEN 'Unreported'
      ELSE NULL 
    END                                                                                             AS age_cohort,
    MIN(DATE(valid_from))                                                                           AS valid_from,
    MAX(DATE(valid_to))                                                                             AS valid_to 
  FROM {{ ref('workday_employee_mapping_source') }} 
  {{ dbt_utils.group_by(n=4)}}
  
),

team_member AS (

  SELECT 
    dim_team_member_sk,
    employee_id,
    nationality,
    ethnicity,
    first_name,
    last_name,
    first_name || ' ' || last_name                                                                  AS full_name,
    gender,
    work_email,
    date_of_birth,
    gitlab_username,
    team_id,
    country,
    region,
    CASE
      WHEN region = 'Americas' AND country IN ('United States', 'Canada','Mexico','United States of America') 
        THEN 'NORAM'
      WHEN region = 'Americas' AND country NOT IN ('United States', 'Canada','Mexico','United States of America') 
        THEN 'LATAM'
      ELSE region END                                                                               AS region_modified,
    IFF(country IN ('United States','United States of America'), 
      COALESCE(gender,'Did Not Identify')  || '_' || 'United States of America', 
      COALESCE(gender,'Did Not Identify')  || '_'|| 'Non-US')                                       AS gender_region,
    IFF(country IN ('United States','United States of America'), 
      COALESCE(ethnicity,'Did Not Identify')  || '_' || 'United States of America', 
      COALESCE(ethnicity,'Did Not Identify')  || '_'|| 'Non-US')                                    AS ethnicity_region,
    CASE
      WHEN COALESCE(ethnicity, 'Did Not Identify') NOT IN ('White','Asian','Did Not Identify','Declined to Answer')
          THEN TRUE
      ELSE FALSE END                                                                                AS urg_group,
    IFF(urg_group = TRUE, 'URG', 'Non-URG')  || '_' ||
        IFF(country IN ('United States','United States of America'),
          'United States of America',
          'Non-US')                                                                                 AS urg_region,
    hire_date,
    termination_date,
    is_current_team_member,
    is_rehire,
    valid_from,
    valid_to
  FROM {{ ref('dim_team_member') }}

),

team_member_position AS (

  SELECT 
    dim_team_member_sk,
    dim_team_sk,
    employee_id,
    team_id,
    manager,
    suporg,
    job_code,
    position,
    job_family,
    job_specialty_single,
    job_specialty_multi,
    management_level,
    job_grade,
    department,
    division,
    entity,
    effective_date                                                                                AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}})  OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ ref('fct_team_member_position') }}


), 

unioned_dates AS (

  SELECT 
    employee_id, 
    NULL AS team_id,
    valid_from
  FROM team_member_mapping
  
  UNION

  SELECT 
    employee_id, 
    team_id,
    valid_from
  FROM team_member
  
  UNION

  SELECT 
    employee_id, 
    team_id,
    valid_from
  FROM team_member_position

),

date_range AS (

  SELECT 
    employee_id,
    team_id,
    valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to,
    IFF(valid_to = {{var('tomorrow')}}, TRUE, FALSE) AS is_current
  FROM unioned_dates
  
),

final AS (

  SELECT 

    -- Surrogate keys
    team_member.dim_team_member_sk,
    {{ dbt_utils.surrogate_key(['team_member.team_id']) }}                                   AS dim_team_sk,

    --Natural keys
    team_member.employee_id,

    --Team member info
    team_member.nationality,
    team_member.ethnicity,
    team_member.first_name,
    team_member.last_name,
    team_member.full_name,
    team_member.gender,
    team_member.work_email,
    team_member.date_of_birth,
    team_member_mapping.age,
    team_member_mapping.age_cohort,
    team_member_mapping.cost_center,
    team_member.gitlab_username,
    team_member.country,
    team_member.region,
    team_member.region_modified,
    team_member.gender_region,
    team_member.ethnicity_region,
    team_member.urg_group,
    team_member.urg_region,
    team_member.hire_date,
    team_member.termination_date,
    team_member.is_current_team_member,
    team_member.is_rehire,
    team_member.team_id,
    team_member_position.manager                                          AS team_manager_name,
    team_member_position.department                                       AS department,
    LAST_VALUE(team_member_position.division IGNORE NULLS) OVER (PARTITION BY date_range.employee_id ORDER BY date_range.valid_from ROWS UNBOUNDED PRECEDING)           
                                                                          AS division,
    team_member_position.suporg,
    team_member_position.job_code,
    team_member_position.position,
    team_member_position.job_family,
    team_member_position.job_specialty_single,
    team_member_position.job_specialty_multi,
    team_member_position.management_level,
    team_member_position.job_grade,
    team_member_position.entity,
    date_range.valid_from,
    date_range.valid_to,
    date_range.is_current
  FROM team_member
  INNER JOIN date_range
    ON date_range.employee_id = team_member.employee_id 
      AND date_range.valid_from != date_range.valid_to
        AND NOT (team_member.valid_to <= date_range.valid_from
          OR team_member.valid_from >= date_range.valid_to)
  LEFT JOIN team_member_mapping
    ON date_range.employee_id = team_member_mapping.employee_id 
      AND NOT (team_member_mapping.valid_to <= date_range.valid_from
          OR team_member_mapping.valid_from >= date_range.valid_to)
  LEFT JOIN team_member_position
    ON date_range.employee_id = team_member_position.employee_id 
      AND NOT (team_member_position.valid_to <= date_range.valid_from
          OR team_member_position.valid_from >= date_range.valid_to)

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2023-07-06",
    updated_date="2023-07-20"
) }}
