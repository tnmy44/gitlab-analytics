WITH job_profiles AS (

  SELECT 
    /*
      We weren't capturing history of job codes and when they changed, we didn't capture it anywhere
      The following job codes from staffing_history don't exist in job_profiles so 
      we are capturing them through this case statement
    */

    CASE 
      WHEN job_code = 'SA.FSDN.P5' 
        THEN job_code = 'SA.FSDN.P5-SAE'                                                        
      WHEN job_code = 'SA.FSDN.P4' 
        THEN job_code = 'SA.FSDN.P4-SAE'
      WHEN job_code = 'MK.PMMF.M3-PM'
        THEN  job_code = 'MK.PMMF.M4-PM'
      ELSE job_code
    END                                                                                                                        AS job_code,
    job_profile                                                                                                                AS position,
    job_family                                                                                                                 AS job_family,
    management_level                                                                                                           AS management_level, 
    job_level                                                                                                                  AS job_grade,
    is_job_profile_active                                                                                                      AS is_position_active
  FROM {{ref('job_profiles_snapshots_source')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY job_code ORDER BY valid_from DESC) = 1 

),

job_info AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['employee_id', 'job_title', 'reports_to','department','division','entity']) }}                 AS unique_key,
    employee_id                                                                                                                AS employee_id, 
    job_title                                                                                                                  AS position,
    reports_to                                                                                                                 AS manager,
    department                                                                                                                 AS department, 
    division                                                                                                                   AS division,
    entity                                                                                                                     AS entity, 
    effective_date                                                                                                             AS effective_date
  FROM {{ref('blended_job_info_source')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY effective_date DESC) = 1

),

team_member_groups AS (

  SELECT
    {{ dbt_utils.surrogate_key(['employee_id', 'team_id_current', 'manager_current','department_current','suporg_current','employee_type_current','job_code_current', 'job_specialty_single_current', 'job_specialty_multi_current', 'entity_current']) }}
                                                                                                                               AS unique_key,
    employee_id                                                                                                                AS employee_id,
    team_id_current                                                                                                            AS team_id,
    manager_current                                                                                                            AS manager,
    department_current                                                                                                         AS department,
    suporg_current                                                                                                             AS suporg,
    employee_type_current                                                                                                      AS employee_type,
    job_code_current                                                                                                           AS job_code,
    job_specialty_single_current                                                                                               AS job_specialty_single,
    job_specialty_multi_current                                                                                                AS job_specialty_multi,
    entity_current                                                                                                             AS entity,
    effective_date                                                                                                             AS effective_date
  FROM {{ref('staffing_history_approved_source')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY date_time_initiated DESC)  = 1 

),

staffing_history AS (

  SELECT 
    team_member_groups.employee_id                                                                                             AS employee_id,
    team_member_groups.team_id                                                                                                 AS team_id,
    team_member_groups.manager                                                                                                 AS manager,
    team_member_groups.department                                                                                              AS department,
    team_member_groups.suporg                                                                                                  AS suporg,
    team_member_groups.employee_type                                                                                           AS employee_type,
    team_member_groups.job_code                                                                                                AS job_code,
    team_member_groups.job_specialty_single                                                                                    AS job_specialty_single,
    team_member_groups.job_specialty_multi                                                                                     AS job_specialty_multi,
    team_member_groups.entity                                                                                                  AS entity,
    job_info.division                                                                                                          AS division,
    job_profiles.position                                                                                                      AS position,
    job_profiles.job_family                                                                                                    AS job_family,
    job_profiles.management_level                                                                                              AS management_level,
    job_profiles.job_grade                                                                                                     AS job_grade,
    job_profiles.is_position_active                                                                                            AS is_position_active,
    team_member_groups.effective_date                                                                                          AS effective_date
  FROM team_member_groups
  LEFT JOIN job_profiles
    ON job_profiles.job_code = team_member_groups.job_code
  --Temporary join while we add a key to join cost_centers to staffing_history
  LEFT JOIN job_info
    ON team_member_groups.employee_id = job_info.employee_id 
      AND team_member_groups.effective_date = job_info.effective_date
  
),

final AS (

  SELECT 

    -- Surrogate keys
    {{ dbt_utils.surrogate_key(['staffing_history.employee_id'])}}                                                             AS dim_team_member_sk,
    {{ dbt_utils.surrogate_key(['staffing_history.team_id'])}}                                                                 AS dim_team_sk,

    -- Team member position attributes
    staffing_history.employee_id                                                                                               AS employee_id,
    staffing_history.team_id                                                                                                   AS team_id,
    staffing_history.manager                                                                                                   AS manager,
    staffing_history.suporg                                                                                                    AS suporg,
    staffing_history.job_code                                                                                                  AS job_code,
    staffing_history.position                                                                                                  AS position,
    staffing_history.job_family                                                                                                AS job_family,
    staffing_history.job_specialty_single                                                                                      AS job_specialty_single,
    staffing_history.job_specialty_multi                                                                                       AS job_specialty_multi,
    staffing_history.management_level                                                                                          AS management_level,
    staffing_history.job_grade                                                                                                 AS job_grade,
    staffing_history.department                                                                                                AS department,
    staffing_history.division                                                                                                  AS division,
    staffing_history.entity                                                                                                    AS entity,
    staffing_history.is_position_active                                                                                        AS is_position_active,
    staffing_history.effective_date                                                                                            AS effective_date
  FROM staffing_history
  WHERE staffing_history.effective_date >= '2022-06-16'

  UNION

  SELECT 
    -- Surrogate keys
    {{ dbt_utils.surrogate_key(['job_info.employee_id'])}}                                                                     AS dim_team_member_sk,                                                                                                              
    NULL                                                                                                                       AS dim_team_sk,

    -- Team member position attributes
    job_info.employee_id                                                                                                       AS employee_id,
    NULL                                                                                                                       AS team_id,
    job_info.manager                                                                                                           AS manager,
    NULL                                                                                                                       AS suporg,
    NULL                                                                                                                       AS job_code,
    job_info.position                                                                                                          AS position,
    NULL                                                                                                                       AS job_family,
    NULL                                                                                                                       AS job_specialty_single,
    NULL                                                                                                                       AS job_specialty_multi,
    NULL                                                                                                                       AS management_level,
    NULL                                                                                                                       AS job_grade,
    job_info.department                                                                                                        AS department,
    job_info.division                                                                                                          AS division,
    job_info.entity                                                                                                            AS entity,
    NULL                                                                                                                       AS is_position_active,
    job_info.effective_date                                                                                                    AS effective_date
  FROM job_info
  WHERE job_info.effective_date < '2022-06-16'
  --Use data from blended job info source up to the transition from BHR to Workday
  --Job profiles didn't exist in BHR

)

SELECT 
  *,
  CASE 
    WHEN ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date DESC) = 1 
      THEN TRUE
    ELSE FALSE
  END                                                                                                                          AS is_current
FROM final
