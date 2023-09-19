
{{ simple_cte([
    ('job_info_source','blended_job_info_source'),
    ('team_member','dim_team_member')
]) }},


 job_profiles AS (

  SELECT 
    job_code,
    job_profile                                                                                                                AS position,
    job_family                                                                                                                 AS job_family,
    management_level                                                                                                           AS management_level, 
    job_level                                                                                                                  AS job_grade,
    is_job_profile_active                                                                                                      AS is_position_active
  FROM {{ref('job_profiles_snapshots_source')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY job_code ORDER BY valid_from DESC) = 1 

),

team_info AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['employee_id', 'job_title', 'department', 'division', 'reports_to','entity']) }}               AS unique_key,
    employee_id                                                                                                                AS employee_id, 
    job_title                                                                                                                  AS position,
    reports_to                                                                                                                 AS manager,
    entity                                                                                                                     AS entity, 
    department                                                                                                                 AS department,
    division                                                                                                                   AS division,
    effective_date                                                                                                             AS effective_date
  FROM job_info_source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, effective_date ORDER BY effective_date DESC) = 1

),

job_info AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['employee_id', 'job_role', 'job_grade', 'jobtitle_speciality_single_select', 'jobtitle_speciality_single_select']) }}   
                                                                                                                               AS unique_key,
    employee_id                                                                                                                AS employee_id, 
    job_role                                                                                                                   AS management_level,
    job_grade                                                                                                                  AS job_grade, 
    jobtitle_speciality_single_select                                                                                          AS job_specialty_single,
    jobtitle_speciality_single_select                                                                                          AS job_specialty_multi,
    DATE_TRUNC('day', uploaded_at)                                                                                             AS effective_date,
    LAG(unique_key, 1, NULL) OVER (PARTITION BY employee_id ORDER BY uploaded_at)                                              AS lag_unique_key,
    CONDITIONAL_TRUE_EVENT(unique_key != lag_unique_key) OVER ( PARTITION BY employee_id ORDER BY uploaded_at)                 AS unique_key_group 
  FROM {{ref('blended_employee_mapping_source')}}
  WHERE uploaded_at >= '2020-02-27'

      
),


team_member_groups AS (

  SELECT
    {{ dbt_utils.surrogate_key(['employee_id', 'team_id_current', 'manager_current','department_current','suporg_current','job_code_current', 'job_specialty_single_current', 'job_specialty_multi_current', 'entity_current']) }}
                                                                                                                               AS unique_key,
    employee_id                                                                                                                AS employee_id,
    team_id_current                                                                                                            AS team_id,
    manager_current                                                                                                            AS manager,
    department_current                                                                                                         AS department,
    suporg_current                                                                                                             AS suporg,

    /*
      We weren't capturing history of job codes and when they changed, we didn't capture it anywhere
      The following job codes from staffing_history don't exist in job_profiles so 
      we are capturing them through this case statement
    */

    CASE 
      WHEN job_code_current = 'SA.FSDN.P5' 
        THEN 'SA.FSDN.P5-SAE'                                                        
      WHEN job_code_current = 'SA.FSDN.P4' 
        THEN 'SA.FSDN.P4-SAE'
      WHEN job_code_current = 'MK.PMMF.M3-PM'
        THEN 'MK.PMMF.M4-PM'
      ELSE job_code_current
    END                                                                                                                        AS job_code,
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
    team_member_groups.job_code                                                                                                AS job_code,
    team_member_groups.job_specialty_single                                                                                    AS job_specialty_single,
    team_member_groups.job_specialty_multi                                                                                     AS job_specialty_multi,
    team_member_groups.entity                                                                                                  AS entity,   
    team_info.division                                                                                                          AS division,
    job_profiles.position                                                                                                      AS position,
    job_profiles.job_family                                                                                                    AS job_family,
    job_profiles.management_level                                                                                              AS management_level,
    job_profiles.job_grade                                                                                                     AS job_grade,
    job_profiles.is_position_active                                                                                            AS is_position_active,
    team_member_groups.effective_date                                                                                          AS effective_date
  FROM team_member_groups
  --Temporary join while we add a key to join cost_centers to staffing_history
  LEFT JOIN job_profiles
    ON job_profiles.job_code = team_member_groups.job_code
  LEFT JOIN team_info
    ON team_member_groups.employee_id = team_info.employee_id 
      AND team_member_groups.effective_date = team_info.effective_date
  
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
    team_info.manager                                                                                                          AS manager,
    NULL                                                                                                                       AS suporg,
    NULL                                                                                                                       AS job_code,
    team_info.position                                                                                                         AS position,
    NULL                                                                                                                       AS job_family,
    job_info.job_specialty_single                                                                                              AS job_specialty_single,
    job_info.job_specialty_multi                                                                                               AS job_specialty_multi,
    job_info.management_level                                                                                                  AS management_level,
    job_info.job_grade                                                                                                         AS job_grade,
    team_info.department                                                                                                       AS department,
    team_info.division                                                                                                         AS division,
    team_info.entity                                                                                                           AS entity,
    NULL                                                                                                                       AS is_position_active,
    team_info.effective_date                                                                                                   AS effective_date
  FROM job_info
  LEFT JOIN team_info
    ON job_info.employee_id = team_info.employee_id
      AND job_info.effective_date = team_info.effective_date
  WHERE job_info.effective_date < '2022-06-16'
  --Use data from blended source up to the transition from BHR to Workday
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
