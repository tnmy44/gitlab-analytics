WITH job_profiles AS (

  SELECT 
    job_code,
    job_profile                AS position,
    job_family,
    management_level, 
    job_level                  AS job_grade,
    is_job_profile_active      AS is_position_active
  FROM {{ref('job_profiles_source')}}

),

team_member_groups AS (

  /*
  We need to identify and isolate groups of consecutive records that share the same fields (islands)
  and the gaps between those groups

  We have used the LAG and CONDITIONAL_TRUE_EVENT window functions to assign a group number to each island and gap 
  */
  SELECT
    employee_id,
    team_id_current                                                                                                                                                    AS team_id,
    manager_current                                                                                                                                                    AS manager,
    department_current                                                                                                                                                 AS department,
    TRIM(SPLIT_PART(SUPORG_CURRENT, '(', 0))                                                                                                                           AS suporg,
    employee_type_current                                                                                                                                              AS employee_type,
    job_code_current                                                                                                                                                   AS job_code,
    job_specialty_single_current                                                                                                                                       AS job_specialty_single,
    job_specialty_multi_current                                                                                                                                        AS job_specialty_multi,
    entity_current                                                                                                                                                     AS entity,
    COALESCE(suporg, '')                                                                                                                                               AS no_null_suporg,
    LAG(no_null_suporg, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                                            AS lag_suporg,
    CONDITIONAL_TRUE_EVENT(no_null_suporg != lag_suporg) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                  AS suporg_group,
    COALESCE(team_id, '')                                                                                                                                              AS no_null_team_id,
    LAG(no_null_team_id, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                                           AS lag_team_id,
    CONDITIONAL_TRUE_EVENT(no_null_team_id != lag_team_id) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                AS team_id_group,
    COALESCE(manager, '')                                                                                                                                              AS no_null_manager,
    LAG(no_null_manager, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                                           AS lag_manager,
    CONDITIONAL_TRUE_EVENT(no_null_manager != lag_manager) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                AS manager_group,
    COALESCE(department, '')                                                                                                                                           AS no_null_department,
    LAG(no_null_department, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                                        AS lag_department,
    CONDITIONAL_TRUE_EVENT(no_null_department != lag_department) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                          AS department_group,
    COALESCE(employee_type, '')                                                                                                                                        AS no_null_employee_type,
    LAG(no_null_employee_type, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                                     AS lag_employee_type,
    CONDITIONAL_TRUE_EVENT(no_null_employee_type != lag_employee_type) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                    AS employee_type_group,
    COALESCE(job_code, '')                                                                                                                                             AS no_null_job_code,
    LAG(no_null_job_code, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                                          AS lag_job_code,
    CONDITIONAL_TRUE_EVENT(no_null_job_code != lag_job_code) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                              AS job_code_group,
    COALESCE(job_specialty_single, '')                                                                                                                                 AS no_null_job_specialty_single,
    LAG(no_null_job_specialty_single, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                              AS lag_job_specialty_single,
    CONDITIONAL_TRUE_EVENT(no_null_job_specialty_single != lag_job_specialty_single) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)      AS job_specialty_single_group,
    COALESCE(job_specialty_multi, '')                                                                                                                                  AS no_null_job_specialty_multi,
    LAG(no_null_job_specialty_multi, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                               AS lag_job_specialty_multi,
    CONDITIONAL_TRUE_EVENT(no_null_job_specialty_multi != lag_job_specialty_multi) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)        AS job_specialty_multi_group,
    COALESCE(entity, '')                                                                                                                                               AS no_null_entity,
    LAG(no_null_entity, 1, '') OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                                            AS lag_entity,
    CONDITIONAL_TRUE_EVENT(no_null_entity != lag_entity) OVER (PARTITION BY employee_id ORDER BY date_time_initiated, effective_date)                                  AS entity_group,
    effective_date                                                                                                                                                     AS effective_date
  FROM {{ref('staffing_history_approved_source')}}

),

staffing_history AS (

  SELECT 
    team_member_groups.employee_id                                                                                                                                     AS employee_id,
    team_member_groups.team_id                                                                                                                                         AS team_id,
    team_member_groups.team_id_group                                                                                                                                   AS team_id_group,
    team_member_groups.manager                                                                                                                                         AS manager,
    team_member_groups.manager_group                                                                                                                                   AS manager_group,
    team_member_groups.department                                                                                                                                      AS department,
    team_member_groups.department_group                                                                                                                                AS department_group,
    team_member_groups.suporg                                                                                                                                          AS suporg,
    team_member_groups.suporg_group                                                                                                                                    AS suporg_group,
    team_member_groups.employee_type                                                                                                                                   AS employee_type,
    team_member_groups.employee_type_group                                                                                                                             AS employee_type_group,
    team_member_groups.job_code                                                                                                                                        AS job_code,
    team_member_groups.job_code_group                                                                                                                                  AS job_code_group,
    team_member_groups.job_specialty_single                                                                                                                            AS job_specialty_single,
    team_member_groups.job_specialty_single_group                                                                                                                      AS job_specialty_single_group,
    team_member_groups.job_specialty_multi                                                                                                                             AS job_specialty_multi,
    team_member_groups.job_specialty_multi_group                                                                                                                       AS job_specialty_multi_group,
    team_member_groups.entity                                                                                                                                          AS entity,
    team_member_groups.entity_group                                                                                                                                    AS entity_group,
    job_profiles.position                                                                                                                                              AS position,
    job_profiles.job_family                                                                                                                                            AS job_family,
    job_profiles.management_level                                                                                                                                      AS management_level,
    job_profiles.job_grade                                                                                                                                             AS job_grade,
    job_profiles.is_position_active                                                                                                                                    AS is_position_active,
    MIN(effective_date)                                                                                                                                                AS position_effective_date
  FROM team_member_groups
  LEFT JOIN job_profiles
    ON job_profiles.job_code = team_member_groups.job_code
  WHERE effective_date >= '2022-06-16'
  {{ dbt_utils.group_by(n=24)}}
  
),

job_info AS (

  SELECT 
    employee_id, 
    job_title         AS position,
    reports_to        AS manager,
    department, 
    entity, 
    effective_date
  FROM {{ref('blended_job_info_source')}}
  WHERE effective_date < '2022-06-16'
  --Use data from blended job info source up to the transition from BHR to Workday
  --Job profiles didn't exist in BHR
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, position, department, entity ORDER BY effective_date DESC) = 1

),

final AS (

  SELECT 

    -- Surrogate keys
    {{ dbt_utils.surrogate_key(['staffing_history.employee_id'])}}                                                                                                     AS dim_team_member_sk,
    {{ dbt_utils.surrogate_key(['staffing_history.team_id'])}}                                                                                                         AS dim_team_sk,

    -- Team member position attributes
    staffing_history.employee_id                                                                                                                                       AS employee_id,
    staffing_history.team_id                                                                                                                                           AS team_id,
    staffing_history.manager                                                                                                                                           AS manager,
    staffing_history.suporg                                                                                                                                            AS suporg,
    staffing_history.job_code                                                                                                                                          AS job_code,
    staffing_history.position                                                                                                                                          AS position,
    staffing_history.job_family                                                                                                                                        AS job_family,
    staffing_history.job_specialty_single                                                                                                                              AS job_specialty_single,
    staffing_history.job_specialty_multi                                                                                                                               AS job_specialty_multi,
    staffing_history.management_level                                                                                                                                  AS management_level,
    staffing_history.job_grade                                                                                                                                         AS job_grade,
    staffing_history.department                                                                                                                                        AS department,
    staffing_history.entity                                                                                                                                            AS entity,
    staffing_history.is_position_active                                                                                                                                AS is_position_active,
    staffing_history.position_effective_date                                                                                                                           AS effective_date,
    CASE 
      WHEN ROW_NUMBER() OVER (PARTITION BY staffing_history.employee_id ORDER BY staffing_history.position_effective_date DESC) = 1 
        THEN TRUE
      ELSE FALSE
    END                                                                                                                                                                AS is_current
  FROM staffing_history

  UNION

  SELECT 
    -- Surrogate keys
    {{ dbt_utils.surrogate_key(['job_info.employee_id'])}}                                                                                                             AS dim_team_member_sk,
    NULL                                                                                                                                                               AS dim_team_sk,

    -- Team member position attributes
    job_info.employee_id                                                                                                                                               AS employee_id,
    NULL                                                                                                                                                               AS team_id,
    job_info.manager                                                                                                                                                   AS manager,
    NULL                                                                                                                                                               AS suporg,
    NULL                                                                                                                                                               AS job_code,
    job_info.position                                                                                                                                                  AS position,
    NULL                                                                                                                                                               AS job_family,
    NULL                                                                                                                                                               AS job_specialty_single,
    NULL                                                                                                                                                               AS job_specialty_multi,
    NULL                                                                                                                                                               AS management_level,
    NULL                                                                                                                                                               AS job_grade,
    job_info.department                                                                                                                                                AS department,
    job_info.entity                                                                                                                                                    AS entity,
    NULL                                                                                                                                                               AS is_position_active,
    job_info.effective_date                                                                                                                                            AS effective_date,
    FALSE                                                                                                                                                              AS is_current
  FROM job_info

)

SELECT *
FROM final
