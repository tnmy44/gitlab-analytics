WITH staffing_history AS (

  SELECT 
    employee_id                                                                                                     AS employee_id,
    team_id_current                                                                                                 AS team_id,
    business_process_type                                                                                           AS business_process_type,
    TRIM(SPLIT_PART(SUPORG_CURRENT, '(', 0))                                                                        AS suporg,
    job_code_current                                                                                                AS job_code,
    job_specialty_single_current                                                                                    AS job_specialty_single,
    job_specialty_multi_current                                                                                     AS job_specialty_multi,
    entity_current                                                                                                  AS entity,
    effective_date                                                                                                  AS position_valid_from,
    LEAD(position_valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY position_valid_from)  AS position_valid_to
  FROM {{ref('staffing_history_approved_source')}}
  WHERE business_process_type = 'Promote Employee Inbound' 
    OR business_process_type = 'Change Job' 
    OR business_process_type = 'Hire' 
    OR job_code_past != job_code_current
    
    /* 
    'Transfer Employee Inbound' captures manager changes but it is sometimes (mistakenly) used to 
    capture job code changes. Since we don't need manager history in this table, we are filtering out
    'Transfer Employee Inbound' and adding 'job_code_past != job_code' to
    capture those edge cases that don't fall under the other 3 categories: Promote Employee Inbound, 
    change job or hire
    */           

),

job_profiles AS (

  SELECT 
    job_code,
    job_profile                AS position,
    job_family,
    management_level, 
    job_level                  AS job_grade,
    is_job_profile_active      AS is_position_active
  FROM {{ref('job_profiles_source')}}

),

final AS (

  SELECT 

    -- Surrogate keys
    {{ dbt_utils.surrogate_key(['staffing_history.employee_id'])}}                                     AS dim_team_member_sk,
    {{ dbt_utils.surrogate_key(['staffing_history.team_id'])}}                                         AS dim_team_sk,

    -- Team member position attributes
    staffing_history.employee_id                                                                       AS employee_id,
    job_profiles.job_code                                                                              AS job_code,
    staffing_history.job_specialty_single                                                              AS job_specialty_single,
    staffing_history.job_specialty_multi                                                               AS job_specialty_multi,
    job_profiles.position                                                                              AS position,
    job_profiles.job_family                                                                            AS job_family,
    job_profiles.management_level                                                                      AS management_level,
    job_profiles.job_grade                                                                             AS job_grade,
    staffing_history.entity                                                                            AS entity,
    job_profiles.is_position_active                                                                    AS is_position_active,

    -- Add is_current flag for most current team member record
    IFF(staffing_history.position_valid_to = {{var('tomorrow')}}, TRUE, FALSE)                         AS is_current,
    staffing_history.position_valid_from                                                               AS position_valid_from,
    staffing_history.position_valid_to                                                                 AS position_valid_to
  FROM staffing_history
  LEFT JOIN job_profiles
    ON job_profiles.job_code = staffing_history.job_code

)

SELECT *
FROM final
