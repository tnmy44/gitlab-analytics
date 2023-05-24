WITH staffing_history AS (

  SELECT 
    employee_id                                                                                  AS employee_id,
    business_process_type                                                                        AS business_process_type,
    TRIM(SPLIT_PART(SUPORG_CURRENT, '(', 0))                                                     AS suporg,
    manager_current                                                                              AS manager,
    job_code_current                                                                             AS job_code,
    job_specialty_single_current                                                                 AS job_specialty_single,
    job_specialty_multi_current                                                                  AS job_specialty_multi,
    effective_date                                                                               AS valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to
  FROM {{ref('staffing_history_approved_source')}}
  WHERE business_process_type != 'Termination'

),

job_profiles AS (

  SELECT 
    job_code,
    job_profile,
    job_family,
    management_level, 
    job_level                         AS job_grade,
    is_job_profile_active
  FROM {{ref('job_profiles_source')}}

),

final AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['staffing_history.employee_id'])}} AS dim_team_member_sk,
    staffing_history.employee_id,
    job_profiles.job_code,
    staffing_history.job_specialty_single,
    staffing_history.job_specialty_multi,
    job_profiles.job_profile,
    job_profiles.job_family,
    job_profiles.management_level,
    job_profiles.job_grade,
    job_profiles.is_job_profile_active,
    staffing_history.valid_from,
    staffing_history.valid_to,
    IFF(staffing_history.valid_to = {{var('tomorrow')}}, TRUE, FALSE) AS is_current
  FROM staffing_history
  LEFT JOIN job_profiles
    ON job_profiles.job_code = staffing_history.job_code

)

SELECT *
FROM final
