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
    date_time_initiated                                                                                             AS position_date_initiated,
    effective_date                                                                                                  AS position_effective_date,
    ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date DESC, date_time_initiated DESC)             AS row_number
  FROM {{ref('staffing_history_approved_source')}}
  WHERE business_process_type = 'Hire' 
      /* 
    The filters below capture changes in job code and job specialties. Business Process types cannot be used to filter out changes in 
    job profiles or specialties as they are not consistently used. Example: Change job can be used to capture country changes
    */  
    OR job_code_past != job_code_current
    OR job_specialty_single_past != job_specialty_single_current
    OR job_specialty_multi_past != job_specialty_multi_current
    
         

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
    staffing_history.team_id                                                                           AS team_id,
    job_profiles.job_code                                                                              AS job_code,
    job_profiles.position                                                                              AS position,
    job_profiles.job_family                                                                            AS job_family,
    staffing_history.job_specialty_single                                                              AS job_specialty_single,
    staffing_history.job_specialty_multi                                                               AS job_specialty_multi,
    job_profiles.management_level                                                                      AS management_level,
    job_profiles.job_grade                                                                             AS job_grade,
    staffing_history.entity                                                                            AS entity,
    job_profiles.is_position_active                                                                    AS is_position_active,
    staffing_history.position_date_initiated                                                           AS position_date_initiated,
    staffing_history.position_effective_date                                                           AS position_effective_date,

    -- Add is_current flag for most current team member record
    IFF(staffing_history.row_number = 1, TRUE, FALSE)                                                  AS is_current
  FROM staffing_history
  LEFT JOIN job_profiles
    ON job_profiles.job_code = staffing_history.job_code

)

SELECT *
FROM final
