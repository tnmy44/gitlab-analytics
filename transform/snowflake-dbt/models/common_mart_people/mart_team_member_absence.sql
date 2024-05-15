WITH team_member AS (

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
    DATEDIFF(year, date_of_birth, valid_from) + CASE WHEN (DATEADD(year,DATEDIFF(year, date_of_birth, valid_from) , date_of_birth) > valid_from) THEN - 1 ELSE 0 END 
                                                                                                    AS age_calculated,
    CASE 
      WHEN age_calculated BETWEEN 18 AND 24  THEN '18-24'
      WHEN age_calculated BETWEEN 25 AND 29  THEN '25-29'
      WHEN age_calculated BETWEEN 30 AND 34  THEN '30-34'
      WHEN age_calculated BETWEEN 35 AND 39  THEN '35-39'
      WHEN age_calculated BETWEEN 40 AND 44  THEN '40-44'
      WHEN age_calculated BETWEEN 44 AND 49  THEN '44-49'
      WHEN age_calculated BETWEEN 50 AND 54  THEN '50-54'
      WHEN age_calculated BETWEEN 55 AND 59  THEN '55-59'
      WHEN age_calculated >= 60              THEN '60+'
      WHEN age_calculated IS NULL            THEN 'Unreported'
      WHEN age_calculated = -1               THEN 'Unreported'
      ELSE NULL 
    END                                                                                             AS age_cohort,
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
    employee_type,
    termination_date,
    is_current_team_member,
    is_rehire,
    valid_from,
    valid_to
  FROM {{ ref('dim_team_member') }}

),
team_member_absence AS (

  SELECT 
    dim_team_member_sk                                                                            AS dim_team_member_sk,
    dim_team_sk                                                                                   AS dim_team_sk,
    employee_id                                                                                   AS employee_id,
    COALESCE(team_id,'Unknown Team ID')                                                           AS team_id,
    COALESCE(job_code,'Unknown Job Code')                                                         AS job_code,
    position                                                                                      AS position,
    COALESCE(job_family,'Unknown Job Family')                                                     AS job_family,
    job_specialty_single                                                                          AS job_specialty_single,
    job_specialty_multi                                                                           AS job_specialty_multi,
    COALESCE(management_level,'Unknown Management Level')                                         AS management_level,
    COALESCE(job_grade,'Unknown Job Grade')                                                       AS job_grade,
    entity                                                                               AS entity,
    is_position_active,
    is_current_team_member_position,
    absence_date,
    absence_start,
    absence_end,
    is_pto,
    is_holiday,
    pto_uuid,
    pto_type_uuid,
    pto_status,
    pto_status_name,
    total_hours,
    recorded_hours,
    absence_status,
    employee_day_length,
    created_by,
    updated_by,
    model_created_date,
    model_updated_date,
    dbt_updated_at,
    dbt_created_at    
  FROM {{ ref('fct_team_member_absence') }}
), 
final AS (
  SELECT 

    -- Surrogate keys
    team_member.dim_team_member_sk,
    {{ dbt_utils.generate_surrogate_key(['team_member.team_id']) }}                AS dim_team_sk,

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
    team_member.age_calculated                                            AS age,
    team_member.age_cohort,
    team_member.gitlab_username,
    team_member.country,
    team_member.region,
    team_member.region_modified,
    team_member.gender_region,
    team_member.ethnicity_region,
    team_member.urg_group,
    team_member.urg_region,
    team_member.hire_date,
    team_member.employee_type,
    team_member.termination_date,
    team_member.is_current_team_member,
    team_member.is_rehire,
    team_member.team_id,
    team_member_absence.job_code,
    team_member_absence.position,
    team_member_absence.entity,
    team_member_absence.job_family,
    team_member_absence.job_specialty_single,
    team_member_absence.job_specialty_multi,
    team_member_absence.management_level,
    team_member_absence.job_grade,
    team_member_absence.is_position_active,
    team_member_absence.is_current_team_member_position,
    team_member_absence.absence_date,
    team_member_absence.absence_start,
    team_member_absence.absence_end,
    team_member_absence.is_pto,
    team_member_absence.is_holiday,
    team_member_absence.pto_uuid,
    team_member_absence.pto_type_uuid,
    team_member_absence.pto_status,
    team_member_absence.pto_status_name,
    team_member_absence.total_hours,
    team_member_absence.recorded_hours,
    team_member_absence.absence_status,
    team_member_absence.employee_day_length
  FROM team_member
  INNER JOIN team_member_absence
    ON team_member_absence.employee_id = team_member.employee_id 
)
{{ dbt_audit(
    cte_ref="final",
    created_by="@rakhireddy",
    updated_by="@rakhireddy",
    created_date="2024-05-15",
    updated_date="2024-05-15"
) }}