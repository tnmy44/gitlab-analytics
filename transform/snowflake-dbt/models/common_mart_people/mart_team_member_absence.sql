
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
    entity                                                                                        AS entity,


    --valid_from                                                                                    AS valid_from,
    --valid_to                                                                                      AS valid_to
  FROM {{ ref('fct_team_member_absence') }}


), 