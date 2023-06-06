WITH team_member_mapping AS (

  SELECT *
  FROM {{ ref('workday_employee_mapping_source') }}
  WHERE workday_employee_mapping_source.is_current = 'True'

),

team_member AS (

    SELECT 
      dim_team_member.dim_team_member_sk,
      dim_team_member.employee_id,
      dim_team_member.nationality,
      dim_team_member.ethnicity,
      dim_team_member.first_name,
      dim_team_member.last_name,
      dim_team_member.first_name || ' ' || dim_team_member.last_name                                  AS full_name,
      dim_team_member.gender,
      dim_team_member.work_email,
      dim_team_member.date_of_birth,
      dim_team_member.gitlab_username,
      team_member_mapping.cost_center,
      dim_team_member.team_id,
      CASE 
        WHEN team_member_mapping.age BETWEEN 18 AND 24  THEN '18-24'
        WHEN team_member_mapping.age BETWEEN 25 AND 29  THEN '25-29'
        WHEN team_member_mapping.age BETWEEN 30 AND 34  THEN '30-34'
        WHEN team_member_mapping.age BETWEEN 35 AND 39  THEN '35-39'
        WHEN team_member_mapping.age BETWEEN 40 AND 44  THEN '40-44'
        WHEN team_member_mapping.age BETWEEN 44 AND 49  THEN '44-49'
        WHEN team_member_mapping.age BETWEEN 50 AND 54  THEN '50-54'
        WHEN team_member_mapping.age BETWEEN 55 AND 59  THEN '55-59'
        WHEN team_member_mapping.age >= 60              THEN '60+'
        WHEN team_member_mapping.age IS NULL            THEN 'Unreported'
        WHEN team_member_mapping.age = -1               THEN 'Unreported'
        ELSE NULL END                                                                                 AS age_cohort,
      dim_team_member.country,
      dim_team_member.region,
      CASE
        WHEN dim_team_member.region = 'Americas' AND dim_team_member.country IN ('United States', 'Canada','Mexico','United States of America') 
          THEN 'NORAM'
        WHEN dim_team_member.region = 'Americas' AND dim_team_member.country NOT IN ('United States', 'Canada','Mexico','United States of America') 
          THEN 'LATAM'
        ELSE dim_team_member.region END                                                               AS region_modified,
      IFF(dim_team_member.country IN ('United States','United States of America'), 
        COALESCE(dim_team_member.gender,'Did Not Identify')  || '_' || 'United States of America', 
        COALESCE(dim_team_member.gender,'Did Not Identify')  || '_'|| 'Non-US')                       AS gender_region,
      IFF(dim_team_member.country IN ('United States','United States of America'), 
        COALESCE(dim_team_member.ethnicity,'Did Not Identify')  || '_' || 'United States of America', 
        COALESCE(dim_team_member.ethnicity,'Did Not Identify')  || '_'|| 'Non-US')                    AS ethnicity_region,
      CASE
        WHEN COALESCE(dim_team_member.ethnicity, 'Did Not Identify') NOT IN ('White','Asian','Did Not Identify','Declined to Answer')
            THEN TRUE
        ELSE FALSE END                                                                                AS urg_group,
      IFF(urg_group = TRUE, 'URG', 'Non-URG')  || '_' ||
          IFF(dim_team_member.country IN ('United States','United States of America'),
            'United States of America',
            'Non-US')                                                                                 AS urg_region,
      dim_team_member.hire_date,
      dim_team_member.termination_date,
      dim_team_member.is_current_team_member,
      dim_team_member.is_rehire
    FROM {{ ref('dim_team_member') }}
    LEFT JOIN team_member_mapping
      ON dim_team_member.employee_id = team_member_mapping.employee_id
    WHERE dim_team_member.is_current = 'True'

),

team AS (

  SELECT 
    dim_team.team_id,
    dim_team.team_superior_team_id,
    dim_team.hierarchy_level_1,
    dim_team.hierarchy_level_2,
    dim_team.hierarchy_level_3,
    dim_team.hierarchy_level_4,
    dim_team.hierarchy_level_5,
    dim_team.hierarchy_level_6,
    dim_team.hierarchy_level_7,
    dim_team.hierarchy_level_8,
    dim_team.hierarchy_level_9,
    dim_team.team_hierarchy_level,
    dim_team.team_name,
    dim_team.team_manager_name,
    dim_team.team_manager_name_id,
    dim_team.team_inactivated_date,
    dim_team.team_members_count,
    dim_team.is_team_active
  FROM {{ ref('dim_team') }}
  WHERE dim_team.is_current = 'True'

), 

final AS (

  SELECT 
    team_member.dim_team_member_sk,
    team_member.employee_id,
    team_member.nationality,
    team_member.ethnicity,
    team_member.first_name,
    team_member.last_name,
    team_member.full_name,
    team_member.gender,
    team_member.work_email,
    team_member.date_of_birth,
    team_member.age_cohort,
    team_member.cost_center,
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
    team.team_id,
    team.team_superior_team_id,
    team.hierarchy_level_1,
    team.hierarchy_level_2,
    team.hierarchy_level_3,
    team.hierarchy_level_4,
    team.hierarchy_level_5,
    team.hierarchy_level_6,
    team.hierarchy_level_7,
    team.hierarchy_level_8,
    team.hierarchy_level_9,
    team.team_hierarchy_level,
    team.team_name,
    team.team_manager_name,
    team.team_manager_name_id,
    team.team_members_count,
    team.is_team_active,
    team.team_inactivated_date
  FROM team_member
  LEFT JOIN team
    ON team_member.team_id = team.team_id

)

SELECT * 
FROM final