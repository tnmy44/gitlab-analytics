WITH team_member_position_dup AS (

  SELECT 
      *
  FROM {{ ref('fct_team_member_position') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id, position_effective_date ORDER BY position_date_time_initiated DESC) = 1

), 

team_member_position AS (

  SELECT 
    dim_team_member_sk,
    dim_team_sk,
    employee_id,
    team_id,
    job_code,
    position,
    job_family,
    job_specialty_single,
    job_specialty_multi,
    management_level,
    job_grade,
    entity,
    is_position_active,
    position_effective_date AS position_valid_from,
    LEAD(position_valid_from, 1, {{var('tomorrow')}})  OVER (PARTITION BY employee_id ORDER BY position_valid_from) AS position_valid_to,
    position_date_time_initiated
  FROM team_member_position_dup
  
),

team_member_status AS (

  SELECT 
    dim_team_member_sk,
    employee_id,
    employment_status,
    termination_reason,
    termination_type,
    exit_impact,
    status_effective_date AS status_valid_from,
    LEAD(status_valid_from, 1, {{var('tomorrow')}})  OVER (PARTITION BY employee_id ORDER BY status_valid_from) AS status_valid_to
  FROM {{ ref('fct_team_member_status') }}

),

unioned AS (

  SELECT 
    dim_team_member_sk,
    position_valid_from AS valid_from
  FROM team_member_position

  UNION

  SELECT 
    dim_team_member_sk,
    status_valid_from
  FROM team_member_status

),

date_range AS (

  SELECT 
    dim_team_member_sk,
    valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY dim_team_member_sk ORDER BY valid_from) AS valid_to,
    IFF(valid_to = {{var('tomorrow')}}, TRUE, FALSE) AS is_current
  FROM unioned
  
),

final AS (

  SELECT 
    team_member_position.dim_team_member_sk,
    team_member_position.dim_team_sk,
    team_member_position.employee_id,
    team_member_position.team_id,
    team_member_position.job_code,
    team_member_position.position,
    team_member_position.job_family,
    team_member_position.job_specialty_single,
    team_member_position.job_specialty_multi,
    team_member_position.management_level,
    team_member_position.job_grade,
    team_member_position.entity,
    team_member_position.is_position_active,
    team_member_status.employment_status,
    team_member_status.termination_reason,
    team_member_status.termination_type,
    team_member_status.exit_impact,
    date_range.valid_from,
    date_range.valid_to,
    date_range.is_current
  FROM team_member_position AS team_member_position
  INNER JOIN date_range
    ON date_range.dim_team_member_sk = team_member_position.dim_team_member_sk 
    AND (
      CASE
        WHEN date_range.valid_from >= team_member_position.position_valid_from AND date_range.valid_from < team_member_position.position_valid_from THEN TRUE
        WHEN date_range.valid_to > team_member_position.position_valid_from AND date_range.valid_to <= team_member_position.position_valid_from THEN TRUE
        WHEN team_member_position.position_valid_from >= date_range.valid_from AND team_member_position.position_valid_from < date_range.valid_to THEN TRUE
        ELSE FALSE
      END) = TRUE
  LEFT JOIN team_member_status 
    ON team_member_position.dim_team_member_sk = team_member_status.dim_team_member_sk
    AND (
      CASE
        WHEN date_range.valid_from >= team_member_status.status_valid_from AND date_range.valid_from < team_member_status.status_valid_from THEN TRUE
        WHEN date_range.valid_to > team_member_status.status_valid_from AND date_range.valid_to <= team_member_status.status_valid_from THEN TRUE
        WHEN team_member_status.status_valid_from >= date_range.valid_from AND team_member_status.status_valid_from < date_range.valid_to THEN TRUE
        ELSE FALSE
      END) = TRUE
)

SELECT *
FROM final