WITH team_member_position_dup AS (

  SELECT *
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
    position_effective_date                                                                                          AS position_valid_from,
    LEAD(position_valid_from, 1, {{ var('tomorrow') }}) OVER (PARTITION BY employee_id ORDER BY position_valid_from) AS position_valid_to,
    position_date_time_initiated
  FROM team_member_position_dup
  WHERE position_valid_from <= CURRENT_DATE()

),

team_member_status AS (

  SELECT
    dim_team_member_sk,
    employee_id,
    employment_status,
    termination_reason,
    termination_type,
    exit_impact,
    status_effective_date                                                                                       AS status_valid_from,
    LEAD(status_valid_from, 1, {{ var('tomorrow') }}) OVER (PARTITION BY employee_id ORDER BY status_valid_from) AS status_valid_to
  FROM {{ ref('fct_team_member_status') }}

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
    LAST_VALUE(team_member_status.employment_status IGNORE NULLS) OVER (PARTITION BY team_member_position.dim_team_member_sk ORDER BY team_member_status.status_valid_from ROWS UNBOUNDED PRECEDING)
                                                                                              AS employment_status,
    LAST_VALUE(team_member_status.termination_reason) OVER (PARTITION BY team_member_position.dim_team_member_sk ORDER BY team_member_status.status_valid_from ROWS UNBOUNDED PRECEDING)
                                                                                              AS termination_reason,
    LAST_VALUE(team_member_status.termination_type) OVER (PARTITION BY team_member_position.dim_team_member_sk ORDER BY team_member_status.status_valid_from ROWS UNBOUNDED PRECEDING)
                                                                                              AS termination_type,
    LAST_VALUE(team_member_status.exit_impact) OVER (PARTITION BY team_member_position.dim_team_member_sk ORDER BY team_member_status.status_valid_from ROWS UNBOUNDED PRECEDING)
                                                                                              AS exit_impact,
    GREATEST(team_member_status.status_valid_from, team_member_position.position_valid_from)  AS valid_from,
    LEAST(team_member_status.status_valid_to, team_member_position.position_valid_to)         AS valid_to,
    IFF(valid_to = DATEADD('day', 1, CURRENT_DATE()), TRUE, FALSE)                            AS is_current
  FROM team_member_position
  LEFT JOIN team_member_status
    ON team_member_position.dim_team_member_sk = team_member_status.dim_team_member_sk
      AND NOT (team_member_status.status_valid_to <= team_member_position.position_valid_from
        OR team_member_status.status_valid_from >= team_member_position.position_valid_to)
)

SELECT *
FROM final
