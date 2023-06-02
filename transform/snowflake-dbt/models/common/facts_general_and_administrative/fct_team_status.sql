WITH final AS (

  SELECT 
    team_status.dim_team_member_sk,
    team_status.dim_team_sk,
    team_status.employee_id,
    team_status.team_id,
    team_status.job_code,
    team_status.position,
    team_status.job_family,
    team_status.job_specialty_single,
    team_status.job_specialty_multi,
    team_status.management_level,
    team_status.job_grade,
    team_status.entity,
    team_status.is_position_active,
    team_status.employment_status,
    team_status.termination_reason,
    team_status.termination_type,
    team_status.exit_impact,
    team_status.valid_from,
    team_status.valid_to,
    team_status.is_current
  FROM FROM {{ref('prep_team_status')}}
   
)

{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@lisvinueza',
    created_date='2023-06-01',
    updated_date='2023-06-01'
) }}