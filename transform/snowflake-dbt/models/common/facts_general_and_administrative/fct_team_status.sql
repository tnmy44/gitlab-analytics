WITH final AS (

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
    employment_status,
    termination_reason,
    termination_type,
    exit_impact,
    valid_from,
    valid_to,
    is_current
  FROM {{ref('prep_team_status')}}
   
)

{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@lisvinueza',
    created_date='2023-06-01',
    updated_date='2023-06-01'
) }}