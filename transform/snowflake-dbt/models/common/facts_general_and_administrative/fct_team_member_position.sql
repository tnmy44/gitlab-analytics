WITH final AS (

  SELECT 
    {{ dbt_utils.surrogate_key(['employee_id', 'position_valid_from']) }}  AS team_member_position_pk,
    prep_team_member_position.dim_team_member_sk,
    prep_team_member_position.dim_team_sk,
    prep_team_member_position.employee_id,
    prep_team_member_position.job_code,
    prep_team_member_position.job_specialty_single,
    prep_team_member_position.job_specialty_multi,
    prep_team_member_position.position,
    prep_team_member_position.job_family,
    prep_team_member_position.management_level,
    prep_team_member_position.job_grade,
    prep_team_member_position.entity,
    prep_team_member_position.is_position_active,
    prep_team_member_position.is_current,
    prep_team_member_position.position_valid_from,
    prep_team_member_position.position_valid_to
  FROM {{ ref('prep_team_member_position') }}

)


{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@lisvinueza',
    created_date='2023-05-30',
    updated_date='2023-05-30'
) }}
