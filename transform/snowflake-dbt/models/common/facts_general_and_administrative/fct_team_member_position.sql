WITH final AS (

  SELECT 
    prep_team_member_position.dim_team_member_sk,
    prep_team_member_position.job_code,
    prep_team_member_position.job_specialty_single,
    prep_team_member_position.job_specialty_multi,
    prep_team_member_position.job_profile,
    prep_team_member_position.job_family,
    prep_team_member_position.management_level,
    prep_team_member_position.job_grade,
    prep_team_member_position.is_job_profile_active,
    prep_team_member_position.valid_from,
    prep_team_member_position.valid_to
  FROM {{ ref('prep_team_member_position') }}

)


{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@lisvinueza',
    created_date='2023-05-24',
    updated_date='2023-05-24'
) }}