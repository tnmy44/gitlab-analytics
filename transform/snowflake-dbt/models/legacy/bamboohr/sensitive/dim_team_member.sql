WITH final AS (

    SELECT 

      prep_team_member.dim_team_member_sk,
      prep_team_member.employee_id,
      prep_team_member.business_process,
      prep_team_member.nationality,
      prep_team_member.ethnicity,
      prep_team_member.first_name,
      prep_team_member.last_name,
      prep_team_member.gender,
      prep_team_member.work_email,
      prep_team_member.date_of_birth,
      prep_team_member.key_talent_status,
      prep_team_member.gitlab_username,
      prep_team_member.growth_potential_rating,
      prep_team_member.performance_rating,
      prep_team_member.country,
      prep_team_member.region,
      prep_team_member.hire_date,
      prep_team_member.termination_date,
      prep_team_member.valid_from,
      prep_team_member.valid_to,
      prep_team_member.event_sequence
    FROM {{ ref('prep_team_member') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2023-03-27",
    updated_date="2023-03-27"
) }}
