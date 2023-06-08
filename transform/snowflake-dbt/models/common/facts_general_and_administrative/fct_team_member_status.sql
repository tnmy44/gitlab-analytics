WITH team_member_status AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date DESC)                                  AS row_number
  FROM {{ref('workday_employment_status_source')}}
  WHERE effective_date <= CURRENT_DATE() 
  /* Ensure future terms are not included in this table.
  A future iteration should add row level access policies to show future terms to 
  the analyst_people role only
  */
),

final AS (

  SELECT

    -- Primary key
    {{ dbt_utils.surrogate_key(['team_member_status.employee_id', 'team_member_status.employment_status', 'team_member_status.effective_date'])}}       
                                                                                                               AS team_member_status_pk,
    -- Surrogate key                                                                                                     
    {{ dbt_utils.surrogate_key(['team_member_status.employee_id'])}}                                           AS dim_team_member_sk,

    -- Team member status attributes
    team_member_status.employee_id                                                                             AS employee_id,
    team_member_status.employment_status                                                                       AS employment_status,
    team_member_status.termination_reason                                                                      AS termination_reason,
    team_member_status.termination_type                                                                        AS termination_type,
    team_member_status.exit_impact                                                                             AS exit_impact,
    team_member_status.effective_date                                                                          AS status_effective_date,

    -- Add is_current flag for most current team member record, especially in case of rehires
    IFF(team_member_status.row_number = 1, TRUE, FALSE)                                                        AS is_current
  FROM team_member_status

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@lisvinueza',
    updated_by='@lisvinueza',
    created_date='2023-05-30',
    updated_date='2023-05-30'
) }}
