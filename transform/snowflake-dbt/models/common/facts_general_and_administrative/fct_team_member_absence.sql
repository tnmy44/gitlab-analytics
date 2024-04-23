WITH team_status_dup AS (
  SELECT 
    *
  FROM {{ref('fct_team_status')}}
),
WITH pto AS (
  SELECT 
    *
  FROM {{ref('gitlab_pto')}}
),
map AS (
  SELECT *
  FROM {{ ref('map_employee_id') }}
),
team_status AS (
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
    is_position_active
    FROM team_status_dup
),
combined_sources AS (
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
    team_status.is_position_active
    pto.start_date AS absence_start,
    pto.end_date AS absence_end,
    pto.is_pto AS is_pto,
    pto.total_hours AS total_hours,
    pto.recorded_hours AS recorded_hours,
     IFF(
      pto.pto_type_name = 'Out Sick'
      AND DATEDIFF('day', pto.start_date, pto.end_date) > 4, 'Out Sick-Extended', pto.pto_type_name
    ) AS absence_status,
    pto.employee_day_length

  FROM team_status
  INNER JOIN pto ON pto.hr_employee_id=team_status.employee_id
),
/* 
Skipping hire date and term date
*/
final AS (

  SELECT *

  FROM combined_sources

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@rakhireddy',
    updated_by='@rakhireddy',
    created_date='2024-04-12',
    updated_date='2024-04-12',
) }}


