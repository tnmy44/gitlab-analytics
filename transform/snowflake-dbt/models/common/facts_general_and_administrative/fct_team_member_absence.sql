WITH team_status_dup AS (
  SELECT 
    *
  FROM {{ref('fct_team_status')}}
),
pto_source AS (
  SELECT 
    *
  FROM {{ref('gitlab_pto')}}
),
pto AS (
  SELECT
    *,
    DATEDIFF(DAY, start_date, end_date) + 1                AS pto_days_requested,
    DAYOFWEEK(pto_date)                                    AS pto_day_of_week,
    NOT COALESCE(total_hours < employee_day_length, FALSE) AS is_full_day,
    ROW_NUMBER() OVER (
      PARTITION BY
        hr_employee_id,
        pto_date
      ORDER BY
        end_date DESC,
        pto_uuid DESC
    )                                                      AS pto_rank,
    'Y'                                                    AS is_pto_date
  FROM pto_source
  WHERE pto_status = 'AP'
    AND pto_date <= CURRENT_DATE
    AND pto_day_of_week BETWEEN 1
    AND 5
    AND pto_days_requested <= 25
    AND COALESCE(pto_group_type, '') != 'EXL'
    AND NOT COALESCE(pto_type_name, '') IN ('CEO Shadow Program', 'Conference', 'Customer Visit')
  QUALIFY pto_rank = 1
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
    is_position_active,
    is_current
    FROM team_status_dup
    WHERE is_current=TRUE
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
    team_status.is_position_active,  
    team_status.is_current AS is_current_team_member_position,
    pto.start_date AS absence_start,
    pto.end_date AS absence_end,
    pto.pto_date AS absence_date,
    pto.is_pto AS is_pto,
    pto.is_holiday AS is_holiday,
    pto.pto_uuid AS pto_uuid,
    pto.pto_type_uuid AS pto_type_uuid,
    pto.pto_status AS pto_status,
    pto.pto_status_name AS pto_status_name,
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


