WITH directory AS (
  SELECT *
  FROM {{ ref('employee_directory_intermediate') }}
),
 pto_source AS (
  SELECT 
    *
  FROM {{ref('gitlab_pto_source')}}
),
map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
),
combined_sources AS (
  SELECT 
    d.employee_id AS hire_id,
    pto.start_date AS absence_start,
    pto.end_date AS absense_end,
    pto.is_pto AS is_pto,
    pto.total_hours AS total_hours,
    pto.recorded_hours AS recorded_hours,
     IFF(
      pto.pto_type_name = 'Out Sick'
      AND DATEDIFF('day', pto.start_date, pto.end_date) > 4, 'Out Sick-Extended', pto.pto_type_name
    ) AS pto_type_name,
    
    
  FROM pto_source pto
  INNER JOIN map ON pto.hr_employee_id=map.bhr_employee_id
  INNER JOIN directory d on map.wk_employee_id=d.employee_id
),
/* 
Skipping hire date and term date
*/
final AS (

  SELECT

    -- Primary key
    {{ dbt_utils.generate_surrogate_key(['hire_id', 'start_date'])}}    AS team_member_absence_pk,
    -- Surrogate key                                                                                                     
    {{ dbt_utils.generate_surrogate_key(['hire_id'])}}                  AS dim_team_member_sk,

    -- Team member status attributes

  FROM combined_sources

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@rakhireddy',
    updated_by='@rakhireddy',
    created_date='2024-04-12',
    updated_date='2024-04-12',
) }}


