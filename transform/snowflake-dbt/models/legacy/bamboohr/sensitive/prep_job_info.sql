WITH staffing_history AS (

  SELECT *
  FROM {{ ref('staffing_history_approved_source') }}

),

team_member_row_num AS (

  SELECT
    employee_id,
    business_process_type,
    current_country         AS country,
    current_region          AS region,
    hire_date,
    termination_date,
    ROW_NUMBER() OVER (PARTITION BY employee_id, business_process_type ORDER BY hire_date, termination_date) AS row_num
  FROM staffing_history 

),

team_member_grouping AS (

  SELECT
    row_num,
    employee_id,
    country,
    region,
    MIN(hire_date)         AS hire_date,
    MIN(termination_date)  AS termination_date
  FROM team_member_row_num 
  {{ dbt_utils.group_by(n=4)}}

), 

final AS (

  SELECT 
    staffing_history.employee_id,
    grouping.country,
    grouping.region,
    grouping.hire_date,
    grouping.termination_date
  FROM staffing_history
  LEFT JOIN team_member_grouping AS grouping 
    ON grouping.employee_id = staffing_history.employee_id
  {{ dbt_utils.group_by(n=5)}}

)


SELECT * 
FROM final
