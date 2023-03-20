WITH staffing_history AS (

  SELECT *
  FROM {{ ref('staffing_history_approved_source') }}

),

intermediate AS (

  SELECT
    staffing_history.employee_id,
    staffing_history.business_process_type,
    staffing_history.hire_date,
    staffing_history.termination_date,
    staffing_history.past_country,
    staffing_history.current_country,
    staffing_history.past_region,
    staffing_history.current_region,
    staffing_history.date_time_initiated,
    staffing_history.effective_date
  FROM staffing_history

)

SELECT *
FROM intermediate
