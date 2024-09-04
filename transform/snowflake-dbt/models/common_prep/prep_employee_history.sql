WITH bamboohr_employee_directory_intermediate AS (
  SELECT *
  FROM {{ ref('employee_directory_intermediate') }}
  WHERE uploaded_row_number_desc = 1 --excludes team members deleted from BambooHR
),

bamboohr_employee_mapping AS (
  SELECT DISTINCT
    employee_id     AS bhr_employee_id,
    employee_number AS wk_employee_id
  FROM bamboohr_employee_directory_intermediate
),

bamboohr_terminations AS (
  SELECT DISTINCT
    employee_number AS wk_employee_id,
    hire_date,
    termination_date
  FROM bamboohr_employee_directory_intermediate
  WHERE termination_date <= '2020-12-31' --use Workday data for 2021-01-01 or later
    AND hire_date <= '2020-12-31'      --use Workday data for 2021-01-01 or later
    AND wk_employee_id != '11595'      --incorrect hire date in bhr_map, listed as 2020-09-10. Correct date is 2020-06-09
    OR wk_employee_id = '11202'        --not in Workday and terminated on 2021-10-29 so WHERE clause would have excluded this team member
),

bamboohr_status AS (
  SELECT *
  FROM {{ ref('bamboohr_employment_status_source') }}
)
,
bamboohr_termination_status AS (
  SELECT
    bamboohr_employee_mapping.wk_employee_id,
    sts.effective_date,
    sts.employment_status
  FROM prep.bamboohr.bamboohr_employment_status_source AS sts
  LEFT JOIN bamboohr_employee_mapping ON sts.employee_id = bamboohr_employee_mapping.bhr_employee_id
  WHERE employment_status = 'Terminated'
    AND effective_date <= '2020-12-31'
),

bamboohr_rehires AS (
  SELECT
    *,
    IFF(employment_status = 'Terminated', 1, 0) AS is_terminated,
    LEAD(employment_status) OVER (
      PARTITION BY
        employee_id ORDER BY effective_date DESC,
      is_terminated DESC
    )                                           AS prior_reason
  FROM bamboohr_status
  WHERE effective_date <= '2020-12-31'
  QUALIFY
    prior_reason = 'Terminated'
    AND employment_status != 'Terminated'
  ORDER BY
    employee_id ASC,
    effective_date DESC
),

bamboohr_hires1 -- displays first non-terminated record for any team members in bamboohr_rehires CTE to capture original hire dateAS (
  SELECT sts.*
  FROM bamboohr_status AS sts
  INNER JOIN bamboohr_rehires ON sts.employee_id = bamboohr_rehires.employee_id
  WHERE sts.effective_date <= '2020-12-31'
    AND sts.employment_status != 'Terminated' QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sts.employee_id ORDER BY sts.effective_date ASC
  ) = 1
),

bamboohr_hires2 --displays non-terminated record for team members not in bamboohr_rehires CTEAS (
  SELECT sts.*
  FROM bamboohr_status AS sts
  LEFT JOIN bamboohr_rehires ON sts.employee_id = bamboohr_rehires.employee_id
  WHERE sts.effective_date <= '2020-12-31'
    AND sts.employment_status != 'Terminated'
    AND bamboohr_rehires.employee_id IS NULL
    AND sts.status_id NOT IN (
      '32108',
      '27971',
      '29556'
    ) -- no hire date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sts.employee_id ORDER BY sts.effective_date ASC
  ) = 1
),

staffing_history_approved_source AS (
  SELECT *
  FROM {{ ref('staffing_history_approved_source') }}
),

bamboohr_hires AS (
  SELECT
    employee_id,
    effective_date
  FROM bamboohr_hires1

  UNION

  SELECT
    employee_id,
    effective_date
  FROM bamboohr_rehires

  UNION

  SELECT
    employee_id,
    effective_date
  FROM bamboohr_hires2
),

hires_stage AS (
  SELECT
    bamboohr_employee_mapping.wk_employee_id,
    bamboohr_hires.effective_date
  FROM bamboohr_hires
  LEFT JOIN bamboohr_employee_mapping ON bamboohr_hires.employee_id = bamboohr_employee_mapping.bhr_employee_id

  UNION

  SELECT
    employee_id    AS hire_id,
    effective_date AS hire_date
  FROM staffing_history_approved_source
  WHERE business_process_type IN (
      'Hire',
      'Contract Contingent Worker'
    )

  UNION

  SELECT
    wk_employee_id,
    hire_date
  FROM bamboohr_terminations
),

terminations_stage AS (
  SELECT
    wk_employee_id,
    termination_date
  FROM bamboohr_terminations

  UNION

  SELECT
    employee_id,
    effective_date
  FROM staffing_history_approved_source
  WHERE business_process_type = 'Termination'

  UNION

  SELECT
    wk_employee_id,
    effective_date
  FROM bamboohr_termination_status
  WHERE employment_status = 'Terminated'
),

start_date AS (
  SELECT
    wk_employee_id AS hire_id,
    effective_date AS hire_date,
    ROW_NUMBER() OVER (
      PARTITION BY wk_employee_id ORDER BY effective_date ASC
    )              AS hire_rank
  FROM hires_stage
),

end_date AS (
  SELECT
    wk_employee_id   AS term_id,
    termination_date AS term_date,
    ROW_NUMBER() OVER (
      PARTITION BY wk_employee_id ORDER BY termination_date ASC
    )                AS term_rank
  FROM terminations_stage
),

combined AS (
  SELECT
    hire_id                             AS employee_id,
    hire_rank,
    hire_date                           AS hired_date,
    term_date                           AS termination_date,
    term_rank,
    COALESCE(term_date, CURRENT_DATE()) AS last_date
  FROM start_date
  LEFT JOIN end_date ON hire_id = term_id
    AND hire_rank = term_rank
)

SELECT *
FROM combined
ORDER BY
  employee_id ASC,
  hire_rank DESC
