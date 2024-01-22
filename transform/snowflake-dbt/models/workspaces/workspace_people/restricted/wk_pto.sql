
WITH directory
AS (
  SELECT * 
  FROM {{ref('employee_directory_analysis')}}
)
,pto_source
AS (
  SELECT * 
  FROM {{ref('gitlab_pto')}}
)
,
start_date
AS (
	SELECT employee_id AS hire_id
		,date_actual AS hire_date
		,ROW_NUMBER() OVER (
			PARTITION BY employee_id ORDER BY date_Actual ASC
			) AS hire_rank
	FROM directory
	WHERE is_hire_date = 'TRUE'
		AND date_actual <= CURRENT_DATE
	)
	,end_date
AS (
	SELECT employee_id AS term_id
		,date_actual AS term_date
		,ROW_NUMBER() OVER (
			PARTITION BY employee_id ORDER BY date_Actual ASC
			) AS term_rank
	FROM directory
	WHERE is_termination_date = 'true'
		AND date_actual <= CURRENT_DATE
	)
	,start_to_end
AS (
	SELECT hire_id
		,hire_rank
		,hire_date
		,term_date
		,term_rank
		,coalesce(term_Date, CURRENT_DATE) AS last_date
	FROM start_date
	LEFT JOIN end_date ON hire_id = term_id
		AND hire_rank = term_rank
	)
	,pto
AS (
	SELECT *
		,DATEDIFF(day, start_date, end_date) + 1 AS pto_days_requested
		,DAYOFWEEK(pto_date) AS pto_day_of_week
		,CASE 
			WHEN total_hours < employee_day_length
				THEN FALSE
			ELSE TRUE
			END AS is_full_day
		,row_number() OVER (
			PARTITION BY hr_employee_id
			,pto_date ORDER BY end_date DESC
			) AS pto_rank
		,'Y' AS is_pto_date
	FROM pto_source
	WHERE pto_status = 'AP'
		AND pto_date <= CURRENT_DATE
		AND pto_day_of_week BETWEEN 1
			AND 5
		AND pto_days_requested <= 25 QUALIFY pto_rank = 1
	)
	,dates
AS (
	SELECT *
	FROM prod.legacy.date_details
	)
	,final
AS (
	SELECT start_to_end.hire_id AS employee_id
		,start_to_end.hire_date
		,start_to_end.term_date
		,start_to_end.last_date
		,dates.date_actual
		,GREATEST(start_to_end.hire_date, DATEADD('month', - 11, DATE_TRUNC('month', date_actual))) AS r12_start_date
		,dates.fiscal_month_name_fy
		,dates.fiscal_quarter_name_fy
		,dates.fiscal_year
		,ifnull(pto.is_pto_date, 'N') AS is_pto_date
		,IFF(pto.is_full_day = 'TRUE', 'Y', 'N') AS is_pto_full_day
		,IFF(pto.is_holiday = 'TRUE', 'Y', 'N') AS is_pto_holiday
		,IFF(pto.is_pto_date = 'Y', 1, 0) AS pto_count
		,sum(pto_count) OVER (
			PARTITION BY start_to_end.hire_id
			,start_to_end.hire_date ORDER BY start_to_end.hire_id
				,dates.date_actual ASC
			) AS running_pto_sum
		,sum(pto_count) OVER (
			PARTITION BY start_to_end.hire_id
			,start_to_end.hire_date
			,dates.fiscal_month_name_fy ORDER BY start_to_end.hire_id
				,dates.date_actual ASC
			) AS running_pto_sum_mtd
		,sum(pto_count) OVER (
			PARTITION BY start_to_end.hire_id
			,start_to_end.hire_date
			,dates.fiscal_quarter_name_fy ORDER BY start_to_end.hire_id
				,dates.date_actual ASC
			) AS running_pto_sum_qtd
		,sum(pto_count) OVER (
			PARTITION BY start_to_end.hire_id
			,start_to_end.hire_date
			,dates.fiscal_year ORDER BY start_to_end.hire_id
				,dates.date_actual ASC
			) AS running_pto_sum_ytd
		,count(pto2.is_pto_date) AS running_pto_sum_r12
	FROM start_to_end
	LEFT JOIN dates ON dates.date_actual BETWEEN start_to_end.hire_date
			AND start_to_end.last_date
	LEFT JOIN pto ON dates.date_actual = pto.pto_date
		AND start_to_end.hire_id = pto.hr_employee_id
	LEFT JOIN pto AS pto2 ON start_to_end.hire_id = pto2.hr_employee_id
		AND dates.date_actual >= pto2.pto_date
		AND r12_start_date <= pto2.pto_date
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
	ORDER BY start_to_end.hire_id
		,dates.date_actual DESC
	)
SELECT employee_id
	,hire_date
	,term_date
	,last_date
	,date_actual
	,is_pto_date
	,is_pto_full_day
	,is_pto_holiday
	,running_pto_sum
	,running_pto_sum_mtd
	,running_pto_sum_qtd
	,running_pto_sum_ytd
	,running_pto_sum_r12
FROM final

