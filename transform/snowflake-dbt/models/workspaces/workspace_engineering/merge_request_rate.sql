WITH merged_merge_requests AS (

    SELECT *
    FROM {{ ref('engineering_merge_requests') }}
    WHERE merged_at IS NOT NULL
    AND merged_at >= '2020-01-01'

), employee_directory_analysis AS (

    SELECT *
    FROM {{ ref('employee_directory_analysis')}}

), bamboohr_engineering_division AS (

    SELECT
      date_actual,
      employee_id,
      full_name,
      job_title,
      CASE 
        WHEN LOWER(job_title) LIKE '%backend%' 
          THEN 'backend'
        WHEN LOWER(job_title) LIKE '%fullstack%'
          THEN 'fullstack'
        WHEN LOWER(job_title) LIKE '%frontend%'
          THEN 'frontend'
        ELSE NULL END               AS technology_group,
      LOWER(TRIM(COALESCE(SUBSTR(REGEXP_SUBSTR(value, ':[^:]*$'), 2),value))) AS job_title_speciality,
      reports_to,
      layers,
      department,
      work_email
    FROM employee_directory_analysis,
    LATERAL FLATTEN(INPUT=>SPLIT(COALESCE(REPLACE(jobtitle_speciality,'&',','),''), ','))
    WHERE division = 'Engineering'
      AND date_actual >= '2020-01-01'

), gitlab_team_members AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_members') }}
    WHERE member_source_type = 'Namespace'
    AND source_id IN (9970,6543) -- 9970 = gitlab-org, 6543 = gitlab-com

), map AS (
  
    SELECT *
    FROM {{ ref('map_team_member_user') }}

), gitlab_team_member_info AS (
  
    SELECT DISTINCT
        gitlab_team_members.user_id,
        map.gitlab_username,
        map.employee_id
    FROM gitlab_team_members
    JOIN map ON gitlab_team_members.user_id = map.user_id
  
), mapped_employee_information AS (

    SELECT
        date_trunc('month',bamboohr_engineering_division.date_actual) AS employee_month, 
        gitlab_team_member_info.employee_id AS bamboohr_employee_id,
        gitlab_team_member_info.user_id AS gitlab_dotcom_user_id,
        bamboohr_engineering_division.full_name AS bamboohr_full_name, -- gitlab full name is mostly not populated
        gitlab_team_member_info.gitlab_username AS gitlab_dotcom_user_name,
        bamboohr_engineering_division.work_email AS gitlab_dotcom_email,
        bamboohr_engineering_division.job_title AS bamboohr_jobtitle,
        bamboohr_engineering_division.department
    FROM gitlab_team_member_info
    JOIN bamboohr_engineering_division
    ON gitlab_team_member_info.employee_id = bamboohr_engineering_division.employee_id

), team_author_product_mrs AS (
  
    SELECT
        merged_merge_requests.merge_month, 
        mapped_employee_information.department,
        COUNT(DISTINCT merged_merge_requests.merge_request_id) AS mrs
    FROM merged_merge_requests 
    JOIN mapped_employee_information ON merged_merge_requests.author_id = mapped_employee_information.gitlab_dotcom_user_id AND merged_merge_requests.merge_month = mapped_employee_information.employee_month
    GROUP BY 1,2

), aggregated AS (

    SELECT
        merged_merge_requests.merge_month,
        merged_merge_requests.group_label AS group_name,
        '' AS department,
        COUNT(DISTINCT bamboohr_engineering_division.employee_id) AS employees,
        COUNT(DISTINCT merged_merge_requests.merge_request_id) AS mrs,
        ROUND(mrs / NULLIF(employees,0),2) AS mr_rate,
        'group' as granularity_level
    FROM merged_merge_requests
    LEFT JOIN bamboohr_engineering_division ON merged_merge_requests.merge_month = date_trunc('month',bamboohr_engineering_division.date_actual) AND merged_merge_requests.group_label = bamboohr_engineering_division.job_title_speciality
    WHERE bamboohr_engineering_division.department = 'Development'
    GROUP BY 1,2,3

    UNION ALL

    SELECT
        merged_merge_requests.merge_month,
        '' AS group_name,
        bamboohr_engineering_division.department,
        COUNT(DISTINCT bamboohr_engineering_division.employee_id) AS employees,
        COUNT(DISTINCT merged_merge_requests.merge_request_id) AS mrs,
        ROUND(mrs / NULLIF(employees,0) - 3,2) AS mr_rate,
        'department' as granularity_level
    FROM merged_merge_requests
    LEFT JOIN bamboohr_engineering_division ON merged_merge_requests.merge_month = DATE_TRUNC('month',bamboohr_engineering_division.date_actual) AND bamboohr_engineering_division.department = 'Development'
    GROUP BY 1,2,3

    UNION ALL

    SELECT
        team_author_product_mrs.merge_month,
        '' AS group_name,
        team_author_product_mrs.department,
        team_author_product_mrs.mrs,
        COUNT(DISTINCT bamboohr_engineering_division.employee_id) AS employees,
        ROUND(mrs / NULLIF(employees,0),2) AS mr_rate,
        'department' as granularity_level
    FROM team_author_product_mrs
    LEFT JOIN bamboohr_engineering_division ON team_author_product_mrs.merge_month = DATE_TRUNC('month',bamboohr_engineering_division.date_actual) AND team_author_product_mrs.department = bamboohr_engineering_division.department
    WHERE bamboohr_engineering_division.department != 'Development'
    GROUP BY 1,2,3,4

)

SELECT *
FROM aggregated
