{{ simple_cte([
    ('issues','infradev_issues_base'),
    ('assigend_users','infradev_current_issue_users'),
    ('label_groups','infradev_label_history')
])}}

, dates AS (

    SELECT
      *,
      MIN(date_actual) OVER () AS min_date_actual
    FROM {{ ref('dim_date') }}  
    WHERE date_actual > DATE_TRUNC('month', DATEADD('year', -2, CURRENT_DATE()))
      AND date_actual < CURRENT_DATE()
)

SELECT 
  {{ dbt_utils.surrogate_key(['dates.date_actual','issues.issue_id']) }} AS daily_issue_id,
  dates.date_actual,
  issues.issue_id,
  issues.issue_iid,
  issues.project_id,
  issues.namespace_id,
  issues.labels,
  issues.issue_title,
  issues.full_group_path,
  issues.url,
  IFF(dates.date_actual > issues.closed_at, 'closed', 'open')        AS issue_state,
  issues.created_at                                                        AS issue_created_at,
  issues.closed_at                                                       AS issue_closed_at,
  issues.priority_label,
  IFNULL(label_groups.severity, 'No Severity')                             AS severity,
  label_groups.severity_label_added_at,
  IFNULL(label_groups.assigned_team, 'Unassigned')                         AS assigned_team,
  label_groups.team_label_added_at,
  label_groups.team_label,
  IFF(dates.date_actual > issues.closed_at, NULL,
      DATEDIFF('day', issues.created_at, dates.date_actual))               AS issue_open_age_in_days,
  DATEDIFF('day', label_groups.severity_label_added_at, dates.date_actual) AS severity_label_age_in_days,
  assigend_users.assigned_usernames,
  IFF(assigend_users.assigned_usernames IS NULL, TRUE, FALSE)              AS is_issue_unassigned,
  issues.group_label,
  issues.section_label,
  issues.stage_label
FROM issues 
INNER JOIN dates 
  ON issues.created_at <= dates.date_actual
  AND (issues.created_at > dates.min_date_actual  OR issues.created_at IS NULL) 
LEFT JOIN assigend_users
  ON issues.issue_id = assigend_users.dim_issue_id
LEFT JOIN label_groups 
  ON issues.issue_id = label_groups.dim_issue_id
  AND dates.date_actual BETWEEN DATE_TRUNC('day', label_groups.label_group_valid_from) AND DATE_TRUNC('day', label_groups.label_group_valid_to)