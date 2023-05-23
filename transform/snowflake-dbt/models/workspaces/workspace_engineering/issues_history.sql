{{ simple_cte([
    ('assigend_users','infradev_current_issue_users')
])}}

, issues AS (
  SELECT *
  FROM {{ ref('internal_issues_enhanced') }} 
)

, label_groups AS (
  SELECT *
  FROM {{ ref('labels_history') }} 
)

, dates AS (

    SELECT
      *,
      MIN(date_actual) OVER () AS min_date_actual
    FROM {{ ref('dim_date') }}  
    WHERE date_actual > DATE_TRUNC('month', DATEADD('year', -2, CURRENT_DATE()))
      AND date_actual < CURRENT_DATE()
)

, workflow_labels AS (

  SELECT * FROM {{ ref('engineering_analytics_workflow_labels') }}

), final AS (
SELECT 
  {{ dbt_utils.surrogate_key(['dates.date_actual','issues.issue_id']) }} AS daily_issue_id,
  dates.date_actual,
  issues.issue_id,
  issues.issue_iid,
  issues.project_id,
  issues.project_path,
  issues.namespace_id,
  issues.ultimate_parent_id,
  issues.labels,
  issues.masked_label_title,
  issues.issue_title,
  issues.full_group_path,
  issues.url,
  issues.milestone_id,
  issues.milestone_title,
  issues.milestone_description,
  issues.milestone_start_date,
  issues.milestone_due_date,
  IFF(dates.date_actual >= date_trunc('day',issues.closed_at), 'closed', 'opened')        AS issue_state,
  issues.created_at                                                        AS issue_created_at,
  issues.closed_at                                                       AS issue_closed_at,
  issues.priority_label,
  IFNULL(severity.severity, 'No Severity')                             AS severity,
  severity.label_added_at AS severity_label_added_at,
  severity.label_valid_to as severity_label_valid_to,
  IFNULL(team.assigned_team, 'Unassigned')                         AS assigned_team,
  team.label_added_at as team_label_added_at,
  team.label_valid_to as team_label_valid_to,
  team.label_title AS team_label,
  wf.label_added_at as workflow_label_added_at,
  wf.label_valid_to as workflow_label_valid_to,
  wf.workflow AS workflow_label,
  wfl.cycle,
  IFF(dates.date_actual > date_trunc('day',issues.closed_at), NULL,
      DATEDIFF('day', issues.created_at, dates.date_actual))               AS issue_open_age_in_days,
  DATEDIFF('day', severity.label_added_at, dates.date_actual) AS severity_label_age_in_days,
  assigend_users.assigned_usernames,
  IFF(assigend_users.assigned_usernames IS NULL, TRUE, FALSE)              AS is_issue_unassigned,
  issues.group_label,
  issues.section_label,
  issues.stage_label,
  issues.type_label,
  issues.subtype_label,
  issues.is_infradev,
  issues.fedramp_vulnerability,
  issues.is_community_contribution,
  issues.is_security,
  issues.is_corrective_action,
  issues.is_part_of_product,
  MAX(dates.date_actual) OVER () AS last_updated_at,
  ROW_NUMBER() OVER (PARTITION BY daily_issue_id 
  ORDER BY LEAST(COALESCE(severity_label_valid_to, CURRENT_DATE),COALESCE(team_label_valid_to,CURRENT_DATE)),COALESCE(workflow_label_valid_to,CURRENT_DATE)) AS rn
FROM issues 
INNER JOIN dates 
  ON COALESCE(date_trunc('day',issues.created_at),dates.date_actual) <= dates.date_actual 
  AND COALESCE(date_trunc('day',issues.closed_at),dates.date_actual) >= dates.min_date_actual
LEFT JOIN assigend_users
  ON issues.issue_id = assigend_users.dim_issue_id
LEFT JOIN label_groups as severity
  ON severity.label_type='severity'
  AND issues.issue_id = severity.dim_issue_id
  AND dates.date_actual BETWEEN DATE_TRUNC('day', severity.label_valid_from) AND DATE_TRUNC('day', severity.label_valid_to)
LEFT JOIN label_groups as team
  ON team.label_type='team'
  AND issues.issue_id = team.dim_issue_id
  AND dates.date_actual BETWEEN DATE_TRUNC('day', team.label_valid_from) AND DATE_TRUNC('day', team.label_valid_to)
LEFT JOIN label_groups as wf
  ON wf.label_type='workflow'
  AND issues.issue_id = wf.dim_issue_id
  AND dates.date_actual BETWEEN DATE_TRUNC('day', wf.label_valid_from) AND DATE_TRUNC('day', wf.label_valid_to)
LEFT JOIN workflow_labels as wfl
  ON wf.workflow = wfl.workflow_label) 

  SELECT * EXCLUDE rn
  FROM final
  WHERE rn=1