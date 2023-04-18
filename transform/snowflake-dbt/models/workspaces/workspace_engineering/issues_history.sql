{{ simple_cte([
    ('assigend_users','infradev_current_issue_users')
])}}

, issues AS (
  SELECT *
  FROM {{ ref('engineering_issues') }} 
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

, final AS (
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
  issues.milestone_id,
  issues.milestone_title,
  IFF(dates.date_actual > issues.closed_at, 'closed', 'opened')        AS issue_state,
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
  IFF(dates.date_actual > issues.closed_at, NULL,
      DATEDIFF('day', issues.created_at, dates.date_actual))               AS issue_open_age_in_days,
  DATEDIFF('day', severity.label_added_at, dates.date_actual) AS severity_label_age_in_days,
  assigend_users.assigned_usernames,
  IFF(assigend_users.assigned_usernames IS NULL, TRUE, FALSE)              AS is_issue_unassigned,
  issues.group_label,
  issues.section_label,
  issues.stage_label,
  issues.type_label,
  issues.subtype_label,
  IFF(ARRAY_CONTAINS('infradev'::VARIANT, issues.labels), TRUE, FALSE) AS is_infradev,
  IFF(ARRAY_CONTAINS('fedramp::vulnerability'::VARIANT, issues.labels), TRUE, FALSE) as fedramp_vulnerability,
  issues.workflow_label,
  ROW_NUMBER() OVER (PARTITION BY daily_issue_id 
  ORDER BY LEAST(COALESCE(severity_label_valid_to, CURRENT_DATE),COALESCE(team_label_valid_to,CURRENT_DATE))) AS rn
FROM issues 
INNER JOIN dates 
  ON COALESCE(date_trunc('day',issues.created_at),dates.date_actual) <= dates.date_actual 
LEFT JOIN assigend_users
  ON issues.issue_id = assigend_users.dim_issue_id
LEFT JOIN label_groups as severity
  ON severity.label_type='severity'
  AND issues.issue_id = severity.dim_issue_id
  AND dates.date_actual BETWEEN DATE_TRUNC('day', severity.label_valid_from) AND DATE_TRUNC('day', severity.label_valid_to)
LEFT JOIN label_groups as team
  ON team.label_type='team'
  AND issues.issue_id = team.dim_issue_id
  AND dates.date_actual BETWEEN DATE_TRUNC('day', team.label_valid_from) AND DATE_TRUNC('day', team.label_valid_to))

  SELECT daily_issue_id,
       date_actual,
       issue_id,
       issue_iid,
       project_id,
       namespace_id,
       labels,
       issue_title,
       full_group_path,
       url,
       milestone_id,
       milestone_title,
       issue_state,
       issue_created_at,
       issue_closed_at,
       priority_label,
       severity,
       severity_label_added_at,
       assigned_team,
       team_label_added_at,
       team_label,
       issue_open_age_in_days,
       severity_label_age_in_days,
       assigned_usernames,
       is_issue_unassigned,
       group_label,
       section_label,
       stage_label,
       type_label,
       subtype_label,
       is_infradev,
       fedramp_vulnerability,
       workflow_label
  FROM final
  WHERE rn=1