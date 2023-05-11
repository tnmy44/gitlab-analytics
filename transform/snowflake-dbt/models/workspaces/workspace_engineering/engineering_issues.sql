WITH engineering_issues AS (

  SELECT *
  FROM {{ ref('internal_issues_enhanced') }}
  WHERE is_part_of_product = TRUE
)

SELECT issue_id,
       issue_iid,
       author_id,
       is_created_by_bot,
       project_id,
       created_at,
       updated_at,
       closed_at,
       created_month,
       closed_month,
       days_to_close,
       issue_title,
       issue_description,
       milestone_id,
       milestone_title,
       milestone_description,
       milestone_start_date,
       milestone_due_date,
       weight,
       namespace_id,
       labels,
       masked_label_title,
       is_community_contribution,
       is_security,
       is_corrective_action,
       priority_label,
       severity_label,
       group_label,
       section_label,
       stage_label,
       type_label,
       subtype_label,
       workflow_label,
       is_infradev,
       fedramp_vulnerability,
       visibility_level,
       project_path,
       full_group_path,
       url
FROM engineering_issues
