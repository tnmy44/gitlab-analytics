WITH internal_issues AS (

  SELECT *
  FROM {{ ref('internal_issues') }}

),

cte_ns_explode AS (

  SELECT
    namespace_id,
    ultimate_parent_id,
    upstream_lineage,
    s.value AS lineage_namespace,
    s.index AS rn
  FROM {{ ref('gitlab_dotcom_namespace_lineage_scd') }},
    LATERAL FLATTEN(upstream_lineage, OUTER=>TRUE) AS s
  WHERE is_current

),

cte_ns_get_path AS (

  SELECT
    a.namespace_id,
    a.ultimate_parent_id,
    a.upstream_lineage,
    lineage_namespace,
    rn,
    b.namespace_path
  FROM cte_ns_explode AS a
  LEFT JOIN {{ ref('dim_namespace') }} AS b ON a.lineage_namespace = b.dim_namespace_id

),

cte_ns_restructure AS (

  SELECT
    namespace_id,
    ultimate_parent_id,
    upstream_lineage,
    ARRAY_AGG(namespace_path) WITHIN GROUP (ORDER BY rn) AS regroup
  FROM cte_ns_get_path
  GROUP BY namespace_id,
    ultimate_parent_id,
    upstream_lineage

),

namespaces AS (

  SELECT
    namespace_id,
    ultimate_parent_id,
    upstream_lineage,
    ARRAY_TO_STRING(regroup, '/') AS full_group_path
  FROM cte_ns_restructure

),

product_categories_yml_base AS (

  SELECT DISTINCT
    LOWER(group_name)                                                         AS group_name,
    LOWER(stage_section)                                                      AS section_name,
    LOWER(stage_display_name)                                                 AS stage_name,
    IFF(group_name LIKE '%::%', SPLIT_PART(LOWER(group_name), '::', 1), NULL) AS root_name
  FROM {{ ref('stages_groups_yaml_source') }}
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {{ ref('stages_groups_yaml_source') }})

),

product_categories_yml AS (

  SELECT
    group_name,
    section_name,
    stage_name
  FROM product_categories_yml_base
  UNION ALL
  SELECT DISTINCT
    root_name AS group_name,
    section_name,
    stage_name
  FROM product_categories_yml_base
  WHERE root_name IS NOT NULL

),

bot_users AS (

  SELECT dim_user_id
  FROM {{ ref('dim_user') }}
  WHERE email_domain LIKE '%noreply.gitlab.com'

),

milestones AS (

  SELECT *,
  CASE
        WHEN group_id=9970
        AND start_date <= DATEADD('month', 1, CURRENT_DATE)
        and regexp_like(milestone_title, '\\d+\.\\d+') THEN 
        DENSE_RANK() OVER (
            PARTITION BY IFF(
                group_id=9970
        AND start_date <= DATEADD('month', 1, CURRENT_DATE)
        and regexp_like(milestone_title, '\\d+\.\\d+'),
                1,
                0
            )
            ORDER BY
                start_date DESC
        )
        else null
    END as milestone_recency
  FROM {{ ref('gitlab_dotcom_milestones') }}

),

workflow_labels AS (

  SELECT * FROM {{ ref('engineering_analytics_workflow_labels') }}

),

excluded_project_path as (
select 'team-tasks' as project_path union
select 'ux-research' union
select 'design.gitlab.com' union
select 'pajamas-adoption-scanner' union
select 'gitlab-design' union 
select 'technical-writing' union
select 'fulfillment-meta'  union
select 'verify-stage' union
select 'team' union 
select 'group-tasks' union 
select 'discussion' union
select 'vulnerability-research' 
),

final AS (

  SELECT
    internal_issues.issue_id                                                                                                                                                                                                                                                                            AS issue_id,
    internal_issues.issue_iid                                                                                                                                                                                                                                                                           AS issue_iid,
    internal_issues.author_id                                                                                                                                                                                                                                                                           AS author_id,
    IFF(bots.dim_user_id IS NOT NULL OR internal_issues.author_id = 1786152 OR ARRAY_CONTAINS('automation:bot-authored'::VARIANT, internal_issues.labels),
      TRUE, FALSE)                                                                                                                                                                                                                                                                                      AS is_created_by_bot,
    internal_issues.project_id                                                                                                                                                                                                                                                                          AS project_id,
    internal_issues.issue_created_at                                                                                                                                                                                                                                                                    AS created_at,
    internal_issues.issue_updated_at                                                                                                                                                                                                                                                                    AS updated_at,
    internal_issues.issue_closed_at                                                                                                                                                                                                                                                                     AS closed_at,
    internal_issues.state,
    DATE_TRUNC('month', internal_issues.issue_created_at)::DATE                                                                                                                                                                                                                                         AS created_month,
    DATE_TRUNC('month', internal_issues.issue_closed_at)::DATE                                                                                                                                                                                                                                          AS closed_month,
    IFF(internal_issues.issue_closed_at > internal_issues.issue_created_at, ROUND(TIMESTAMPDIFF(HOURS, internal_issues.issue_created_at, internal_issues.issue_closed_at) / 24, 2), 0)                                                                                                                  AS days_to_close,
    internal_issues.issue_title                                                                                                                                                                                                                                                                         AS issue_title,
    internal_issues.issue_description                                                                                                                                                                                                                                                                   AS issue_description,
    internal_issues.milestone_id                                                                                                                                                                                                                                                                        AS milestone_id,
    milestones.milestone_title                                                                                                                                                                                                                                                                          AS milestone_title,
    milestones.milestone_description                                                                                                                                                                                                                                                                    AS milestone_description,
    milestones.start_date                                                                                                                                                                                                                                                                               AS milestone_start_date,
    milestones.due_date                                                                                                                                                                                                                                                                                 AS milestone_due_date,
    internal_issues.weight                                                                                                                                                                                                                                                                              AS weight,
    internal_issues.namespace_id                                                                                                                                                                                                                                                                        AS namespace_id,
    internal_issues.ultimate_parent_id                                                                                                                                                                                                                                                                  AS ultimate_parent_id,
    internal_issues.labels                                                                                                                                                                                                                                                                              AS labels,
    ARRAY_TO_STRING(internal_issues.labels, '|')                                                                                                                                                                                                                                                        AS masked_label_title,
    ARRAY_CONTAINS('community contribution'::VARIANT, internal_issues.labels)                                                                                                                                                                                                                           AS is_community_contribution,
    ARRAY_CONTAINS('sus::impacting'::VARIANT, internal_issues.labels)                                                                                                                                                                                                                                   AS is_sus_impacting,
    ARRAY_CONTAINS('corrective action'::VARIANT, internal_issues.
      labels)                                                                                                                                                                                                                                                                                           AS is_corrective_action,
    internal_issues.priority_tag                                                                                                                                                                                                                                                                        AS priority_label,
    internal_issues.severity_tag                                                                                                                                                                                                                                                                        AS severity_label,
    CASE
      WHEN ARRAY_CONTAINS('group::gitaly::cluster'::VARIANT, internal_issues.labels)
        THEN 'gitaly::cluster'
      WHEN ARRAY_CONTAINS('group::gitaly::git'::VARIANT, internal_issues.labels)
        THEN 'gitaly::git'
      WHEN ARRAY_CONTAINS('group::distribution::build'::VARIANT, internal_issues.labels)
        THEN 'distribution::build'
      WHEN ARRAY_CONTAINS('group::distribution::deploy'::VARIANT, internal_issues.labels)
        THEN 'distribution::deploy'
      WHEN ARRAY_CONTAINS('group::distribution::operate'::VARIANT, internal_issues.labels)
        THEN 'distribution::operate'
      ELSE
        IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bgroup::*([^,]*)'), 'group::', '') IN (SELECT group_name FROM product_categories_yml), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bgroup::*([^,]*)'), 'group::', ''), 'undefined') END   AS group_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bsection::*([^,]*)'), 'section::', '') IN (SELECT section_name FROM product_categories_yml), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bsection::*([^,]*)'), 'section::', ''), 'undefined') AS section_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bdevops::*([^,]*)'), 'devops::', '') IN (SELECT stage_name FROM product_categories_yml), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bdevops::*([^,]*)'), 'devops::', ''), 'undefined')       AS stage_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\btype::*([^,]*)'), 'type::', '') IN ('bug', 'feature', 'maintenance'), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\btype::*([^,]*)'), 'type::', ''), 'undefined')
    AS type_label,
    CASE
      WHEN type_label = 'bug'
        THEN COALESCE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bbug::*([^,]*)'), 'undefined')
      WHEN type_label = 'maintenance'
        THEN COALESCE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bmaintenance::*([^,]*)'), 'undefined')
      WHEN type_label = 'feature'
        THEN COALESCE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bfeature::*([^,]*)'), 'undefined')
      ELSE 'undefined' END                                                                                                                                                                                                                                                                              AS subtype_label,
    COALESCE(subtype_label = 'bug::vulnerability' --change requested by James & Darva correct way to label vulnerability issues
      AND internal_issues.namespace_id NOT IN (5821789, 1819570, 1986712, 2139148, 4955423, 3786502)
      AND NOT ARRAY_CONTAINS('feature'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('test'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('securitybot::ignore'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('documentation'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('type::feature'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('fedramp::dr status::open'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('vulnerability::vendor package::will not be fixed'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('vulnerability::vendor package::fix unavailable'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('vulnerability::vendor base container::will not be fixed'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('vulnerability::vendor base container::fix unavailable'::VARIANT, internal_issues.labels)
      AND NOT ARRAY_CONTAINS('featureflag::disabled'::VARIANT, internal_issues.labels), FALSE)                                                                                                                                                                                                          AS is_security,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bworkflow::*([^,]*)'), 'workflow::', '') IN (SELECT workflow_label FROM workflow_labels), REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bworkflow::*([^,]*)'), 'workflow::', ''), 'undefined')  AS workflow_label,
    IFF(ARRAY_CONTAINS('infradev'::VARIANT, internal_issues.labels), TRUE, FALSE)                                                                                                                                                                                                                       AS is_infradev,
    IFF(ARRAY_CONTAINS('fedramp::vulnerability'::VARIANT, internal_issues.labels), TRUE, FALSE)                                                                                                                                                                                                         AS fedramp_vulnerability,
    projects.visibility_level                                                                                                                                                                                                                                                                           AS visibility_level,
    projects.project_path                                                                                                                                                                                                                                                                               AS project_path,
    ns.full_group_path                                                                                                                                                                                                                                                                                  AS full_group_path,

    CASE
      WHEN projects.visibility_level = 'public' AND internal_issues.issue_is_confidential = FALSE
        THEN '[' || REPLACE(REPLACE(LEFT(internal_issues.issue_title, 64), '[', ''), ']', '') || '](https://gitlab.com/' || full_group_path || '/' || projects.project_path || '/-/issues/' || internal_issues.issue_iid || ')'
      ELSE 'https://gitlab.com/' || full_group_path || '/' || projects.project_path || '/-/issues/' || internal_issues.issue_iid
    END                                                                                                                                                                                                                                                                                                 AS url,
    internal_issues.is_part_of_product,
    milestones.milestone_recency AS milestone_recency,
    coalesce(internal_issues.is_part_of_product and internal_issues.namespace_id in (6543,9970) and projects.project_path not in (select * from excluded_project_path) and masked_label_title not like '%type::ignore%' and is_part_of_product and group_label!='undefined', False) as is_milestone_issue_reporting --this logic comes from [issues_by_milestone] sisense snippet
  FROM internal_issues
  LEFT JOIN {{ ref('dim_project') }} AS projects
    ON projects.dim_project_id = internal_issues.project_id
  LEFT JOIN bot_users AS bots
    ON bots.dim_user_id = internal_issues.author_id
  LEFT JOIN namespaces AS ns
    ON ns.namespace_id = projects.dim_namespace_id
  LEFT JOIN milestones
    ON milestones.milestone_id = internal_issues.milestone_id
)

SELECT *
FROM final
