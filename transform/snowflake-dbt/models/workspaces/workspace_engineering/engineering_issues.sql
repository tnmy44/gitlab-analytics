WITH internal_issues AS (

  SELECT *
  FROM {{ ref('internal_issues') }}

), namespaces AS (

    SELECT *
    FROM {{ ref('dim_namespace') }}

), stages_yml AS (

    SELECT
        DISTINCT LOWER(stage_display_name) AS stage_name,
        LOWER(stage_section) AS section_name
    FROM {{ ref('stages_yaml_source') }}
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM {{ ref('stages_yaml_source') }})

), groups_yml AS (

    SELECT DISTINCT LOWER(group_name) AS group_name
    FROM {{ ref('stages_groups_yaml_source') }}
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM {{ ref('stages_groups_yaml_source') }})

), bot_users AS (

    SELECT dim_user_id
    FROM {{ ref('dim_user') }}
    WHERE email_domain LIKE '%noreply.gitlab.com'

), milestones AS (

	SELECT *
    FROM {{ ref('gitlab_dotcom_milestones') }}

), engineering_issues AS (

  SELECT
    internal_issues.issue_id                                                                                    AS issue_id,
    internal_issues.issue_iid                                                                                   AS issue_iid,
    internal_issues.author_id                                                                                           AS author_id,
    IFF(bots.dim_user_id IS NOT NULL or internal_issues.author_id = 1786152 or ARRAY_CONTAINS('automation:bot-authored'::variant, internal_issues.labels),
        TRUE, FALSE)                                                                                                            AS is_created_by_bot,
    internal_issues.project_id                                                                                          AS project_id,
    internal_issues.issue_created_at                                                                                    AS created_at,
    internal_issues.issue_updated_at                                                                                    AS updated_at,
    internal_issues.issue_closed_at                                                                                     AS closed_at,
    DATE_TRUNC('month',internal_issues.issue_created_at)::date                                                          AS created_month,
    DATE_TRUNC('month',internal_issues.issue_closed_at)::date                                                           AS closed_month,
    ROUND(TIMESTAMPDIFF(HOURS, internal_issues.issue_created_at, internal_issues.issue_closed_at)/24,2)       AS days_to_close,
    internal_issues.issue_title                                                                                 AS issue_title,
    internal_issues.issue_description                                                                           AS issue_description,
    internal_issues.milestone_id                                                                                        AS milestone_id,
    milestones.milestone_title                                                                                     AS milestone_title,
    milestones.milestone_description                                                                               AS milestone_description,
    milestones.start_date                                                                                           AS milestone_start_date,
    milestones.due_date                                                                                                 AS milestone_due_date,
    internal_issues.weight                                                                                        AS weight,
    internal_issues.namespace_id                                                                                        AS namespace_id,
    internal_issues.labels                                                                                              AS labels,
    ARRAY_TO_STRING(internal_issues.labels,'|')                                                                         AS masked_label_title,
    ARRAY_CONTAINS('community contribution'::variant, internal_issues.labels)                                           AS is_community_contribution,
    ARRAY_CONTAINS('security'::variant, internal_issues.labels)                                                         AS is_security,
    internal_issues.priority_tag                                                                                        AS priority_label,
    internal_issues.severity_tag                                                                                        AS severity_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bgroup::*([^,]*)'), 'group::', '') IN (SELECT group_name FROM groups_yml),REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bgroup::*([^,]*)'), 'group::', ''),'undefined')                                                                                                                    AS group_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bsection::*([^,]*)'), 'section::', '') IN (SELECT section_name FROM stages_yml),REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bsection::*([^,]*)'), 'section::', ''),'undefined')                                                                                                                AS section_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bdevops::*([^,]*)'), 'devops::', '') IN (SELECT stage_name FROM stages_yml),REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\bdevops::*([^,]*)'), 'devops::', ''),'undefined')                                                                                                                    AS stage_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\btype::*([^,]*)'), 'type::', '') IN ('bug','feature','maintenance'),REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','), '\\btype::*([^,]*)'), 'type::', ''),'undefined') 
                                                                                                                                AS type_label,
    CASE
        WHEN type_label = 'bug' 
            THEN IFNULL(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','),'\\bbug::*([^,]*)'),'undefined') 
        WHEN type_label = 'maintenance' 
            THEN IFNULL(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','),'\\bmaintenance::*([^,]*)'),'undefined') 
        WHEN type_label = 'feature' 
            THEN IFNULL(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_issues.labels, ','),'\\bfeature::*([^,]*)'),'undefined') 
        ELSE 'undefined' END                                                                                                    AS subtype_label,
    IFNULL(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(labels, ','),'\\bworkflow::*([^,]*)'), 'workflow::', ''),'undefined')          AS workflow_label,
    projects.visibility_level                                                                                                   AS visibility_level,
    projects.project_path                                                                                                       AS project_path,
    CASE
        WHEN ns_4.dim_namespace_id IS NOT NULL
            THEN ns_4.namespace_path || '/' || ns_3.namespace_path || '/' || ns_2.namespace_path || '/' || ns_1.namespace_path || '/' ||  ns.namespace_path
        WHEN ns_3.dim_namespace_id IS NOT NULL 
            THEN ns_3.namespace_path || '/' || ns_2.namespace_path || '/' || ns_1.namespace_path || '/' ||  ns.namespace_path
        WHEN ns_2.dim_namespace_id IS NOT NULL 
            THEN ns_2.namespace_path || '/' || ns_1.namespace_path || '/' ||  ns.namespace_path
        WHEN ns_1.dim_namespace_id IS NOT NULL 
            THEN ns_1.namespace_path || '/' || ns.namespace_path
        ELSE  ns.namespace_path END                                                                                             AS full_group_path,
    CASE
        WHEN projects.visibility_level = 'public'
            THEN '[' || REPLACE(REPLACE(LEFT(internal_issues.issue_title,64),'[',''),']','') ||'](https://gitlab.com/' || full_group_path || '/' || projects.project_path || '/issues/' || internal_issues.issue_iid || ')'
        ELSE 'https://gitlab.com/' || full_group_path || '/' || projects.project_path || '/issues/' || internal_issues.issue_iid 
        END                                                                                                                     AS url
  FROM internal_issues
  LEFT JOIN {{ ref('dim_project') }} AS projects
    ON projects.dim_project_id = internal_issues.project_id
  LEFT JOIN bot_users AS bots
    ON bots.dim_user_id = internal_issues.author_id
  LEFT JOIN namespaces AS ns
    ON ns.dim_namespace_id = projects.dim_namespace_id
  LEFT JOIN namespaces ns_1 
    ON ns_1.dim_namespace_id = ns.parent_id and ns.namespace_is_ultimate_parent = FALSE
  LEFT JOIN namespaces ns_2 
    ON ns_2.dim_namespace_id = ns_1.parent_id and ns_1.namespace_is_ultimate_parent = FALSE
  LEFT JOIN namespaces ns_3 
    ON ns_3.dim_namespace_id = ns_2.parent_id and ns_2.namespace_is_ultimate_parent = FALSE
  LEFT JOIN namespaces ns_4 
    ON ns_4.dim_namespace_id = ns_3.parent_id and ns_3.namespace_is_ultimate_parent = FALSE
  LEFT JOIN milestones
  	ON milestones.milestone_id = internal_issues.milestone_id
  WHERE is_part_of_product = TRUE
)

SELECT *
FROM engineering_issues
