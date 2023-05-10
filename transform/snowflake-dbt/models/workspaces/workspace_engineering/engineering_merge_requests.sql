WITH internal_merge_requests AS (

  SELECT *
  FROM {{ ref('internal_merge_requests') }}

), namespaces AS (

    SELECT *
    FROM {{ ref('dim_namespace') }}

), product_categories_yml_base AS (

    SELECT
        DISTINCT LOWER(group_name) AS group_name,
        LOWER(stage_section) AS section_name,
        LOWER(stage_display_name) AS stage_name,
        IFF(group_name LIKE '%::%',SPLIT_PART(LOWER(group_name),'::',1),NULL) as root_name
    FROM {{ ref('stages_groups_yaml_source') }}
    WHERE snapshot_date = (SELECT max(snapshot_date) FROM {{ ref('stages_groups_yaml_source') }})

), product_categories_yml AS (

    SELECT group_name,
       section_name,
       stage_name
    FROM product_categories_yml_base
    UNION ALL
    SELECT DISTINCT root_name AS group_name,
                section_name,
                stage_name
    FROM product_categories_yml_base
    WHERE root_name IS NOT NULL
    
), bot_users AS (

    SELECT dim_user_id
    FROM {{ ref('dim_user') }}
    WHERE email_domain LIKE '%noreply.gitlab.com'

), milestones AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_milestones') }}

),engineering_merge_requests AS (

  SELECT
    internal_merge_requests.merge_request_id                                                                                    AS merge_request_id,
    internal_merge_requests.merge_request_iid                                                                                   AS merge_request_iid,
    internal_merge_requests.author_id                                                                                           AS author_id,
    IFF(bots.dim_user_id IS NOT NULL or internal_merge_requests.author_id = 1786152 or ARRAY_CONTAINS('automation:bot-authored'::variant, internal_merge_requests.labels),
        TRUE, FALSE)                                                                                                            AS is_created_by_bot,
    internal_merge_requests.assignee_id                                                                                         AS assignee_id,
    internal_merge_requests.project_id                                                                                          AS project_id,
    internal_merge_requests.target_project_id                                                                                   AS target_project_id,
    internal_merge_requests.merge_request_state                                                                                 AS merge_request_state,
    internal_merge_requests.created_at                                                                                          AS created_at,
    internal_merge_requests.updated_at                                                                                          AS updated_at,
    internal_merge_requests.merged_at                                                                                           AS merged_at,
    DATE_TRUNC('month',internal_merge_requests.created_at)::date                                                                AS created_month,
    DATE_TRUNC('month',internal_merge_requests.merged_at)::date                                                                 AS merge_month,
    ROUND(TIMESTAMPDIFF(HOURS, internal_merge_requests.merge_request_created_at, internal_merge_requests.merged_at)/24,2)       AS days_to_merge,
    internal_merge_requests.merge_request_title                                                                                 AS merge_request_title,
    internal_merge_requests.merge_request_description                                                                           AS merge_request_description,
    internal_merge_requests.milestone_id                                                                                        AS milestone_id,
    internal_merge_requests.milestone_title                                                                                     AS milestone_title,
    internal_merge_requests.milestone_description                                                                               AS milestone_description,
    milestones.start_date AS milestone_start_date,
    milestones.due_date AS milestone_due_date,
    internal_merge_requests.namespace_id                                                                                        AS namespace_id,
    internal_merge_requests.ultimate_parent_id AS ultimate_parent_id,
    internal_merge_requests.labels                                                                                              AS labels,
    ARRAY_TO_STRING(internal_merge_requests.labels,'|')                                                                         AS masked_label_title,
    ARRAY_CONTAINS('community contribution'::variant, internal_merge_requests.labels)                                           AS is_community_contribution,
    ARRAY_CONTAINS('security'::variant, internal_merge_requests.labels)                                                         AS is_security,
    IFNULL(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bpriority::([0-9]+)'), 'priority::', ''),'undefined')                                                                                                                    AS priority_label,
    IFNULL(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bseverity::([0-9]+)'), 'severity::', ''),'undefined')                                                                                                                    AS severity_label,
    CASE
      WHEN array_contains('group::gitaly::cluster'::variant,internal_merge_requests.labels)
        THEN 'gitaly::cluster'
      WHEN array_contains('group::gitaly::git'::variant,internal_merge_requests.labels)
        THEN 'gitaly::git'
      WHEN array_contains('group::distribution::build'::variant,internal_merge_requests.labels)
        THEN 'distribution::build'
      WHEN array_contains('group::distribution::deploy'::variant,internal_merge_requests.labels)
        THEN 'distribution::deploy'
      WHEN array_contains('group::distribution::operate'::variant,internal_merge_requests.labels)
        THEN 'distribution::operate'
        ELSE
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bgroup::*([^,]*)'), 'group::', '') IN (SELECT group_name FROM product_categories_yml),REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bgroup::*([^,]*)'), 'group::', ''),'undefined') END                    AS group_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bsection::*([^,]*)'), 'section::', '') IN (SELECT section_name FROM product_categories_yml),REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bsection::*([^,]*)'), 'section::', ''),'undefined')                                                                                                                AS section_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bdevops::*([^,]*)'), 'devops::', '') IN (SELECT stage_name FROM product_categories_yml),REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\bdevops::*([^,]*)'), 'devops::', ''),'undefined')                                                                                                                    AS stage_label,
    IFF(REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\btype::*([^,]*)'), 'type::', '') IN ('bug','feature','maintenance'),REPLACE(REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','), '\\btype::*([^,]*)'), 'type::', ''),'undefined') 
                                                                                                                                AS type_label,
    CASE
        WHEN type_label = 'bug' 
            THEN REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','),'\\bbug::*([^,]*)') 
        WHEN type_label = 'maintenance' 
            THEN REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','),'\\bmaintenance::*([^,]*)') 
        WHEN type_label = 'feature' 
            THEN REGEXP_SUBSTR(ARRAY_TO_STRING(internal_merge_requests.labels, ','),'\\bfeature::*([^,]*)') 
        ELSE 'undefined' END                                                                                                    AS subtype_label,
    projects.visibility_level                                                                                                   AS visibility_level,
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
            THEN '[' || REPLACE(REPLACE(LEFT(internal_merge_requests.merge_request_title,64),'[',''),']','') ||'](https://gitlab.com/' || full_group_path || '/' || projects.project_path || '/merge_requests/' || internal_merge_requests.merge_request_iid || ')'
        ELSE 'https://gitlab.com/' || full_group_path || '/' || projects.project_path || '/merge_requests/' || internal_merge_requests.merge_request_iid 
        END                                                                                                                     AS url
  FROM internal_merge_requests
  LEFT JOIN {{ ref('dim_project') }} AS projects
    ON projects.dim_project_id = internal_merge_requests.target_project_id
  LEFT JOIN bot_users AS bots
    ON bots.dim_user_id = internal_merge_requests.author_id
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
    ON milestones.milestone_id = internal_merge_requests.milestone_id
  WHERE is_part_of_product = TRUE
)

SELECT *
FROM engineering_merge_requests
