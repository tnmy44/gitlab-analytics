{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_merge_request_id",
    "on_schema_change": "sync_all_columns"
    })
}}

{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan'),
    ('gitlab_dotcom_merge_request_metrics_source', 'gitlab_dotcom_merge_request_metrics_source'),
    ('prep_user', 'prep_user'),
    ('prep_milestone', 'prep_milestone'),
    ('prep_ci_pipeline', 'prep_ci_pipeline')
]) }}

, gitlab_dotcom_merge_requests_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_requests_source')}}
    {% if is_incremental() %}

      WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), renamed AS (

    SELECT

      {{ dbt_utils.surrogate_key(['gitlab_dotcom_merge_requests_source.merge_request_id'])}}    AS dim_merge_request_sk,

      gitlab_dotcom_merge_requests_source.merge_request_id                                      AS merge_request_id,

      -- LEGACY NATURAL KEY
      gitlab_dotcom_merge_requests_source.merge_request_id                                      AS dim_merge_request_id,

      -- FOREIGN KEYS
      prep_project.dim_project_sk,
      target_project.dim_project_sk                                                             AS dim_project_sk_target,
      prep_project.dim_namespace_sk,
      prep_project.ultimate_parent_namespace_id,
      prep_milestone.dim_milestone_sk,
      prep_date.date_id                                                                         AS created_date_id,
      prep_gitlab_dotcom_plan.dim_plan_sk                                                       AS dim_plan_sk_at_creation,
      author.dim_user_sk                                                                        AS dim_user_sk_author,
      assignee.dim_user_sk                                                                      AS dim_user_sk_assignee,
      merge_user.dim_user_sk                                                                    AS dim_user_sk_merge_user,
      updated_by.dim_user_sk                                                                    AS dim_user_sk_updated_by,
      last_edited_by.dim_user_sk                                                                AS dim_user_sk_last_edited_by,
      merged_by.dim_user_sk                                                                     AS dim_user_sk_merged_by,
      latest_closed_by.dim_user_sk                                                              AS dim_user_sk_latest_closed_by,
      prep_ci_pipeline.dim_ci_pipeline_sk                                                       AS dim_ci_pipeline_sk_head,
      gitlab_dotcom_merge_requests_source.latest_merge_request_diff_id, -- currently no common model, surrogate key can be added when this is created

      -- maintained for prep_event until we have fully migrated all gitlab_dotcom data and can redo prep_event entirely
      prep_project.project_id                                                                   AS project_id,
      --target_project.project_id                                                                 AS project_id_target,
      IFNULL(prep_namespace_plan_hist.dim_plan_id, 34)                                          AS dim_plan_id_at_creation,
      gitlab_dotcom_merge_requests_source.author_id,


      -- merge request attributes
      gitlab_dotcom_merge_requests_source.merge_request_iid                                     AS merge_request_internal_id,
      gitlab_dotcom_merge_requests_source.merge_request_title,
      gitlab_dotcom_merge_requests_source.is_merge_to_master,
      gitlab_dotcom_merge_requests_source.merge_error,
      gitlab_dotcom_merge_requests_source.approvals_before_merge,
      gitlab_dotcom_merge_requests_source.lock_version,
      gitlab_dotcom_merge_requests_source.time_estimate,
      gitlab_dotcom_merge_requests_source.merge_request_state_id,
      gitlab_dotcom_merge_requests_source.merge_request_state,
      gitlab_dotcom_merge_requests_source.merge_request_status,
      gitlab_dotcom_merge_requests_source.does_merge_when_pipeline_succeeds,
      gitlab_dotcom_merge_requests_source.does_squash,
      gitlab_dotcom_merge_requests_source.is_discussion_locked,
      gitlab_dotcom_merge_requests_source.does_allow_maintainer_to_push,
      gitlab_dotcom_merge_requests_source.created_at,
      gitlab_dotcom_merge_requests_source.updated_at,
      gitlab_dotcom_merge_requests_source.merge_request_last_edited_at,

      -- merge request metrics
      gitlab_dotcom_merge_request_metrics_source.merged_at,
      gitlab_dotcom_merge_request_metrics_source.first_comment_at,
      gitlab_dotcom_merge_request_metrics_source.latest_closed_at,
      gitlab_dotcom_merge_request_metrics_source.first_approved_at,
      gitlab_dotcom_merge_request_metrics_source.latest_build_started_at,
      gitlab_dotcom_merge_request_metrics_source.removed_lines,
      gitlab_dotcom_merge_request_metrics_source.added_lines,
      gitlab_dotcom_merge_request_metrics_source.modified_paths_size,
      gitlab_dotcom_merge_request_metrics_source.diff_size,
      gitlab_dotcom_merge_request_metrics_source.commits_count

    FROM gitlab_dotcom_merge_requests_source
    LEFT JOIN prep_project
      ON gitlab_dotcom_merge_requests_source.project_id = prep_project.dim_project_id
    LEFT JOIN prep_project target_project
      ON gitlab_dotcom_merge_requests_source.target_project_id = target_project.dim_project_id
    LEFT JOIN prep_namespace_plan_hist
      ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_merge_requests_source.created_at >= prep_namespace_plan_hist.valid_from
      AND gitlab_dotcom_merge_requests_source.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN prep_date
      ON TO_DATE(gitlab_dotcom_merge_requests_source.created_at) = prep_date.date_day
    LEFT JOIN prep_gitlab_dotcom_plan
      ON IFNULL(prep_namespace_plan_hist.dim_plan_id, 34) = prep_gitlab_dotcom_plan.dim_plan_id
    LEFT JOIN gitlab_dotcom_merge_request_metrics_source
      ON gitlab_dotcom_merge_requests_source.merge_request_id = gitlab_dotcom_merge_request_metrics_source.merge_request_id
    LEFT JOIN prep_milestone
      ON gitlab_dotcom_merge_requests_source.milestone_id = prep_milestone.milestone_id
    LEFT JOIN prep_user author
      ON gitlab_dotcom_merge_requests_source.author_id = author.user_id
    LEFT JOIN prep_user assignee
      ON gitlab_dotcom_merge_requests_source.assignee_id = assignee.user_id
    LEFT JOIN prep_user merge_user
      ON gitlab_dotcom_merge_requests_source.merge_user_id = merge_user.user_id
    LEFT JOIN prep_user updated_by
      ON gitlab_dotcom_merge_requests_source.updated_by_id = updated_by.user_id
    LEFT JOIN prep_user last_edited_by
      ON gitlab_dotcom_merge_requests_source.last_edited_by_id = last_edited_by.user_id
    LEFT JOIN prep_user merged_by
      ON gitlab_dotcom_merge_request_metrics_source.merged_by_id = merged_by.user_id
    LEFT JOIN prep_user latest_closed_by
      ON gitlab_dotcom_merge_request_metrics_source.latest_closed_by_id = latest_closed_by.user_id
    LEFT JOIN prep_ci_pipeline
      ON gitlab_dotcom_merge_requests_source.head_pipeline_id = prep_ci_pipeline.ci_pipeline_id
    WHERE gitlab_dotcom_merge_requests_source.project_id IS NOT NULL

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-06-17",
    updated_date="2023-10-26"
) }}
