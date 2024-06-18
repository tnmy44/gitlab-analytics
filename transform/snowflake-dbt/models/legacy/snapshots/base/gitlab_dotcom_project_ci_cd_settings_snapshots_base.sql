{{ config(

    materialized='incremental',
    unique_key='project_ci_cd_settings_snapshot_pk',
    alias='gitlab_dotcom_project_ci_cd_settings_snapshot'
  )

}}

WITH source AS (

  SELECT
    *,
    TO_TIMESTAMP(_uploaded_at::INT)                                         AS uploaded_at,
    {{ 
         dbt_utils.generate_surrogate_key(
           [
             'project_id',
             'group_runners_enabled',
             'merge_pipelines_enabled',
             'default_git_depth',
             'forward_deployment_enabled',
             'merge_trains_enabled',
             'auto_rollback_enabled',
             'keep_latest_artifact',
             'restrict_user_defined_variables',
             'job_token_scope_enabled',
             'runner_token_expiration_interval',
             'separated_caches',
             'allow_fork_pipelines_to_run_in_parent_project',
             'inbound_job_token_scope_enabled'
           ]
         ) 
    }}                                                                      AS record_checksum
  FROM {{ source('gitlab_dotcom', 'project_ci_cd_settings') }}
  {% if is_incremental() %}

  WHERE uploaded_at > (SELECT MAX(record_checked_at) FROM {{ this }} )
    AND record_checksum not in (SELECT record_checksum FROM {{ this }} WHERE valid_to is null )

  {% endif %}

),

base AS (

  SELECT
    *,
    LEAD(_uploaded_at) OVER (PARTITION BY id ORDER BY _uploaded_at)          AS next_uploaded_at,
    LAG(record_checksum, 1, '') OVER (PARTITION BY id ORDER BY _uploaded_at) AS lag_checksum,
    CONDITIONAL_TRUE_EVENT(record_checksum != lag_checksum)
      OVER (PARTITION BY id ORDER BY _uploaded_at)                           AS checksum_group
  FROM source

),

grouped AS (

  SELECT
    id                                   AS project_ci_cd_settings_snapshot_id,
    project_id,
    group_runners_enabled,
    merge_pipelines_enabled,
    default_git_depth,
    forward_deployment_enabled,
    merge_trains_enabled,
    auto_rollback_enabled,
    keep_latest_artifact,
    restrict_user_defined_variables,
    job_token_scope_enabled,
    runner_token_expiration_interval,
    separated_caches,
    allow_fork_pipelines_to_run_in_parent_project,
    inbound_job_token_scope_enabled,
    record_checksum,
    CURRENT_TIMESTAMP                    AS record_checked_at,
    TO_TIMESTAMP(MIN(_uploaded_at)::INT) AS valid_from,
    IFF(
      MAX(COALESCE(next_uploaded_at, 9999999999) = 9999999999),
      NULL, TO_TIMESTAMP(MAX(next_uploaded_at)::INT)
    )                                    AS valid_to
  FROM base
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, checksum_group

)

SELECT
  *,
  {{
      dbt_utils.generate_surrogate_key(
        [
          'project_ci_cd_settings_snapshot_id',
          'valid_from',
          'valid_to'
        ]
      ) 
  }}                                              AS project_ci_cd_settings_snapshot_pk
FROM grouped
