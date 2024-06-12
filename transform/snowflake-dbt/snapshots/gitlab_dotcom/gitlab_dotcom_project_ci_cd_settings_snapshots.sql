{% snapshot gitlab_dotcom_project_ci_cd_settings_snapshots %}

    {{
        config(
          unique_key='id',
          strategy='check',
          check_cols=[
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
          ],
        )
    }}
    
    SELECT *       
    FROM {{ source('gitlab_dotcom', 'project_ci_cd_settings') }}
    
{% endsnapshot %}