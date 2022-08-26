{{ config(
    tags=["mnpi_exception", "product"]
) }}

    SELECT
      reporting_month,
      dim_namespace_id,
      uuid,
      hostname,
      delivery_type,
      cleaned_version,
      dim_crm_account_id,
      crm_account_name,
      parent_crm_account_name,
      ping_date,
      umau_28_days_user,
      action_monthly_active_users_project_repo_28_days_user,
      merge_requests_28_days_user,
      projects_with_repositories_enabled_28_days_user,
      commit_comment_all_time_event,
      source_code_pushes_all_time_event,
      ci_pipelines_28_days_user,
      ci_internal_pipelines_28_days_user,
      ci_builds_28_days_user,
      ci_builds_all_time_user,
      ci_builds_all_time_event,
      ci_runners_all_time_event,
      auto_devops_enabled_all_time_event,
      gitlab_shared_runners_enabled,
      container_registry_enabled,
      template_repositories_all_time_event,
      ci_pipeline_config_repository_28_days_user,
      user_unique_users_all_secure_scanners_28_days_user,
      user_sast_jobs_28_days_user,
      user_dast_jobs_28_days_user,
      user_dependency_scanning_jobs_28_days_user,
      user_license_management_jobs_28_days_user,
      user_secret_detection_jobs_28_days_user,
      user_container_scanning_jobs_28_days_user,
      object_store_packages_enabled,
      projects_with_packages_all_time_event,
      projects_with_packages_28_days_user,
      deployments_28_days_user,
      releases_28_days_user,
      epics_28_days_user,
      issues_28_days_user,
      ci_internal_pipelines_all_time_event,
      ci_external_pipelines_all_time_event,
      merge_requests_all_time_event,
      todos_all_time_event,
      epics_all_time_event,
      issues_all_time_event,
      projects_all_time_event,
      deployments_28_days_event,
      packages_28_days_event,
      sast_jobs_all_time_event,
      dast_jobs_all_time_event,
      dependency_scanning_jobs_all_time_event,
      license_management_jobs_all_time_event,
      secret_detection_jobs_all_time_event,
      container_scanning_jobs_all_time_event,
      projects_jenkins_active_all_time_event,
      projects_bamboo_active_all_time_event,
      projects_jira_active_all_time_event,
      projects_drone_ci_active_all_time_event,
      projects_github_active_all_time_event,
      projects_jira_server_active_all_time_event,
      projects_jira_dvcs_cloud_active_all_time_event,
      projects_with_repositories_enabled_all_time_event,
      protected_branches_all_time_event,
      remote_mirrors_all_time_event,
      projects_enforcing_code_owner_approval_28_days_user,
      project_clusters_enabled_28_days_user,
      analytics_28_days_user,
      issues_edit_28_days_user,
      user_packages_28_days_user,
      terraform_state_api_28_days_user,
      incident_management_28_days_user,
      auto_devops_enabled,
      gitaly_clusters_instance,
      epics_deepest_relationship_level_instance,
      clusters_applications_cilium_all_time_event,
      network_policy_forwards_all_time_event,
      network_policy_drops_all_time_event,
      requirements_with_test_report_all_time_event,
      requirement_test_reports_ci_all_time_event,
      projects_imported_from_github_all_time_event,
      projects_jira_cloud_active_all_time_event,
      projects_jira_dvcs_server_active_all_time_event,
      service_desk_issues_all_time_event,
      ci_pipelines_all_time_user,
      service_desk_issues_28_days_user,
      projects_jira_active_28_days_user,
      projects_jira_dvcs_cloud_active_28_days_user,
      projects_jira_dvcs_server_active_28_days_user,
      merge_requests_with_required_code_owners_28_days_user,
      analytics_value_stream_28_days_event,
      code_review_user_approve_mr_28_days_user,
      epics_usage_28_days_user,
      ci_templates_usage_28_days_event,
      project_management_issue_milestone_changed_28_days_user,
      project_management_issue_iteration_changed_28_days_user,
      protected_branches_28_days_user,
      ci_cd_lead_time_usage_28_days_event,
      ci_cd_deployment_frequency_usage_28_days_event,
      projects_with_repositories_enabled_all_time_user,
      api_fuzzing_jobs_usage_28_days_user,
      coverage_fuzzing_pipeline_usage_28_days_event,
      api_fuzzing_pipeline_usage_28_days_event,
      container_scanning_pipeline_usage_28_days_event,
      dependency_scanning_pipeline_usage_28_days_event,
      sast_pipeline_usage_28_days_event,
      secret_detection_pipeline_usage_28_days_event,
      dast_pipeline_usage_28_days_event,
      coverage_fuzzing_jobs_28_days_user,
      environments_all_time_event,
      feature_flags_all_time_event,
      successful_deployments_28_days_event,
      failed_deployments_28_days_event,
      projects_compliance_framework_all_time_event,
      commit_ci_config_file_28_days_user,
      view_audit_all_time_user,
      dependency_scanning_jobs_all_time_user,
      analytics_devops_adoption_all_time_user,
      projects_imported_all_time_event,
      preferences_security_dashboard_28_days_user,
      web_ide_edit_28_days_user,
      auto_devops_pipelines_all_time_event,
      projects_prometheus_active_all_time_event,
      prometheus_enabled,
      prometheus_metrics_enabled,
      group_saml_enabled,
      jira_issue_imports_all_time_event,
      author_epic_all_time_user,
      author_issue_all_time_user,
      failed_deployments_28_days_user,
      successful_deployments_28_days_user,
      geo_enabled,
      geo_nodes_all_time_event,
      auto_devops_pipelines_28_days_user,
      active_instance_runners_all_time_event,
      active_group_runners_all_time_event,
      active_project_runners_all_time_event,
      gitaly_version,
      gitaly_servers_all_time_event,
      api_fuzzing_scans_all_time_event,
      api_fuzzing_scans_28_days_event,
      coverage_fuzzing_scans_all_time_event,
      coverage_fuzzing_scans_28_days_event,
      secret_detection_scans_all_time_event,
      secret_detection_scans_28_days_event,
      dependency_scanning_scans_all_time_event,
      dependency_scanning_scans_28_days_event,
      container_scanning_scans_all_time_event,
      container_scanning_scans_28_days_event,
      dast_scans_all_time_event,
      dast_scans_28_days_event,
      sast_scans_all_time_event,
      sast_scans_28_days_event,
      packages_pushed_registry_all_time_event,
      packages_pulled_registry_all_time_event,
      compliance_dashboard_view_28_days_user,
      audit_screen_view_28_days_user,
      instance_audit_screen_view_28_days_user,
      credential_inventory_view_28_days_user,
      compliance_frameworks_pipeline_all_time_event,
      compliance_frameworks_pipeline_28_days_event,
      groups_streaming_destinations_all_time_event,
      groups_streaming_destinations_28_days_event,
      audit_event_destinations_all_time_event,
      audit_event_destinations_28_days_event,
      projects_status_checks_all_time_event,
      external_status_checks_all_time_event,
      paid_license_search_28_days_user,
      last_activity_28_days_user,
      is_latest_data,

      -- METADATA COLUMNS FOR USE IN PUMP (NOT INTEGRATION)
      last_changed

    FROM {{ ref('mart_product_usage_free_user_metrics_monthly')}}
