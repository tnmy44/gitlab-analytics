WITH prep_ci_runner AS (

  SELECT
    dim_ci_runner_id,
    -- FOREIGN KEYS
    created_date_id,
    created_at,
    updated_at,
    ci_runner_description,

    --- CI Runner Manager Mapping
    CASE
      --- Private Runners
      WHEN ci_runner_description ILIKE '%private%manager%'
        THEN 'private-runner-mgr'
      --- Linux Runners
      WHEN ci_runner_description ILIKE 'shared-runners-manager%'
        THEN 'linux-runner-mgr'
      WHEN ci_runner_description ILIKE '%.shared.runners-manager.%'
        THEN 'linux-runner-mgr'
      WHEN ci_runner_description ILIKE '%saas-linux-%-amd64%'
        AND ci_runner_description NOT ILIKE '%shell%'
        THEN 'linux-runner-mgr'
      --- Internal GitLab Runners
      WHEN ci_runner_description ILIKE 'gitlab-shared-runners-manager%'
        THEN 'gitlab-internal-runner-mgr'
      --- Window Runners 
      WHEN ci_runner_description ILIKE 'windows-shared-runners-manager%'
        THEN 'windows-runner-mgr'
      --- Shared Runners
      WHEN ci_runner_description ILIKE '%.shared-gitlab-org.runners-manager.%'
        THEN 'shared-gitlab-org-runner-mgr'
      --- macOS Runners
      WHEN LOWER(ci_runner_description) ILIKE '%macos%'
        THEN 'macos-runner-mgr'
      --- Other
      ELSE 'Other'
    END         AS ci_runner_manager,

    --- CI Runner Machine Type Mapping
    CASE
      --- SaaS Linux Runners
      WHEN LOWER(ci_runner_description) ILIKE '%.shared.runners-manager.%'
        THEN 'Hosted Runner Linux - Small'
      WHEN LOWER(ci_runner_description) ILIKE '%.saas-linux-small-amd64%'
        THEN 'Hosted Runner Linux - Small'
      WHEN LOWER(ci_runner_description) ILIKE '%.saas-linux-medium-amd64%'
        AND LOWER(ci_runner_description) NOT ILIKE '%gpu-standard%'
        THEN 'Hosted Runner Linux - Medium - amd64'
      WHEN LOWER(ci_runner_description) ILIKE '%.saas-linux-medium-arm64.%'
        THEN 'Hosted Runner Linux - Medium - arm64'
      WHEN LOWER(ci_runner_description) ILIKE '%.saas-linux-medium-amd64-gpu-standard.%'
        THEN 'Hosted GPU-Enabled Runners'
      WHEN LOWER(ci_runner_description) ILIKE '%saas-linux-large-amd64%'
        THEN 'Hosted Runner Linux - Large'
      WHEN LOWER(ci_runner_description) ILIKE '%saas-linux-xlarge-amd64%'
        THEN 'Hosted Runner Linux - XLarge'
      WHEN LOWER(ci_runner_description) ILIKE '%saas-linux-2xlarge-amd64%'
        THEN 'Hosted Runner Linux - 2XLarge'
      --- MacOS Runners
      WHEN LOWER(ci_runner_description) ILIKE '%macos-medium-m1%'
        THEN 'Hosted Runners macOS - Medium - amd64'
      --- Window Runners 
      WHEN LOWER(ci_runner_description) ILIKE '_.saas-windows-%'
        THEN 'Hosted Runners Windows - Medium'
      ELSE 'Other'
    END         AS ci_runner_machine_type,
    contacted_at,
    is_active,
    ci_runner_version,
    revision,
    platform,
    is_untagged,
    is_locked,
    access_level,
    maximum_timeout,
    runner_type AS ci_runner_type,
    CASE runner_type
      WHEN 1 THEN 'shared'
      WHEN 2 THEN 'group-runner-hosted runners'
      WHEN 3 THEN 'project-runner-hosted runners'
    END         AS ci_runner_type_summary,
    public_projects_minutes_cost_factor,
    private_projects_minutes_cost_factor

  FROM {{ ref('prep_ci_runner') }}

)

{{ dbt_audit(
    cte_ref="prep_ci_runner",
    created_by="@snalamaru",
    updated_by="@nhervas",
    created_date="2021-06-23",
    updated_date="2024-04-22"
) }}
