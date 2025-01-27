{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}


{{ simple_cte([
    ('dim_gitlab_releases', 'dim_gitlab_releases'),
    ('dim_ping_metric', 'dim_ping_metric'),
    ('fct_ping_instance_metric', 'fct_ping_instance_metric'),
    ('dim_ping_instance', 'dim_ping_instance')
    ])
}}

,
final AS (


    SELECT
      fct_ping_instance_metric.metrics_path,
      dim_ping_instance.ping_edition,
      dim_ping_instance.version_is_prerelease,
      dim_ping_instance.major_minor_version_id ,
      dim_ping_instance.major_minor_version,
      dim_ping_instance.major_version,
      dim_ping_instance.minor_version,
      dim_ping_metric.time_frame,
      fct_ping_instance_metric.dim_installation_id
    FROM fct_ping_instance_metric
    INNER JOIN dim_ping_metric
      ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
    INNER JOIN dim_ping_instance
      ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
    INNER JOIN dim_gitlab_releases --limit to valid versions
        ON dim_ping_instance.major_minor_version = dim_gitlab_releases.major_minor_version

    WHERE 
      -- Removing SaaS
      fct_ping_instance_metric.dim_instance_id != 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'

)
-- find min and max version for each metric

, transformed AS (

    SELECT DISTINCT
      {{ dbt_utils.generate_surrogate_key(['metrics_path', 'ping_edition', 'version_is_prerelease']) }} AS ping_metric_first_last_versions_id,
      metrics_path                                                                                                                               AS metrics_path,
      ping_edition                                                                                                                               AS ping_edition,
      version_is_prerelease                                                                                                                      AS version_is_prerelease,
      -- Grab first major/minor edition where metric/edition was present
      FIRST_VALUE(final.major_minor_version_id) OVER (
        PARTITION BY metrics_path, ping_edition, version_is_prerelease
          ORDER BY major_minor_version_id ASC
      )                                                                                                                                          AS first_major_minor_version_id_with_counter,
      -- Grab first major/minor edition where metric/edition was present
      FIRST_VALUE(final.major_minor_version) OVER (
        PARTITION BY metrics_path, ping_edition, version_is_prerelease
          ORDER BY major_minor_version_id ASC
      )                                                                                                                                          AS first_major_minor_version_with_counter,
      -- Grab first major edition where metric/edition was present
      FIRST_VALUE(final.major_version) OVER (
        PARTITION BY metrics_path, ping_edition, version_is_prerelease
          ORDER BY major_minor_version_id ASC
      )                                                                                                                                          AS first_major_version_with_counter,
      -- Grab first minor edition where metric/edition was present
      FIRST_VALUE(final.minor_version) OVER (
        PARTITION BY metrics_path, ping_edition, version_is_prerelease
          ORDER BY major_minor_version_id ASC
      )                                                                                                                                          AS first_minor_version_with_counter,
      -- Grab last major/minor edition where metric/edition was present
      LAST_VALUE(final.major_minor_version_id) OVER (
        PARTITION BY metrics_path, ping_edition, version_is_prerelease
          ORDER BY major_minor_version_id ASC
      )                                                                                                                                          AS last_major_minor_version_id_with_counter,
      -- Grab last major/minor edition where metric/edition was present
      LAST_VALUE(final.major_minor_version) OVER (
        PARTITION BY metrics_path, ping_edition, version_is_prerelease
          ORDER BY major_minor_version_id ASC
      )                                                                                                                                          AS last_major_minor_version_with_counter,
      -- Grab last major edition where metric/edition was present
      LAST_VALUE(final.major_version) OVER (
        PARTITION BY metrics_path, ping_edition, version_is_prerelease
          ORDER BY major_minor_version_id ASC
      )                                                                                                                                          AS last_major_version_with_counter,
      -- Grab last minor edition where metric/edition was present
      LAST_VALUE(final.minor_version) OVER (
        PARTITION BY metrics_path, ping_edition, version_is_prerelease
          ORDER BY major_minor_version_id ASC
      )                                                                                                                                          AS last_minor_version_with_counter,
      -- Get count of installations per each metric/edition
      COUNT(DISTINCT dim_installation_id) OVER (PARTITION BY metrics_path, ping_edition, version_is_prerelease)                                  AS dim_installation_count
    FROM final


)

{{ dbt_audit(
    cte_ref="transformed",
    created_by="@icooper-acp",
    updated_by="@mpetersen",
    created_date="2022-04-07",
    updated_date="2023-02-02"
) }}
