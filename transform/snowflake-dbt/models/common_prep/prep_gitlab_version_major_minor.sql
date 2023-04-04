WITH base AS (

    SELECT
        {{ dbt_utils.surrogate_key(['major_minor_version']) }} AS dim_gitlab_version_major_minor_sk,
        *
    FROM {{ ref('release_managers_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY major_minor_version ORDER BY snapshot_date DESC, rank DESC) = 1

), final AS (

    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY release_date)                      AS version_number,
        LEAD(release_date) OVER (ORDER BY release_date)                AS next_version_release_date
    FROM base

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2023-04-04",
    updated_date="2023-04-04"
) }}
