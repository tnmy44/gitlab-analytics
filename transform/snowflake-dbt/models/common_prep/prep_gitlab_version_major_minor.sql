WITH base AS (

    SELECT

        -- Surrogate key
        {{ dbt_utils.surrogate_key(['major_minor_version']) }} AS dim_gitlab_version_major_minor_sk,

        -- Natural key
        major_version * 100 + minor_version                    AS gitlab_version_major_minor_id,

        major_minor_version,
        major_version,
        minor_version,
        release_date,
        release_manager_americas,
        release_manager_emea

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
