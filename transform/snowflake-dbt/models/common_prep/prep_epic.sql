{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_epic_sk"
    })
}}

{{ simple_cte([
    ('prep_date', 'prep_date'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('gitlab_dotcom_routes_source', 'gitlab_dotcom_routes_source'),
    ('prep_label_links', 'prep_label_links'),
    ('prep_labels', 'prep_labels'),
    ('gitlab_dotcom_award_emoji_source', 'gitlab_dotcom_award_emoji_source')
]) }}

,  namespace_prep AS (

    SELECT *
    FROM {{ ref('prep_namespace') }}
    WHERE is_currently_valid = TRUE

), gitlab_dotcom_epics_dedupe_source AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_epics_dedupe_source') }} 
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), upvote_count AS (

    SELECT
      awardable_id                                        AS epic_id,
      SUM(IFF(award_emoji_name LIKE 'thumbsup%', 1, 0))   AS thumbsups_count,
      SUM(IFF(award_emoji_name LIKE 'thumbsdown%', 1, 0)) AS thumbsdowns_count,
      thumbsups_count - thumbsdowns_count                 AS upvote_count
    FROM gitlab_dotcom_award_emoji_source
    WHERE awardable_type = 'Epic'
    GROUP BY 1

), agg_labels AS (

    SELECT 
      prep_label_links.epic_id                                                                      AS epic_id,
      ARRAY_AGG(LOWER(prep_labels.label_title)) WITHIN GROUP (ORDER BY prep_labels.label_title ASC) AS labels
    FROM prep_label_links
    LEFT JOIN prep_labels
      ON prep_label_links.dim_label_id = prep_labels.dim_label_id
    WHERE prep_label_links.epic_id IS NOT NULL
    GROUP BY 1  

), joined AS (

    SELECT 
      {{ dbt_utils.surrogate_key(['gitlab_dotcom_epics_dedupe_source.id']) }}                AS dim_epic_sk,
      gitlab_dotcom_epics_dedupe_source.id::NUMBER                                           AS epic_id,
      gitlab_dotcom_epics_dedupe_source.author_id::NUMBER                                    AS author_id,
      gitlab_dotcom_epics_dedupe_source.group_id::NUMBER                                     AS namespace_id,
      namespace_prep.ultimate_parent_namespace_id::NUMBER                                    AS ultimate_parent_namespace_id,
      prep_date.date_id::NUMBER                                                              AS created_date_id,
      IFNULL(prep_namespace_plan_hist.dim_plan_id, 34)::NUMBER                               AS dim_plan_id_at_creation,
      gitlab_dotcom_epics_dedupe_source.assignee_id::NUMBER                                  AS assignee_id,
      gitlab_dotcom_epics_dedupe_source.iid::NUMBER                                          AS epic_internal_id,
      gitlab_dotcom_epics_dedupe_source.updated_by_id::NUMBER                                AS updated_by_id,
      gitlab_dotcom_epics_dedupe_source.last_edited_by_id::NUMBER                            AS last_edited_by_id,
      gitlab_dotcom_epics_dedupe_source.lock_version::NUMBER                                 AS lock_version,
      gitlab_dotcom_epics_dedupe_source.start_date::DATE                                     AS epic_start_date,
      gitlab_dotcom_epics_dedupe_source.end_date::DATE                                       AS epic_end_date,
      gitlab_dotcom_epics_dedupe_source.last_edited_at::TIMESTAMP                            AS epic_last_edited_at,
      gitlab_dotcom_epics_dedupe_source.created_at::TIMESTAMP                                AS created_at,
      gitlab_dotcom_epics_dedupe_source.updated_at::TIMESTAMP                                AS updated_at,
      IFF(namespace_prep.visibility_level = 'private',
        'private - masked',
        gitlab_dotcom_epics_dedupe_source.title::VARCHAR)                                    AS epic_title,
      IFF(namespace_prep.visibility_level = 'private',
        'private - masked',
        gitlab_dotcom_epics_dedupe_source.description::VARCHAR)                              AS epic_description,
      gitlab_dotcom_epics_dedupe_source.closed_at::TIMESTAMP                                 AS closed_at,
      gitlab_dotcom_epics_dedupe_source.state_id::NUMBER                                     AS state_id,
      gitlab_dotcom_epics_dedupe_source.parent_id::NUMBER                                    AS parent_id,
      gitlab_dotcom_epics_dedupe_source.relative_position::NUMBER                            AS relative_position,
      gitlab_dotcom_epics_dedupe_source.start_date_sourcing_epic_id::NUMBER                  AS start_date_sourcing_epic_id,
      gitlab_dotcom_epics_dedupe_source.confidential::BOOLEAN                                AS is_confidential,
      namespace_prep.namespace_is_internal                                                   AS is_internal_epic,
      {{ map_state_id('gitlab_dotcom_epics_dedupe_source.state_id') }}                       AS epic_state,
      LENGTH(gitlab_dotcom_epics_dedupe_source.title)::NUMBER                                AS epic_title_length,
      LENGTH(gitlab_dotcom_epics_dedupe_source.description)::NUMBER                          AS epic_description_length,
      IFF(namespace_prep.visibility_level = 'private',
        'private - masked',
        'https://gitlab.com/groups/' || gitlab_dotcom_routes_source.path || '/-/epics/' || gitlab_dotcom_epics_dedupe_source.iid)
                                                                                             AS epic_url,
      IFF(namespace_prep.visibility_level = 'private',
        ARRAY_CONSTRUCT('private - masked'),
        agg_labels.labels)                                                                   AS labels,
      IFNULL(upvote_count.upvote_count, 0)                                                   AS upvote_count
    FROM gitlab_dotcom_epics_dedupe_source
    LEFT JOIN namespace_prep
        ON gitlab_dotcom_epics_dedupe_source.group_id = namespace_prep.dim_namespace_id
    LEFT JOIN prep_namespace_plan_hist
        ON namespace_prep.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_epics_dedupe_source.created_at >= prep_namespace_plan_hist.valid_from
        AND gitlab_dotcom_epics_dedupe_source.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN prep_date
        ON TO_DATE(gitlab_dotcom_epics_dedupe_source.created_at) = prep_date.date_day
    LEFT JOIN gitlab_dotcom_routes_source
        ON gitlab_dotcom_routes_source.source_id = gitlab_dotcom_epics_dedupe_source.group_id
        AND gitlab_dotcom_routes_source.source_type = 'Namespace'
    LEFT JOIN agg_labels
        ON agg_labels.epic_id = gitlab_dotcom_epics_dedupe_source.id
    LEFT JOIN upvote_count
        ON upvote_count.epic_id = gitlab_dotcom_epics_dedupe_source.id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-06-22",
    updated_date="2023-09-05"
) }}
