{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_epic_issue_sk"
    })
}}

{{ simple_cte([
    ('prep_issue', 'prep_issue'),
    ('prep_epic', 'prep_epic')
]) }}

, epic_issues AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_epic_issues_source') }} 
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), final AS (

    SELECT 
      -- Surrogate Key
      {{ dbt_utils.surrogate_key(['epic_issues.epic_issues_relation_id']) }}  AS dim_epic_issue_sk,

      -- Natural Key
      epic_issues.epic_issues_relation_id                                     AS epic_issue_id,

      -- Foreign Keys
      prep_issue.dim_issue_sk,
      prep_epic.dim_epic_sk,
      epic_issues.issue_id,
      epic_issues.epic_id,

      -- Other Attributes
      epic_issues.epic_issue_relative_position                                AS epic_issue_relative_position

    FROM epic_issues
    LEFT JOIN prep_issue
      ON epic_issues.issue_id = prep_issue.issue_id
    LEFT JOIN prep_epic
      ON epic_issues.epic_id = prep_epic.epic_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-09-07",
    updated_date="2023-09-07"
) }}