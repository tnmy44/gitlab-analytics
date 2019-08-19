{{config({
    "materialized": "table",
    "schema": "staging"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce_archive', 'sfdc_opportunity_archived') }}

), final AS (

    SELECT *,
            "scd_id"            AS unique_identifier,
            "dbt_updated_at"    AS dbt_last_updated_timestamp
    FROM source
    {{ dbt_utils.group_by(n=59) }}

)

SELECT *
FROM final
