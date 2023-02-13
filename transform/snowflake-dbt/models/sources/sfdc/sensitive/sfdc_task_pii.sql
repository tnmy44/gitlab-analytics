WITH source AS (

    SELECT *
    FROM {{ ref('sfdc_task_source') }}

), sfdc_task_pii AS (

    SELECT
      task_id,
      {{ nohash_sensitive_columns('sfdc_task_source','task_id') }}
    FROM source

)

SELECT *
FROM sfdc_task_pii
