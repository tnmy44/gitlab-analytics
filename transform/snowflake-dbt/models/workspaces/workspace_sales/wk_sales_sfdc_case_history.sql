{{ config(alias='sfdc_case_history') }}

WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_case_history_source') }}

)

SELECT *
FROM base