{{ config(alias='sfdc_case_creation_data') }}


WITH source AS (

  SELECT *
  FROM {{ ref('sheetload_case_creation_data_source') }}

)

SELECT *
FROM source



sfdc_subscription_renewal_case_creation