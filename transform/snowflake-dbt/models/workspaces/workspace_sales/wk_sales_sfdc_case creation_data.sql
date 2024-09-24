{{ config(alias='sfdc_case') }}


WITH source AS (

  SELECT *
  FROM {{ ref('sheetload_case_creation_data_source') }}

)

SELECT *
FROM source
