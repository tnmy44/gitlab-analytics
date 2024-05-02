WITH source AS (
  SELECT *
  FROM {{ ref('bamboohr_id_employee_number_mapping_source') }}
),

renamed AS (

  SELECT DISTINCT
    employee_id AS bhr_employee_id,
    employee_number AS wk_employee_id
  FROM source
  WHERE uploaded_row_number_desc = 1

)

SELECT *
FROM renamed