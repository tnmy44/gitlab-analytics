WITH source AS (

        SELECT * 
        FROM {{ source('sheetload','sales_dev_role_hierarchy_fy25') }}

        )

SELECT * 
FROM source