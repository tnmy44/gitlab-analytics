        WITH source AS (

        SELECT * 
        FROM {{ source('sheetload','sales_targets') }}

        )
        SELECT * 
        FROM source
