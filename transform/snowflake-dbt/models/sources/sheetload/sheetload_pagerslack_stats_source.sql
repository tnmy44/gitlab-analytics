WITH source AS (

SELECT * 
FROM {{ source('sheetload','pagerslack_stats') }}

)

SELECT * 
FROM source
