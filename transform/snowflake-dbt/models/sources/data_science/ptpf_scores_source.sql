WITH source AS (

    SELECT *
    FROM {{ source('data_science', 'ptpf_scores') }}
)
SELECT *
FROM source
