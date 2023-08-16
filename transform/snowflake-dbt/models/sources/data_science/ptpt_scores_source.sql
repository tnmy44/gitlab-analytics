WITH source AS (

    SELECT
        namespace_id,
        score_date,
        score,
        decile,
        importance,
        grouping,
        insights,
        uploaded_at::TIMESTAMP as uploaded_at
    FROM {{ source('data_science', 'ptpt_scores') }}
)

SELECT *
FROM source