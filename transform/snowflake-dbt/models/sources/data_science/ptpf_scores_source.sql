WITH source AS (

    SELECT
        score_date,
        score,
        decile,
        score_group,
        insights,
        sub_model,
        days_since_trial_start
    FROM {{ source('data_science', 'ptpf_scores') }}
)

SELECT *
FROM source