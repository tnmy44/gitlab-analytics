WITH source AS (

    SELECT *
    FROM {{ source('data_science', 'ptpf_scores') }}

), intermediate AS (

    SELECT
      d.value as data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), parsed AS (

    SELECT

      data_by_row['namespace_id']::VARCHAR                  AS namespace_id,
      data_by_row['score_date']::TIMESTAMP                  AS score_date,
      data_by_row['score']::NUMBER(38,4)                    AS score,
      data_by_row['decile']::INT                            AS decile,
      data_by_row['score_group']::INT                       AS score_group,
      data_by_row['insights']::VARCHAR                      AS insights,
      data_by_row['sub_model']::VARCHAR                     AS sub_model,
      data_by_row['days_since_trial_start']::NUMBER(38,0)   AS days_since_trial_start,
      uploaded_at::TIMESTAMP                                AS uploaded_at

    FROM intermediate

)
SELECT *
FROM parsed
