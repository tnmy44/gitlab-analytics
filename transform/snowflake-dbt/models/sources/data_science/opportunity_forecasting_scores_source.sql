WITH source AS (

    SELECT *
    FROM {{ source('data_science', 'opportunity_forecasting_scores') }}

), intermediate AS (

    SELECT
      d.value as data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), parsed AS (

    SELECT

      data_by_row['dim_crm_opportunity_id']::VARCHAR                            AS dim_crm_opportunity_id,
      data_by_row['score_date']::TIMESTAMP                                      AS score_date,
      data_by_row['forecasted_days_remaining']::NUMBER(38,0)                    AS forecasted_days_remaining,
      data_by_row['forecasted_close_won_date']::TIMESTAMP                       AS forecasted_close_won_date,
      data_by_row['forecasted_close_won_quarter']::VARCHAR                      AS forecasted_close_won_quarter,
      data_by_row['score_group']::VARCHAR                                       AS score_group,
      data_by_row['insights']::VARCHAR                                          AS insights,
      data_by_row['submodel']::VARCHAR                                          AS submodel,
      data_by_row['model_version']::NUMBER(38,4)                                AS model_version,
      uploaded_at::TIMESTAMP                                                    AS uploaded_at

    FROM intermediate

)
SELECT *
FROM parsed