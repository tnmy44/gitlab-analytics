WITH source AS (

  SELECT * 
  FROM {{ source('workday','performance_growth_potential') }}
  
), intermediate AS (

    SELECT
        d.value AS data_by_row,
        source.employee_id
    FROM
        source,
        LATERAL FLATTEN(input => parse_json(REVIEWS_ALL_STATUSES_GROUP)) AS d

), parsed AS (

    SELECT
        employee_id::NUMBER                                 AS employee_id, 
        data_by_row['GROWTH_POTENTIAL_RATING']::VARCHAR     AS growth_potential_rating,
        data_by_row['PERFORMANCE_RATING']::VARCHAR          AS performance_rating,
        data_by_row['WORKDAY_ID']::VARCHAR                  AS workday_id,
        data_by_row['Review_Period_-_Start_Date']::DATE     AS review_period_start_date,
        data_by_row['Review_Period_-_End_Date']::DATE       AS review_period_end_date
    FROM intermediate

)

SELECT * 
FROM  parsed;

