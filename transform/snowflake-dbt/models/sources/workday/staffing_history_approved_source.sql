WITH source AS (

  SELECT *
  FROM {{ source('workday','staffing_history_approved') }}

), 

renamed AS (

  SELECT
    source.employee_id::NUMBER                    AS employee_id,
    d.value['WORKDAY_ID']::VARCHAR                AS workday_id,
    d.value['BUSINESS_PROCESS_TYPE']::VARCHAR     AS business_process_type,
    d.value['HIRE_DATE']::DATE                    AS hire_date,
    d.value['TERMINATION_DATE']::DATE             AS termination_date,
    d.value['COUNTRY_CURRENT']::VARCHAR           AS past_country,
    d.value['COUNTRY_PROPOSED']::VARCHAR          AS current_country,
    d.value['REGION_CURRENT']::VARCHAR            AS past_region,
    d.value['REGION_PROPOSED']::VARCHAR           AS current_region,
    d.value['DATE_TIME_INITIATED']::DATE          AS date_time_initiated,
    d.value['EFFECTIVE_DATE']::DATE               AS effective_date
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(STAFFING_HISTORY_APPROVED)) AS d

)

SELECT *
FROM renamed
