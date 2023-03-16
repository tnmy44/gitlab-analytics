WITH source AS (

    SELECT *
    FROM {{ source('workday','staffing_history_approved') }}
  
), intermediate AS (

    SELECT
        d.value AS data_by_row,
        source.employee_id,
        source._fivetran_synced, 
        source._fivetran_deleted    
    FROM
        source,
        LATERAL FLATTEN(input => parse_json(STAFFING_HISTORY_APPROVED)) AS d

), parsed AS (


    SELECT
        employee_id::NUMBER                               AS employee_id, 
        data_by_row['WORKDAY_ID']::VARCHAR                AS workday_id,
        data_by_row['BUSINESS_PROCESS_TYPE']::VARCHAR     AS business_process_type,
        data_by_row['HIRE_DATE']::VARCHAR                 AS hire_date,
        data_by_row['TERMINATION_DATE']::VARCHAR          AS termination_date,
        data_by_row['COUNTRY_CURRENT']::VARCHAR           AS past_country,
        data_by_row['COUNTRY_PROPOSED']::VARCHAR          AS current_country,
        data_by_row['REGION_CURRENT']::VARCHAR            AS past_region,
        data_by_row['REGION_PROPOSED']::VARCHAR           AS current_region,
        data_by_row['DATE_TIME_INITIATED']::DATE          AS date_time_initiated,
        data_by_row['EFFECTIVE_DATE']::DATE               AS effective_date
    FROM intermediate

)

SELECT
* from parsed