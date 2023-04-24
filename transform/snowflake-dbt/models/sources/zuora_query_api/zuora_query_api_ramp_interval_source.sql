WITH source AS (

    SELECT *
    FROM {{ source('zuora_query_api', 'rampinterval') }}

), renamed AS (

    SELECT

      "Id"::TEXT                                                             AS ramp_interval_id,
      "Rampid"::TEXT                                                         AS ramp_id,
      "StartDate"::TEXT                                                      AS start_date,
      "EndDate"::TEXT                                                        AS end_date,
      "Name"::TEXT                                                           AS name,
      "Description"::TEXT                                                    AS description,
      "GrossTcb"::TEXT                                                       AS gross_tcb,
      "DiscountTcv"::TEXT                                                    AS discount_tcv,
      "NetTcb"::TEXT                                                         AS net_tcb,
      "GrossTcv"::TEXT                                                       AS gross_tcv,
      "NetTcv"::TEXT                                                         AS net_tcv,
      "DiscountTcb"::TEXT                                                    AS discount_tcb,
      "CreatedById"::TEXT                                                    AS created_by_id,
      TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "CreatedDate"))::TIMESTAMP        AS created_date,
      "UpdatedById"::TEXT                                                    AS updated_by_id,
      "UpdatedDate"::TEXT                                                    AS updated_date,
      TO_TIMESTAMP_NTZ(CAST(_uploaded_at AS INT))::TIMESTAMP                 AS uploaded_at

    FROM source

)

SELECT *
FROM renamed
