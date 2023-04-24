WITH source AS (

    SELECT *
    FROM {{ source('zuora_query_api', 'ramp') }}

), renamed AS (

    SELECT

      "Id"::TEXT                                                             AS ramp_id,
      "OrderId"::TEXT                                                        AS order_id,
      "Name"::TEXT                                                           AS name,
      "Description"::TEXT                                                    AS description,
      "GrossTcb"::TEXT                                                       AS gross_tcb,
      "DiscountTcv"::TEXT                                                    AS discount_tcv,
      "NetTcb"::TEXT                                                         AS net_tcb,
      "MetricsProcessingStatus"::TEXT                                        AS metrics_processing_status,
      "GrossTcv"::TEXT                                                       AS gross_tcv,
      "Number"::TEXT                                                         AS number,
      "NetTcv"::TEXT                                                         AS net_tcv,
      "SubscriptionNumbers"::TEXT                                            AS subscription_numbers,
      "ChargeNumbers"::TEXT                                                  AS charge_numbers,
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

