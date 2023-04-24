WITH source AS (

    SELECT *
    FROM {{ source('zuora_query_api', 'ramps') }}

), renamed AS (

    SELECT

      Id::TEXT                                                             AS id,
      OrderId::TEXT                                                        AS orderid,
      Name::TEXT                                                           AS name,
      Description::TEXT                                                    AS description,
      GrossTcb::TEXT                                                       AS grosstcb,
      DiscountTcv::TEXT                                                    AS discounttcv,
      NetTcb::TEXT                                                         AS net_tcb,
      MetricsProcessingStatus::TEXT                                        AS metrics_processing_status,
      GrossTcv::TEXT                                                       AS gross_tcv,
      Number::TEXT                                                         AS number,
      NetTcv::TEXT                                                         AS net_tcv,
      SubscriptionNumbers::TEXT                                            AS subscription_numbers,
      ChargeNumbers::TEXT                                                  AS charge_numbers,
      DiscountTcb::TEXT                                                    AS discount_tcb,
      CreatedById::TEXT                                                    AS created_by_id,
      CreatedDate::TEXT                                                    AS created_date,
      UpdatedById::TEXT                                                    AS updated_by_id,
      UpdatedDate::TEXT                                                    AS updated_date,
      TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "CreatedDate"))::TIMESTAMP      AS created_date,
      TO_TIMESTAMP_NTZ(CAST(_uploaded_at AS INT))::TIMESTAMP               AS uploaded_at

    FROM source

)

SELECT *
FROM renamed

