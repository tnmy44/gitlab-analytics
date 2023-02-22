WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_reconciliations') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT DISTINCT
      completed_email_sent_at::TIMESTAMP,
      created_at::TIMESTAMP,
      updated_at::TIMESTAMP,
      error_message::VARCHAR,
      id::NUMBER,
      order_id::NUMBER,
      qsr_opportunity_id::NUMBER,
      quoted_invoice_amount::NUMBER,
      quoted_invoice_at::TIMESTAMP,
      reconcile_done_at::TIMESTAMP,
      reconcile_on::TIMESTAMP,
      skip_reason::VARCHAR,
      upcoming_email_sent_at::TIMESTAMP,
      updated_at::TIMESTAMP,
      user_count::NUMBER
    FROM source
    
)

SELECT *
FROM renamed