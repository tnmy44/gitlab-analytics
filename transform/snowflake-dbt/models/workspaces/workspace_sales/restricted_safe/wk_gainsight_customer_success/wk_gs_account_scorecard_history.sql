WITH source AS (

    SELECT
    {{ hash_sensitive_columns('account_scorecard_history') }}
    FROM {{ source('gainsight_customer_success', 'account_scorecard_history') }}

)

SELECT *
FROM source