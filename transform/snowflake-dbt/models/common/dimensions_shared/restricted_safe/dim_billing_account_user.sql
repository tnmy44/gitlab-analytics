{{ simple_cte([
    ('prep_billing_account_user','prep_billing_account_user')
]) }}

, billing_account_user AS (

    SELECT 
      zuora_user_id         AS dim_billing_account_user_id,
      user_name,
      is_integration_user
    FROM prep_billing_account_user
)

{{ dbt_audit(
    cte_ref="billing_account_user",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2023-04-28",
    updated_date="2023-04-28"
) }}
