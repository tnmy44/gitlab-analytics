{{ sfdc_account_fields('live') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@snalamaru",
    created_date="2020-06-01",
    updated_date="2023-11-16"
) }}
