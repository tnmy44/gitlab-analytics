{{ config({
        "tags": ["mnpi_exception"],
    })
}}

{{ simple_cte([
    ('prep_billing_account','prep_billing_account')
]) }}

, billing_account AS (


    SELECT
    --surrogate key
      prep_billing_account.dim_billing_account_sk,
    
     --natural key
       prep_billing_account.dim_billing_account_id,
 
     --foreign key
       prep_billing_account.dim_crm_account_id,

     --Other attributes
      prep_billing_account.billing_account_number,
      prep_billing_account.billing_account_name,
      prep_billing_account.account_status,
      prep_billing_account.parent_id,
      prep_billing_account.sfdc_account_code             AS crm_account_code, 
      prep_billing_account.sfdc_entity                   AS crm_entity,
      prep_billing_account.account_currency,
      prep_billing_account.sold_to_country,
      prep_billing_account.ssp_channel,
      prep_billing_account.po_required,
      prep_billing_account.auto_pay,
      prep_billing_account.default_payment_method_type,
      prep_billing_account.is_deleted,
      prep_billing_account.batch,
      prep_billing_account.record_data_source
    FROM prep_billing_account

)

{{ dbt_audit(
    cte_ref="billing_account",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-04-25",
    updated_date="2023-04-25"
) }}
