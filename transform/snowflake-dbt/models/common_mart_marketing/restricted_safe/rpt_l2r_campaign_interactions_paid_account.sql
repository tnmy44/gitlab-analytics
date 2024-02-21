{{ config(materialized='table') }}

{{ simple_cte([
    ('campaigns', 'rpt_l2r_campaign_interactions'),
    ('account', 'mart_crm_account')
]) }}

, final AS (

  select
    campaigns.*,
  from
    campaigns 
   inner join
      account 
      on campaigns.dim_crm_account_id = account.dim_crm_account_id 
where
   account.crm_account_type = 'Customer' 
   and account.is_deleted = false
   and touchpoint_type = 'Person Touchpoint'
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jonglee1218",
    updated_by="@jonglee1218",
    created_date="2024-02-15",
    updated_date="2024-02-15",
  ) }}