{{ config(materialized='table') }}

{{ simple_cte([
('campaigns', 'rpt_l2r_campaign_interactions'),
('touchpoint_offer','person_touchpoint_offer_type'),
('account', 'mart_crm_account')
]) }}

,final AS (

  select
    campaigns.*,
    touchpoint_offer_type,
    touchpoint_offer_type_grouped
  from
    campaigns 
   inner join
      touchpoint_offer
      on campaigns.dim_crm_touchpoint_id = touchpoint_offer.dim_crm_touchpoint_id
   inner join
      account 
      on campaigns.dim_crm_account_id = account.dim_crm_account_id 
where
   account.crm_account_type = 'Customer' 
   and account.is_deleted = false
   and account.carr_this_account > 0
   and touchpoint_type = 'Person Touchpoint'
)

SELECT * 
FROM final