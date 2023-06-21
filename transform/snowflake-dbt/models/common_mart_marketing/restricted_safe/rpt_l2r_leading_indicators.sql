{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('rpt_lead_to_revenue','rpt_lead_to_revenue')
]) }}

, final AS ( 

    SELECT
    --IDs    
        dim_crm_person_id,
        dim_crm_opportunity_id,

    --Person Data
        email_hash,
        email_domain_type,
        person_order_type,
        account_demographics_sales_segment,
        account_demographics_geo,
        lead_source,
        inquiry_sum,
        mql_sum,

    --Person Dates
        true_inquiry_date,
        mql_date_first_pt,

    --Opportunity Data
        opp_order_type,
        crm_opp_owner_sales_segment_stamped,
        crm_opp_owner_geo_stamped,
        sales_qualified_source_name,

    --Opportunity Dates
        sales_accepted_date,

    --Account Data
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        
    --Bizible Fields
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,

    --Flags
        is_mql,
        is_sao
    FROM rpt_lead_to_revenue
    WHERE (account_demographics_geo != 'JIHU'
     OR account_demographics_geo IS null) 
     AND (crm_opp_owner_geo_stamped != 'JIHU'
     OR crm_opp_owner_geo_stamped IS null)

    )

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-06-21",
    updated_date="2023-06-21",
  ) }}