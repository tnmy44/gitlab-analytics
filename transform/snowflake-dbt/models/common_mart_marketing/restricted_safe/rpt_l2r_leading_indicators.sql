{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('rpt_lead_to_revenue','rpt_lead_to_revenue'),
    ('dim_date','dim_date')
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
        is_sao,

    --Inquiry Dates
        inquiry_date.first_day_of_weekAS inquiry_date_range_week,
        DATE_TRUNC(month, inquiry_date.date_actual) AS inquiry_date_range_month,
        inquiry_date.fiscal_quarter_name_fy AS inquiry_date_range_quarter,
        inquiry_date.fiscal_year AS inquiry_date_range_year,

    --MQL Dates
        mql_date.first_day_of_weekAS mql_date_range_week,
        DATE_TRUNC(month, mql_date.date_actual) AS mql_date_range_month,
        mql_date.fiscal_quarter_name_fy AS mql_date_range_quarter,
        mql_date.fiscal_year AS mql_date_range_year,

    --SAO Dates
        sao_date.first_day_of_weekAS sao_date_range_week,
        DATE_TRUNC(month, sao_date.date_actual) AS sao_date_range_month,
        sao_date.fiscal_quarter_name_fy AS sao_date_range_quarter,
        sao_date.fiscal_year AS sao_date_range_year
    FROM rpt_lead_to_revenue
    LEFT JOIN dim_date inquiry_date 
        ON rpt_lead_to_revenue.true_inquiry_date.dim_date.date_actual
    LEFT JOIN dim_date mql_date 
        ON rpt_lead_to_revenue.mql_date_first_pt.dim_date.date_actual
    LEFT JOIN dim_date sao_date 
        ON rpt_lead_to_revenue.sales_accepted_date.dim_date.date_actual
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